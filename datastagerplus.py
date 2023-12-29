import fileprocessing
import logging
import os
import glob
import time
import threading
import pandas as pd
import sqlalchemy
import dask.dataframe as dd
import configparser
import urllib.parse
import json
import queue

# This function will be called in thread to process all files in a folder. So each folder will be processed in a different thread. Note here that each folder also maps to a single table.
def process_folder_files(thread, monitor_folder, dir_path, supported_delimiters, server, database, schema_name, connectiontype, rdms_name, usr, pwd): 
    logging.warning("Thread %s: starting", thread)
    connection = fileprocessing.getdbconnection(server
                                            , database
                                            , connectiontype
                                            , rdms_name
                                            , usr
                                            , pwd)
    # get list of files to process
    files = glob.glob(os.path.join(dir_path, '*'))

    # process each file into target table
    for file in files:
        if os.path.isfile(file):

            file_name = os.path.basename(file)
            archive_folder = monitor_folder + '/archive/'+schema_name+'/'+str(os.path.basename(dir_path))+'/'
            error_folder = monitor_folder + '/error/'+schema_name+'/'+str(os.path.basename(dir_path))+'/'
            target_table = os.path.basename(dir_path).lower()

            # Check that file is not being used (in flight...)
            if not fileprocessing.check_file_status(file):
                continue

            # Check that file is not already loaded
            if fileprocessing.is_file_loaded(file_name
                                             , target_table
                                             , connection):
                fileprocessing.archive_file(file, archive_folder)  # Archive the file
                continue

            # Attemp to create panda from file and figure out the delimiter used in file
            data_object = fileprocessing.prep_file(file, supported_delimiters)
            df = data_object[0]  # process file i.e. read into dataframe
            meta_data = data_object[1]
            profile_hk = meta_data['profile_hk']
            
            if isinstance(df, pd.DataFrame) or isinstance(df, dd.DataFrame):  # if a dataframe was returned
                status = fileprocessing.write_profile_data(df
                                                           , meta_data
                                                           , file
                                                           , target_table
                                                           , schema_name
                                                           , connection)  # write date profile

                if status[0] == 1:  # if profile was not written skip this file
                    continue
                status = load_data(df
                                , file
                                , target_table
                                , schema_name
                                , connection)  # load data to target database

                if status[0] == 1:  # if not able to load data, move file to error folder
                    status = fileprocessing.generate_error_log_entry(profile_hk
                                                                     , target_table
                                                                     , str(status[1])
                                                                     , connection)
                    fileprocessing.error_file(file, error_folder)
                    continue
                # if previous file was not processed to schema or data issues skip the file
                if status[0] == 2:  # if not able to load data, move file to error folder
                    continue
                 
                deletetablequeue.put(status[1])
                fileprocessing.archive_file(file, archive_folder)  # Archive the file

                # only move to next file if file archiving is done
                while os.path.isfile(file):
                    time.sleep(5)
                # Close File load status
                fileprocessing.set_file_processed_status(profile_hk, connection)
            else:  # data was not processed into dataframe
                message = 'Error reading file into a dataframe...Make sure format is supported.'
                status = fileprocessing.generate_error_log_entry(file_name
                                                                 , target_table
                                                                 , message
                                                                 , connection)
                fileprocessing.error_file(file, error_folder)

    connection[0].dispose()
        
    logging.warning("Thread %s: Ending", thread)


# This function writes dataframe to target database table
def load_data(df_object, file_path, targettablename, schemaname, connection):
    ret_val = []

    file_name = os.path.splitext(os.path.basename(file_path))[0]
    full_file_name = file_name+os.path.splitext(os.path.basename(file_path))[1]
    load_table = targettablename + '_' + file_name
    targettableexits = fileprocessing.check_table_exists(targettablename, schemaname, connection)

    # If this is first file for given table set up so that it is loaded directly
    if not targettableexits:
        load_table = targettablename
        
    # If load table for a file persists from last attempt, someone needs to delete manually
    if fileprocessing.check_table_exists(load_table, schemaname, connection):
        ret_val.append(2)

    try:

        # Here we get create table statement and put in queue.
        createtablescript = str(pd.io.sql.get_schema(df_object, 'load_table_name', con=connection[0])).replace('load_table_name',schemaname+'.'+load_table)
        createtablequeue.put(createtablescript)

        # lets wait for the table to be created
        while not fileprocessing.check_table_exists(load_table, schemaname, connection):
            time.sleep(5)            

        if isinstance(df_object, pd.DataFrame):
            df_object.to_sql(load_table
                         , con=connection[0]
                         , schema=schemaname
                         , if_exists='append'
                         , index=False
                         , chunksize=10000)
        else: # This will be run in case we use dask Dataframe instead.
            df_object.to_sql(load_table
                         , uri=connection[1]
                         , schema=schemaname
                         , if_exists='append'
                         , index=False
                         , chunksize=10000)

        if targettableexits:
            # Insert data from temp table to actual target table
            insert_statement = "INSERT INTO {}.{} SELECT * FROM {}.[{}]".format(schemaname,targettablename, schemaname,load_table)
            conn = connection[0].connect()
            conn.execute(
                sqlalchemy.text(insert_statement))
            conn.commit()
            conn.close()

        print(f"Successfully loaded File '{full_file_name}' into table {schemaname}.{targettablename}")
        ret_val.append(0)

        if targettableexits:
            ret_val.append(schemaname+'.'+load_table)
        else:
            ret_val.append('NEW.TABLE')

    except Exception as e:
        logging.error('An exception occurred while loading panda into target table: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val

# This function will continuously run to drop tmp tables, this is to deal with deadlock issues when multiple tables are being dropped
def delete_load_table(connection):
    while True:
        # Drop temp table is insert was successful
        if not deletetablequeue.empty():
            tablename = deletetablequeue.get()
            tablename = str(tablename).split('.')
            if fileprocessing.check_table_exists(tablename[1], tablename[0], connection):
                drop_table = "DROP TABLE {}.{}".format(tablename[0],tablename[1])

                conn = connection[0].connect()
                conn.execute(sqlalchemy.text(drop_table))
                conn.commit()
                conn.close()
        time.sleep(10)

# This function will continuously run to create tmp tables, this is to deal with deadlock issues when multiple tables are being created
def create_load_table(connection):
    while True:
        # Drop temp table is insert was successful
        if not createtablequeue.empty():
            createtablescript = createtablequeue.get()
            conn = connection[0].connect()
            conn.execute(sqlalchemy.text(createtablescript))
            conn.commit()
            conn.close()
        time.sleep(10)


# Main Function Entry
if __name__ == "__main__":
    # get information from configuration file.
    config = configparser.ConfigParser()
    config.read('setting.cfg')
    targetserver = urllib.parse.quote(config['DATABASE_SERVER']['SERVER'])
    targetdatabase = config['DATABASE_SERVER']['DATABASE']
    connectiontype = config['DATABASE_SERVER']['CONNECTIONTYPE'] 
    rdms = config['DATABASE_SERVER']['RDMS']
    user = config['DATABASE_SERVER']['USER']
    password = urllib.parse.quote(config['DATABASE_SERVER']['PASSWORD'])
    watched_folder = config['FILE_PATH']['ROOTDROPFOLDER']
    delimiters = urllib.parse.quote(config['SUPPORTED_DELIMITERS']['DELIMITERS'].strip()).split('~')

    # This will be token used to determine which new table is in creation
    deletetablequeue = queue.Queue()
    createtablequeue = queue.Queue()


    while True:
        # Check all active threads
        active_threads = []
        for active_thread in threading.enumerate():
            active_threads.append(active_thread.name)
        
        # If a drop folder does not exist exit the program
        drop_folder = watched_folder + '/drop/'
        if not os.path.isdir(drop_folder):
            break

        # Scan drop folder's first level folders for files
        for (dir_root, dir_name, file_list) in os.walk(drop_folder):
            # and we are just one level deep from drop folder,
            # and folder is not empty,
            # and folder is not currently being processed, start a thread to process files in the folder
            if ((drop_folder != dir_root)
                  and (dir_root.count(os.path.sep) ==1)
                  and (len(file_list) != 0)
                  and str(os.path.basename(dir_root)).lower() not in active_threads):

                # Setup Master Thread Connection
                # We are naming the thread with folder name (So we should have only one thread per folder)
                connection = fileprocessing.getdbconnection(targetserver, targetdatabase, connectiontype, rdms, user, password)
                schema_name = os.path.basename(os.path.dirname(dir_root))
                threadname = str(os.path.basename(dir_root)).lower()

                # Check if there is a load table delete thread
                if 'datafilestage_delete_load_table' not in active_threads:
                    deletethread = threading.Thread(target=delete_load_table,
                                                    name = 'datafilestage_delete_load_table',
                                                    args = (connection,))
                    deletethread.start()

                # Check if there is a load table create thread
                if 'datafilestage_create_load_table' not in active_threads:
                    createthread = threading.Thread(target=create_load_table,
                                                    name = 'datafilestage_create_load_table',
                                                    args = (connection,))
                    createthread.start()

                # If it is new table, and we are currently not processing a new table, put in queue
                folderthread = threading.Thread(target=process_folder_files,
                                                name=threadname,
                                                args=(threadname,
                                                      watched_folder,
                                                      dir_root,
                                                      delimiters,
                                                      targetserver,
                                                      targetdatabase,
                                                      schema_name,
                                                      connectiontype,
                                                      rdms,
                                                      user,
                                                      password,))
                folderthread.start()

        time.sleep(5)  # Wait for 5 minutes before checking for new files
