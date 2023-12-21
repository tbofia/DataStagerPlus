import fileprocessing
import logging
import os
import glob
import time
import threading
import pandas as pd
import dask.dataframe as dd
import configparser
import urllib.parse
import json
import queue

# This function will be called in thread to process all files in a folder. So each folder will be processed in a different thread. Note here that each folder also maps to a single table.
def process_folder_files(thread, monitor_folder, dir_path, supported_delimiters, server, database, connectiontype, rdms_name, usr, pwd, objectexists): 
    logging.warning("Thread %s: starting", thread)
    connection = fileprocessing.getdbconnection(server
                                            , database
                                            , connectiontype
                                            , rdms_name
                                            , usr
                                            , pwd)
    engine = connection[0]
    # get list of files to process
    files = glob.glob(os.path.join(dir_path, '*'))

    # process each file into target table
    for file in files:
        if os.path.isfile(file):
            file_name = os.path.basename(file)
            schema_name = os.path.basename(os.path.dirname(dir_path))

            archive_folder = monitor_folder + '/archive/'+schema_name+'/'+str(os.path.basename(dir_path))+'/'
            error_folder = monitor_folder + '/error/'+schema_name+'/'+str(os.path.basename(dir_path))+'/'
            target_table = str(os.path.basename(dir_path).upper())

            # Check that file is not being used (in flight...)
            if not fileprocessing.check_file_status(file):
                continue

            # Check that file is not already loaded
            if fileprocessing.is_file_loaded(file_name, target_table, server, database, connectiontype, rdms_name, usr, pwd):
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
                                                           , engine)  # write date profile

                if status[0] == 1:  # if profile was not written skip this file
                    continue
                status = fileprocessing.load_data(df
                                                  , file
                                                  , target_table
                                                  , schema_name
                                                  , connection)  # load data to target database
                if status[0] == 1:  # if not able to load data, move file to error folder
                    status = fileprocessing.generate_error_log_entry(profile_hk
                                                                     , target_table
                                                                     , str(status[1])
                                                                     , engine)
                    fileprocessing.error_file(file, error_folder)
                    continue
                fileprocessing.archive_file(file, archive_folder)  # Archive the file
                fileprocessing.set_file_processed_status(profile_hk, engine)
            else:  # data was not processed into dataframe
                message = 'Error reading file into a dataframe...Make sure format is supported.'
                status = fileprocessing.generate_error_log_entry(file_name
                                                                 , target_table
                                                                 , message
                                                                 , engine)
                fileprocessing.error_file(file, error_folder)

    time.sleep(120) # wait 2 minutes before dispose of connection, give some time for archiving
    engine.dispose()

    # if this a new table, free up new table queue for next new table
    if not objectexists:
        newtablequeue.get()
        
    logging.warning("Thread %s: Ending", thread)


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
    newtablequeue = queue.Queue()


    while True:
        # Check all active threads
        active_threads = []
        for active_thread in threading.enumerate():
            active_threads.append(active_thread.name)
        
        # If a drop folder does not exist exixt the program
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
                  and str(os.path.basename(dir_root)) not in active_threads):
                      
                # If table does not exist, put it in new tables queue, we will only create one new table at a time
                tableexist = fileprocessing.check_table_exists(dir_root, targetserver, targetdatabase, connectiontype, rdms, user, password)

                # We are naming the thread with folder name (So we should have only one thread per folder)
                threadname = os.path.basename(dir_root)

                # If it is new table, and we are currently not processing a new table, put in queue
                if not tableexist and newtablequeue.empty():
                    newtablequeue.put(threadname)
                # if it is new table, and we are currently processing a new table, then skip and continue
                elif not newtablequeue.empty() and not tableexist:
                    continue
                    
                folderthread = threading.Thread(target=process_folder_files,
                                                name=threadname,
                                                args=(threadname,
                                                      watched_folder,
                                                      dir_root,
                                                      delimiters,
                                                      targetserver,
                                                      targetdatabase,
                                                      connectiontype,
                                                      rdms,
                                                      user,
                                                      password,
                                                      tableexist,))
                folderthread.start()

        time.sleep(5)  # Wait for 5 minutes before checking for new files
