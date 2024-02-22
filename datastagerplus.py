import fileprocessing
import logging
import os
import glob
import time
import threading
import pandas as pd
import sqlalchemy
import configparser
import queue

from pathlib import Path


def process_folder_files(thread, dir_path, config_attribs):
    logging.warning("Thread %s: starting", thread)
    connection = fileprocessing.getdbconnection(config_attribs['targetserver'], config_attribs['targetdatabase'])
    files = glob.glob(os.path.join(dir_path, '*'))

    for file in files:
        if os.path.isfile(file):
            archive_folder = os.path.dirname(dir_path) + '/archive/'  # get parent directory of 'drop'
            error_folder = os.path.dirname(dir_path) + '/error/'
            target_table = str(os.path.basename(os.path.dirname(dir_path))).upper()
            file_name = os.path.basename(file)

            # Check that file is not being used (in flight...)
            if not fileprocessing.check_file_status(file):
                continue

            # Check that file is not already loaded
            if fileprocessing.is_file_loaded(config_attribs, file_name, target_table, connection):
                fileprocessing.archive_file(file, archive_folder)  # Archive the file
                continue

            # Get file size in gb
            byte = Path(file).stat().st_size
            size_in_gb = byte / (1024 * 1024 * 1024)

            if size_in_gb > config_attribs['filesizelimit']:
                split_large_file(config_attribs, file, dir_path, archive_folder, target_table)
                while os.path.isfile(file):
                    time.sleep(10)

                files_being_created = True
                while files_being_created:  # While there are still files being created for this table lets wait
                    active_file_threads = []
                    for file_thread in threading.enumerate():
                        if len(file_thread.name.split('%')) == 2:  # This will get all active file creation threads
                            table_files = file_thread.name.split('%')
                            if table_files[0] == target_table:
                                active_file_threads.append(file_thread.name)

                    if len(active_file_threads) > 0:
                        files_being_created = True
                        time.sleep(30)
                    else:
                        files_being_created = False

                break
            # Here we try to create a dataframe from current file
            data_object = fileprocessing.prep_file(config_attribs
                                                   , dir_path
                                                   , file
                                                   , target_table
                                                   , connection)
            df = data_object[0]  # process file i.e. read into dataframe
            delimiter = data_object[1]
            error_lines = data_object[2]

            if isinstance(df, pd.DataFrame):  # if a dataframe was returned
                status = fileprocessing.write_profile_data(config_attribs
                                                           , df
                                                           , delimiter
                                                           , file
                                                           , target_table
                                                           , error_lines
                                                           , connection[0]
                                                           )  # write date profile
                profile_hk = status[0]
                if status[1] == 1:  # if profile was not written skip this file
                    continue
                # load data to target database
                status = load_data(df, file, target_table, connection)

                # If Failed to load file for error other than schema
                if status[0] == 1:  # if not able to load data, move file to error folder
                    error_type = 1
                    status = fileprocessing.generate_error_log_entry(config_attribs
                                                                     , profile_hk
                                                                     , target_table
                                                                     , str(status[1])
                                                                     , connection[0])
                    fileprocessing.error_file(file, error_folder)
                    fileprocessing.send_notifications(config_attribs
                                                      , error_type
                                                      , profile_hk
                                                      , target_table
                                                      , file
                                                      , connection)
                    continue

                # if previous file was not processed to schema or data issues skip the file
                if status[0] == 2 or status[0] == 3:  # if not able to load data, move file to error folder
                    error_type = status[0]
                    temp_table = status[2]

                    fileprocessing.error_file(file, error_folder)
                    status = fileprocessing.generate_error_log_entry(config_attribs
                                                                     , profile_hk
                                                                     , target_table
                                                                     , str(status[1])
                                                                     , connection[0])
                    fileprocessing.send_notifications(config_attribs
                                                      , error_type
                                                      , profile_hk
                                                      , target_table
                                                      , file
                                                      , connection)

                    deletetablequeue.put(temp_table)

                    # let's wait for the table to be deleted
                    while fileprocessing.check_table_exists(temp_table, connection):
                        time.sleep(5)
                    continue

                fileprocessing.archive_file(file, archive_folder)  # Archive the file
                # Only move to next file if archiving is done
                while os.path.isfile(file):
                    time.sleep(10)
                fileprocessing.set_file_processed_status(config_attribs, profile_hk, connection)
            else:  # data was not processed into dataframe
                error_type = 4
                message = 'Error reading file into a dataframe...Make sure format is supported.'
                status = fileprocessing.generate_error_log_entry(config_attribs
                                                                 , file_name
                                                                 , target_table
                                                                 , message
                                                                 , connection[0])
                fileprocessing.error_file(file, error_folder)
                fileprocessing.send_notifications(config_attribs
                                                  , error_type
                                                  , file_name
                                                  , target_table
                                                  , file
                                                  , connection)

    connection[0].dispose()

    logging.warning("Thread %s: Ending", thread)


def load_data(dataframe, file_path, table_name, connection):
    ret_val = []
    schemacompare = None
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    full_file_name = file_name + os.path.splitext(os.path.basename(file_path))[1]
    temp_table = table_name + '_' + file_name.replace(".", "").replace("-", "")

    # If temp table for a file persists from last attempt, someone needs to delete manually
    if fileprocessing.check_table_exists(temp_table, connection):
        ret_val.append(2)
        ret_val.append("Temp Table was not cleaned up from last Run.")
        ret_val.append(temp_table)

        return ret_val

    targettableexits = fileprocessing.check_table_exists(table_name, connection)

    # If this is first file for given table set up so that it is loaded directly
    if not targettableexits:
        temp_table = table_name

    # Here we get create table statement and put in queue.
    createtablescript = str(pd.io.sql.get_schema(dataframe, 'load_table_name', con=connection[0])).replace(
        'load_table_name', 'dbo.' + temp_table)
    createtablequeue.put(createtablescript)
    # let's wait for the table to be created
    while not fileprocessing.check_table_exists(temp_table, connection):
        time.sleep(5)

    try:
        dataframe.to_sql(temp_table
                         , con=connection[0]
                         , schema='dbo'
                         , if_exists='append'
                         , index=False
                         , chunksize=10000)

        if targettableexits:
            schemacompare = fileprocessing.check_schema_differences(temp_table, table_name, connection)

        # If table exists and there are no schema differences then copy data into main table
        if targettableexits and schemacompare is None:
            # Insert data from temp table to actual target table
            insert_statement = "INSERT INTO dbo.{} SELECT * FROM dbo.[{}]".format(table_name, temp_table)
            conn = connection[0].connect()
            conn.execute(
                sqlalchemy.text(insert_statement))
            conn.commit()
            conn.close()

            deletetablequeue.put(temp_table)

        if schemacompare is not None:
            ret_val.append(3)
            ret_val.append("Schema Differences Detected")
            ret_val.append(temp_table)
        else:
            print(f"Successfully loaded File '{full_file_name}' into table {table_name}")
            ret_val.append(0)

    except Exception as e:
        logging.error('An exception occurred: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val


# This function will continuously run to drop tmp tables, this is to deal with deadlock issues when multiple tables
# are being dropped
def delete_load_table(connection):
    while True:
        # Drop temp table is insert was successful
        if not deletetablequeue.empty():
            tablename = deletetablequeue.get()
            if fileprocessing.check_table_exists(tablename, connection):
                drop_table = "DROP TABLE {}".format(tablename)

                conn = connection[0].connect()
                conn.execute(sqlalchemy.text(drop_table))
                conn.commit()
                conn.close()
        time.sleep(10)


# This function will continuously run to create tmp tables, this is to deal with deadlock issues when multiple tables
# are being created
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


def create_file_chunk(file_number, file_path, header_line, file_lines):
    small_file = open(f'{file_path}', 'a')

    if file_number > 0:  # Place header for subsequent files
        small_file.write(header_line)

    small_file.write("".join(file_lines))  # small_file.writelines(file_lines)

    small_file.close()

    return


def manage_create_file_queue(thread):
    while True:
        if not createfilequeue.empty():
            active_file_threads = []
            for file_thread in threading.enumerate():
                if len(file_thread.name.split('%')) == 2:  # This will get all active file creation threads
                    active_file_threads.append(file_thread.name)

            if len(active_file_threads) < 5:  # if we have less than five file creation threads add
                fileattribs = createfilequeue.get()
                thread = fileattribs[0] + '%' + str(fileattribs[1])
                createfilethread = threading.Thread(target=create_file_chunk,
                                                    name=thread,
                                                    args=(fileattribs[1],
                                                          fileattribs[2],
                                                          fileattribs[3],
                                                          fileattribs[4],))
                createfilethread.start()
        time.sleep(5)


# This function splits large file into manageable chunks
def split_large_file(config_attribs, file_path, folder_name, archive_path, table_name):
    folder_name = folder_name + '/'
    breakup_size = config_attribs['chunksize'] * 1024 * 1024  # Convert MB to bytes

    with open(file_path, 'r') as file:
        # memory-map the file
        base_file_name = os.path.splitext(os.path.basename(file_path))[0]
        base_file_ext = os.path.splitext(os.path.basename(file_path))[1]
        file_lines = file.readlines(breakup_size)
        file_number = 0
        # While we are able to read lines from file
        while len(file_lines) > 0:
            if file_number == 0:  # Get header from first file
                header_line = file_lines[0]
            file_name = folder_name + base_file_name + '_' + str(file_number) + '_' + str(breakup_size) + base_file_ext

            # I only want to have 10 chunks in queue at a given time, this is to manage memory
            while createfilequeue.qsize() > 9:
                time.sleep(30)

            createfilequeue.put([table_name, file_number, file_name, header_line, file_lines])

            file_lines = file.readlines(breakup_size)
            file_number += 1

    fileprocessing.archive_file(file_path, archive_path)
    return


if __name__ == "__main__":
    # get information from configuration file.
    config = configparser.RawConfigParser()
    config.read('.config')

    config_attribs = {'targetserver': config['DATABASE_SERVER']['SERVER'],
                      'targetdatabase': config['DATABASE_SERVER']['DATABASE'],
                      'monitor_folder': config['FILE_PATH']['ROOTDROPFOLDER'],
                      'email': config['COMMUNICATION']['EMAIL'],
                      'log_schema': config['ADMINISTRATION']['SCHEMA_NAME'],
                      'log_table': config['ADMINISTRATION']['LOG_TABLE'],
                      'error_log_table': config['ADMINISTRATION']['ERROR_LOG'],
                      'columns_table': config['ADMINISTRATION']['TABLE_COLUMNS'],
                      'chunksize': float(config['MISC']['SPLITSIZE']),
                      'filesizelimit': float(config['MISC']['FILELIMIT'])
                      }

    # This will be token used to determine which new table is in creation
    deletetablequeue = queue.Queue()
    createtablequeue = queue.Queue()
    createfilequeue = queue.Queue()

    while True:
        # Check all active threads
        active_threads = []
        for active_thread in threading.enumerate():
            active_threads.append(active_thread.name)

        # Monitor root folder and all sub folders and for each path check for files
        for (dir_root, dir_name, file_list) in os.walk(config_attribs['monitor_folder']):
            # if the path is 'drop' folder
            # and 'drop' folder not in root,
            # and we are just one level deep from root ,
            # and folder is not empty,
            # and folder is not currently being processed, start a thread to process files in the folder
            if ((((os.path.basename(dir_root) == 'drop')
                  and (config_attribs['monitor_folder'] != os.path.dirname(dir_root))
                  and (dir_root.count(os.path.sep) == 2))
                 and len(file_list) != 0)
                    and str(os.path.basename(os.path.dirname(dir_root))) not in active_threads):

                connection = fileprocessing.getdbconnection(config_attribs['targetserver'],
                                                            config_attribs['targetdatabase'])
                # We are naming the thread with folder name (So we should have only one thread per folder)

                # Check if there is a load table delete thread
                if 'datafilestage_delete_load_table' not in active_threads:
                    threadname = 'datafilestage_delete_load_table'
                    deletethread = threading.Thread(target=delete_load_table,
                                                    name=threadname,
                                                    args=(connection,))
                    deletethread.start()

                # Check if there is a load table create thread
                if 'datafilestage_create_load_table' not in active_threads:
                    threadname = 'datafilestage_create_load_table'
                    createthread = threading.Thread(target=create_load_table,
                                                    name=threadname,
                                                    args=(connection,))
                    createthread.start()

                # Check if there is a load table create thread
                if 'datafilestage_create_file' not in active_threads:
                    threadname = 'datafilestage_create_file'
                    filecreationthread = threading.Thread(target=manage_create_file_queue,
                                                          name=threadname,
                                                          args=(threadname,))
                    filecreationthread.start()

                threadname = str(os.path.basename(os.path.dirname(dir_root)))
                folderthread = threading.Thread(target=process_folder_files,
                                                name=threadname,
                                                args=(threadname,
                                                      dir_root,
                                                      config_attribs,))
                folderthread.start()

        time.sleep(5)  # Wait for 5 minutes before checking for new files
