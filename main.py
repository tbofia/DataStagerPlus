import fileprocessing
import logging
import os
import glob
import time
import threading
import pandas as pd
import configparser


def process_folder_files(thread, dir_path, server, database, project):
    logging.warning("Thread %s: starting", thread)
    connection = fileprocessing.getdbconnection(server, database)
    files = glob.glob(os.path.join(dir_path, '*'))

    for file in files:
        if os.path.isfile(file):
            archive_folder = os.path.dirname(dir_path) + '\\archive\\'  # get parent directory of 'drop'
            error_folder = os.path.dirname(dir_path) + '\\error\\'
            target_table = str(os.path.basename(os.path.dirname(dir_path))).upper()

            df = fileprocessing.prep_file(file)  # process file i.e. read into dataframe
            if isinstance(df, pd.DataFrame):  # if a dataframe was returned
                status = fileprocessing.write_profile_data(df
                                                           , file
                                                           , target_table
                                                           , connection[0]
                                                           , project)  # write date profile
                profile_hk = status[0]
                if status[1] == 1:  # if profile was not written skip this file
                    continue
                status = fileprocessing.load_data(df, file, target_table, connection[0])  # load data to target database
                if status[0] == 1:  # if not able to load data, move file to error folder
                    status = fileprocessing.generate_error_log_entry(profile_hk, target_table, str(status[1]),
                                                                     connection[0])
                    fileprocessing.error_file(file, error_folder)
                    continue
                fileprocessing.archive_file(file, archive_folder)  # Archive the file
                fileprocessing.set_file_processed_status(profile_hk, connection)
            else:  # data was not processed into dataframe
                message = 'Error reading file into a dataframe...Make sure format is supported.'
                file_name = os.path.basename(file)
                status = fileprocessing.generate_error_log_entry(file_name, target_table, message, connection[0])
                fileprocessing.error_file(file, error_folder)

    time.sleep(120) # wait 2 minutes before dispose of connection, give some time for archiving
    connection[0].dispose()
    logging.warning("Thread %s: Ending", thread)


if __name__ == "__main__":
    # get information from configuration file.
    config = configparser.RawConfigParser()
    config.read('.config')
    targetserver = config['DATABASE_SERVER']['SERVER']
    targetdatabase = config['DATABASE_SERVER']['DATABASE']
    monitor_folder = config['FILE_PATH']['ROOTDROPFOLDER']
    project_name = config['DATALOADX_PROJECT']['PROJECT_NAME']

    while True:
        # Check all active threads
        active_threads = []
        for active_thread in threading.enumerate():
            active_threads.append(active_thread.name)

        # Monitor root folder and all sub folders and for each path check for files
        for (dir_root, dir_name, file_list) in os.walk(monitor_folder):
            # if the path is 'drop' folder
            # and 'drop' folder not in root,
            # and we are just one level deep from root ,
            # and folder is not empty,
            # and folder is not currently being processed, start a thread to process files in the folder
            if ((((os.path.basename(dir_root) == 'drop')
                  and (monitor_folder != os.path.dirname(dir_root))
                  and (dir_root.count(os.path.sep) == 1))
                 and len(file_list) != 0)
                    and str(os.path.basename(os.path.dirname(dir_root))) not in active_threads):
                # We are naming the thread with folder name (So we should have only one thread per folder)
                threadname = str(os.path.basename(os.path.dirname(dir_root)))
                folderthread = threading.Thread(target=process_folder_files,
                                                name=threadname,
                                                args=(threadname,
                                                      dir_root,
                                                      targetserver,
                                                      targetdatabase,
                                                      project_name,))
                folderthread.start()

        time.sleep(5)  # Wait for 5 minutes before checking for new files
