import datetime
import logging
import os
import time
import pandas as pd
import shutil
import pyodbc
import sqlalchemy
from datetime import datetime
import hashlib


def getdbconnection(server, database):
    # Set up the SQL Server connection
    ret_val = []

    connectionstring = 'mssql+pyodbc://{}/{}?driver=ODBC+Driver+17+for+SQL+Server'
    connectionstring = connectionstring.format(server, database)

    try:
        engine = sqlalchemy.create_engine(connectionstring)
        ret_val.append(engine)

    except Exception as e:
        logging.error('An exception occurred: %s', e)
        engine = None
        ret_val.append(engine)

    return ret_val


def prep_file(file_path):
    # Determine file extension
    file_extension = os.path.splitext(file_path)[1].lower()

    # Load the file into a Pandas DataFrame
    if file_extension == '.csv':
        delimiter = ','
        dataframe = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)
    elif file_extension == '.txt':
        delimiter = '\t'
        dataframe = pd.read_csv(file_path, delimiter=delimiter, low_memory=False)
    elif file_extension == '.xlsx':
        dataframe = pd.read_excel(file_path)
    elif file_extension == '.json':
        dataframe = pd.read_json(file_path, lines=True)
    elif file_extension == '.xml':
        dataframe = pd.read_xml(file_path)
    else:
        print(f"Unsupported file format: {file_extension}")
        return -1
    # Add load_datetime and filename columns to the DataFrame
    dataframe['load_datetime'] = datetime.now()
    dataframe['filename'] = os.path.basename(file_path)

    return dataframe


def write_profile_data(dataframe, file_path, table_name, engine, project_name):
    ret_val = []
    # Perform data profiling
    file_name = os.path.basename(file_path)

    t_obj = time.strptime(time.ctime(os.path.getmtime(file_path)))
    file_drop_time = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

    profile_time = datetime.now()
    profile_hk = file_name + str(profile_time)

    profile_hk = profile_hk.encode('utf-8')
    hashed_var = hashlib.md5(profile_hk).hexdigest()

    profiling_entry = {
        'DataloadX_HK': hashed_var,
        'DataLoadX_Project': project_name,
        'Filename': file_name,
        'TargetTableName': table_name,
        'NumberOfColumns': len(dataframe.columns),
        'TotalRecords': len(dataframe),
        'DuplicateRecords': dataframe.duplicated().sum(),
        'InvalidCharactersRecords': 0,
        'LoadSuccessStatus': 0,
        'FileDropTime': datetime.strptime(file_drop_time, "%Y-%m-%d %H:%M:%S"),
        'ProfileCreateTime': profile_time
    }
    ret_val.append(hashed_var)
    try:
        # Insert data profiling details into the DataProfiling table
        pd.DataFrame([profiling_entry]).to_sql('DATALOADX_LOG'
                                               , con=engine
                                               , schema='admin'
                                               , if_exists='append'
                                               , index=False)
        ret_val.append(0)
    except Exception as e:
        logging.error('An exception occurred: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val


def generate_error_log_entry(profile_hash, table_name, error_message, engine):
    # Insert log entry into the LoadLog table
    ret_val = []
    log_entry = {
        'DataloadX_HK': profile_hash,
        'TargetTableName': table_name,
        'Message': error_message,
        'ErrorDateTime': datetime.now()
    }

    try:
        pd.DataFrame([log_entry]).to_sql('DATALOADX_ERROR_LOG'
                                         , con=engine
                                         , schema='admin'
                                         , if_exists='append'
                                         , index=False)
        ret_val.append(0)
    except Exception as e:
        logging.error('An exception occurred: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val


def load_data(dataframe, file_path, table_name, engine):
    ret_val = []
    try:
        dataframe.to_sql(table_name
                         , con=engine
                         , schema='dbo'
                         , if_exists='append'
                         , index=False
                         , chunksize=10000)
        print(f"File {file_path} successfully loaded into table {table_name}")
        ret_val.append(0)
    except Exception as e:
        logging.error('An exception occurred: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val


def set_file_processed_status(profile_hk, engine):
    # update file profile status as completed
    conn = engine[0].connect()

    try:
        conn.execute(
            sqlalchemy.text("UPDATE admin.DATALOADX_LOG "
                            "SET LoadSuccessStatus=:loadstatus, LoadCompleteTime=:loadtime  "
                            "WHERE DataloadX_HK=:id"),
            {'id': profile_hk, 'loadstatus': 1, 'loadtime': datetime.now()})
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error('An exception occurred: %s', e)
        conn.close()


def archive_file(file_path, archive_path):
    # Archive the file by moving it to the archive folder
    file_name = os.path.basename(file_path)
    archive_path = os.path.join(archive_path, file_name)
    shutil.move(file_path, archive_path)

    return


def error_file(file_path, error_path):
    # Archive the file by moving it to the archive folder
    file_name = os.path.basename(file_path)
    error_path = os.path.join(error_path, file_name)
    shutil.move(file_path, error_path)

    return
