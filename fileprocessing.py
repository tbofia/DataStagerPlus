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
import ctypes
from ctypes import wintypes
import csv

# this function returns a database engine based on config file
def getdbconnection(server, database, rdms, usr, pwd):
    # Set up the SQL Server connection

    if rdms == 'MSSQL':
        connectionstring = 'mssql+pyodbc://{}/{}?driver=ODBC+Driver+17+for+SQL+Server?trusted_connection=yes'
        connectionstring = connectionstring.format(server, database)
    if rdms == 'postgres':
        connectionstring = 'postgresql+psycopg2://{}:{}@{}:5432/{}'
        connectionstring = connectionstring.format(usr, pwd, server, database)  

    try:
        engine = sqlalchemy.create_engine(connectionstring)

    except Exception as e:
        logging.error('An exception occurred: %s', e)
        engine = None

    return engine

# this functions returns a dataframe from data at a particular file path, appropiate funstion will be called based on file extention
def prep_file(file_path):
    # Determine file extension
    file_extension = os.path.splitext(file_path)[1].lower()

    # Load the file into a Pandas DataFrame
    df_object = pd.DataFrame()
    delimiter = None

    if file_extension in ['.csv', '.txt']:
        with open(file_path, 'r') as file:
            delimiter = str(csv.Sniffer().sniff(file.read()).delimiter)
        df_object = pd.read_csv(file_path, sep=delimiter, low_memory=False, encoding='unicode_escape')
    elif file_extension == '.xlsx':
        df_object = pd.read_excel(file_path)
    elif file_extension == '.json':
        df_object = pd.read_json(file_path, lines=True)
    elif file_extension == '.xml':
        df_object = pd.read_xml(file_path)
    else:
        print(f"Unsupported file format: {file_extension}")
        df_object = -1

    # Add load_datetime and filename columns to the start of DataFrame
    if isinstance(df_object, pd.DataFrame):
        df_object.insert(0, 'load_datetime', datetime.now())
        df_object.insert(0, 'filename', os.path.basename(file_path))

    return [df_object, delimiter]

# This function creates a profile in the log table for the dataframe, returns a list of hashkey,success status and errors if any.
def write_profile_data(df_object, file_path, table_name, engine, project_name):
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
        'datastagerplushk': hashed_var,
        'datastagerplusproject': project_name,
        'filename': file_name,
        'targettablename': table_name,
        'numberofcolumns': len(df_object.columns),
        'totalrecords': len(df_object),
        'duplicaterecords': df_object.duplicated().sum(),
        'invalidcharactersrecords': 0,
        'loadsuccessstatus': 0,
        'filecreatetime': datetime.strptime(file_drop_time, "%Y-%m-%d %H:%M:%S"),
        'loadstarttime': profile_time
    }
    ret_val.append(hashed_var)
    try:
        # Insert data profiling details into the DataProfiling table
        pd.DataFrame([profiling_entry]).to_sql('datastagerpluslog'
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

# This function write log into error log table. Called when we cant write dataframe or if dataframe not created. 
def generate_error_log_entry(profile_hash, targettablename, error_message, engine):
    # Insert log entry into the LoadLog table
    ret_val = []
    log_entry = {
        'datastagerplushk': profile_hash,
        'targettablename': targettablename,
        'message': error_message,
        'errordatetime': datetime.now()
    }

    try:
        pd.DataFrame([log_entry]).to_sql('datastagerpluserrorlog'
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

# This function writes dataframe to target database table
def load_data(df_object, file_path, targettablename, engine):
    ret_val = []
    try:
        df_object.to_sql(targettablename
                         , con=engine
                         , schema='dbo'
                         , if_exists='append'
                         , index=False
                         , chunksize=10000)
        print(f"File {file_path} successfully loaded into table {targettablename}")
        ret_val.append(0)
    except Exception as e:
        logging.error('An exception occurred: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val

# Update status of dataload after writing to target table
def set_file_processed_status(profile_hk, engine):
    # update file profile status as completed
    conn = engine.connect()

    try:
        conn.execute(
            sqlalchemy.text("UPDATE admin.datastagerpluslog "
                            "SET loadsuccessstatus=:loadstatus, loadendtime=:loadtime  "
                            "WHERE datastagerplushk=:id"),
            {'id': profile_hk, 'loadstatus': 1, 'loadtime': datetime.now()})
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error('An exception occurred: %s', e)
        conn.close()
        
# Check if given table exixts        
def check_table_exists(file_path, server, database, rdms, usr, pwd):

    engine = getdbconnection(server, database, rdms, usr, pwd)

    targettable = str(os.path.basename(os.path.dirname(file_path))).upper()
    conn = engine.connect()
    tablelist = conn.execute(sqlalchemy.text("SELECT TABLE_NAME "
                                             "FROM INFORMATION_SCHEMA.TABLES "
                                             "WHERE TABLE_SCHEMA = 'dbo'"
                                             "AND TABLE_NAME=:id"), {'id': targettable}).fetchall()
    conn.close()
    return len(tablelist) > 0
    
# Archive given file by moving to different location
def archive_file(file_path, archive_path):
    # Archive the file by moving it to the archive folder
    file_name = os.path.basename(file_path)
    archive_path = os.path.join(archive_path, file_name)
    shutil.move(file_path, archive_path)

    return

# Move file to error folder
def error_file(file_path, error_path):
    # Archive the file by moving it to the archive folder
    file_name = os.path.basename(file_path)
    error_path = os.path.join(error_path, file_name)
    shutil.move(file_path, error_path)

    return


# This function will tell us if file is being used. This is to make sure that we dont process files that are still being writen (Created)
def check_file_status(file_path):
    """
    class IO_STATUS_BLOCK(ctypes.Structure):
        class _STATUS(ctypes.Union):
            _fields_ = (('Status', wintypes.LONG),
                        ('Pointer', wintypes.LPVOID))
        _anonymous_ = '_Status',
        _fields_ = (('_Status', _STATUS),
                    ('Information', wintypes.WPARAM))
        
    class FILE_PROCESS_IDS_USING_FILE_INFORMATION(ctypes.Structure):
        _fields_ = (('NumberOfProcessIdsInList', wintypes.LARGE_INTEGER),
                    ('ProcessIdList', wintypes.LARGE_INTEGER * 64))
        
    # -----------------------------------------------------------------------------
    # prepare data types for system call
    # -----------------------------------------------------------------------------

    iosb = IO_STATUS_BLOCK()
    info = FILE_PROCESS_IDS_USING_FILE_INFORMATION()
        
    # -----------------------------------------------------------------------------
    # generic strings and constants
    # -----------------------------------------------------------------------------

    ntdll = ctypes.WinDLL('ntdll')
    kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)


    INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value
    FILE_READ_ATTRIBUTES = 0x80
    FILE_SHARE_READ = 1
    OPEN_EXISTING = 3
    FILE_FLAG_BACKUP_SEMANTICS = 0x02000000

    FILE_INFORMATION_CLASS = wintypes.ULONG
    FileProcessIdsUsingFileInformation = 47

    LPSECURITY_ATTRIBUTES = wintypes.LPVOID

    # -----------------------------------------------------------------------------
    # create handle on concerned file with dwDesiredAccess == FILE_READ_ATTRIBUTES
    # -----------------------------------------------------------------------------

    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.CreateFileW.argtypes = (
        wintypes.LPCWSTR,      # In     lpFileName
        wintypes.DWORD,        # In     dwDesiredAccess
        wintypes.DWORD,        # In     dwShareMode
        LPSECURITY_ATTRIBUTES,  # In_opt lpSecurityAttributes
        wintypes.DWORD,        # In     dwCreationDisposition
        wintypes.DWORD,        # In     dwFlagsAndAttributes
        wintypes.HANDLE)       # In_opt hTemplateFile
    hFile = kernel32.CreateFileW(
        file_path, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, None, OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS, None)
    if hFile == INVALID_HANDLE_VALUE:
        raise ctypes.WinError(ctypes.get_last_error())


    PIO_STATUS_BLOCK = ctypes.POINTER(IO_STATUS_BLOCK)
    ntdll.NtQueryInformationFile.restype = wintypes.LONG
    ntdll.NtQueryInformationFile.argtypes = (
        wintypes.HANDLE,        # In  FileHandle
        PIO_STATUS_BLOCK,       # Out IoStatusBlock
        wintypes.LPVOID,        # Out FileInformation
        wintypes.ULONG,         # In  Length
        FILE_INFORMATION_CLASS)  # In  FileInformationClass

    # -----------------------------------------------------------------------------
    # system call to retrieve list of PIDs currently using the file
    # -----------------------------------------------------------------------------
    status = ntdll.NtQueryInformationFile(hFile, ctypes.byref(iosb),
                                        ctypes.byref(info),
                                        ctypes.sizeof(info),
                                        FileProcessIdsUsingFileInformation)
    pidList = info.ProcessIdList[0:info.NumberOfProcessIdsInList]
    """
    pidList = []

    return pidList
