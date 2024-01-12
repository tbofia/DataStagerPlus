import datetime
import logging
import os
import time
import pandas as pd
import dask.dataframe as dd
import shutil
import pyodbc
import sqlalchemy
import hashlib
import ctypes
import csv
import urllib.parse

from ctypes import wintypes
from pathlib import Path
from datetime import datetime


# this function returns a database engine based on config file
def getdbconnection(server, database, connectiontype, rdms, usr, pwd):
    # Set up the SQL Server connection
    connectionstring = None
    engine = None

    if rdms == 'MSSQL':
        if connectiontype == 'ODBC':
            connectionstring = 'mssql+pyodbc://{}'.format(server)
        else:
            connectionstring = 'mssql+pyodbc://{}/{}?driver=ODBC+Driver+17+for+SQL+Server?trusted_connection=yes'.format(server, database)

    if rdms == 'postgres':
        connectionstring = 'postgresql+psycopg2://{}:{}@{}:5432/{}'.format(usr, pwd, server, database)  

    try:
        engine = sqlalchemy.create_engine(connectionstring, fast_executemany=True)

    except Exception as e:
        logging.error('An exception occurred while creating engine: %s', e)

    return [engine,connectionstring]

# function to compare table shemas and return any differences
def check_schema_differences(left_table, right_table, connection):
    # Query List
    l1 = []
    htmlcode = ""

    if check_table_exists(left_table, connection) and check_table_exists(right_table, connection):
        conn = connection[0].connect()
        # Query to get new columns.
        newcolumns = sqlalchemy.text("SELECT TL.COLUMN_NAME AS NewColumns "
                                     "FROM INFORMATION_SCHEMA.COLUMNS TL "
                                     "LEFT OUTER JOIN INFORMATION_SCHEMA.COLUMNS TR "
                                     "ON TL.COLUMN_NAME = TR.COLUMN_NAME "
                                     "AND TR.TABLE_NAME = '{}' "
                                     "WHERE TL.TABLE_NAME = '{}' "
                                     "AND TR.COLUMN_NAME IS NULL".format(right_table, left_table))
        h1 = '<h2>New Columns</h2>'
        t1 = {"header": h1,
              "code": newcolumns}
        l1.append(t1)
        # Query to get columns missing from new dataset.
        missingcolumns = sqlalchemy.text("SELECT TL.COLUMN_NAME AS MissingColumns "
                                         "FROM INFORMATION_SCHEMA.COLUMNS TL "
                                         "LEFT OUTER JOIN INFORMATION_SCHEMA.COLUMNS TR "
                                         "ON TL.COLUMN_NAME = TR.COLUMN_NAME "
                                         "AND TR.TABLE_NAME = '{}' "
                                         "WHERE TL.TABLE_NAME = '{}' "
                                         "AND TR.COLUMN_NAME IS NULL".format(left_table, right_table))
        h1 = '<h2>Missing Columns</h2>'
        t1 = {"header": h1,
              "code": missingcolumns}
        l1.append(t1)

        # Query to get any data type changes.
        datatypechanges = sqlalchemy.text(
            "SELECT TL.COLUMN_NAME AS ColumnName, "
            "TL.DATA_TYPE AS NewDataType, "
            "TR.DATA_TYPE AS OldDataType "
            "FROM INFORMATION_SCHEMA.COLUMNS TL "
            "INNER JOIN INFORMATION_SCHEMA.COLUMNS TR "
            "ON TL.COLUMN_NAME = TR.COLUMN_NAME "
            "AND TR.TABLE_NAME = '{}' "
            "WHERE TL.TABLE_NAME = '{}' "
            "AND TR.DATA_TYPE <> TL.DATA_TYPE".format(right_table, left_table))
        h1 = '<h2>Datatype Changes</h2>'
        t1 = {"header": h1,
              "code": datatypechanges}
        l1.append(t1)

        # Query to get column position changes
        columnpositionchanges = sqlalchemy.text(
            "SELECT TL.COLUMN_NAME AS ColumnName, "
            "TL.ORDINAL_POSITION AS NewPosition, "
            "TR.ORDINAL_POSITION AS OldPosition "
            "FROM INFORMATION_SCHEMA.COLUMNS TL "
            "INNER JOIN INFORMATION_SCHEMA.COLUMNS TR "
            "ON TL.COLUMN_NAME = TR.COLUMN_NAME "
            "AND TR.TABLE_NAME = '{}' "
            "WHERE TL.TABLE_NAME = '{}' "
            "AND TR.ORDINAL_POSITION <> TL.ORDINAL_POSITION".format(right_table, left_table))
        h1 = '<h2>Column Position Changes</h2>'
        t1 = {"header": h1,
              "code": columnpositionchanges}
        l1.append(t1)

        for le in l1:
            data = pd.read_sql(le["code"], connection[0])
            if not data.empty:
                data = data.to_html(classes='table table-stripped')
                htmlcode = htmlcode + le["header"] + data

    if htmlcode == "":
        htmlcode = None

    return htmlcode

# This function splits large file into manageable chunks
def split_large_file(file_path, folder_name, archive_path):
    folder_name = folder_name + '/'
    breakup_size = 160000000  # Number of bytes to read, this should be about 150 MB

    with open(file_path, 'r') as file:
        # memory-map the file
        base_file_name = os.path.splitext(os.path.basename(file_path))[0]
        base_file_ext = os.path.splitext(os.path.basename(file_path))[1]
        file_lines = file.readlines(breakup_size)
        i = 0
        # While we are able to read lines from file
        while len(file_lines) > 0:
            if i == 0:  # Get header from first file
                header_line = file_lines[0]
            file_name = folder_name + base_file_name + '_' + str(i) + '_' + str(breakup_size) + base_file_ext
            small_file = open(f'{file_name}', 'a')

            if i > 0:  # Place header for subsequent files
                small_file.write(header_line)

            small_file.writelines(file_lines)
            small_file.close()

            file_lines = file.readlines(breakup_size)
            i += 1

    archive_file(file_path, archive_path)
    return

# This function tries to figure out delimiter for files by looking at couple of lines
def usedefaultdelimiter(file_path,supported_delimiters):
    default = '\t'
    with open(file_path, 'r') as file:
        sample_lines = file.readlines()[0:5]

    first_line = sample_lines[1]

    for delimiter in supported_delimiters:
        delimiter = urllib.parse.unquote(delimiter)
        match = 0
        column_count = len(first_line.split(delimiter))
        number_of_lines = len(sample_lines)-1
        for line in sample_lines[1:5]:
            if len(line.split(delimiter)) == column_count:
                   match += 1
        if match ==  number_of_lines and column_count > 1:
            return delimiter
    return default



# this functions returns a dataframe from data at a particular file path, appropiate funstion will be called based on file extention
def prep_file(file_path,supported_delimiters):
    # Start of process
    profiling_start_time = datetime.now()

    # Determine file extension
    file_extension = os.path.splitext(file_path)[1].lower()

    # Load the file into a Pandas DataFrame
    df_object = pd.DataFrame()
    delimiter = None
    is_dask = False
    numberofpartitions = 0

    # Get file size in gb
    byte=Path(file_path).stat().st_size
    size_in_gb = byte/(1024 * 1024 * 1024)
    size_in_mb = byte/(1024 * 1024)

    # Get number of partitions in chunks of 100mb
    numberofpartitions = int((size_in_mb//100)+1)

    if file_extension not in ['.xlsx', '.json','.xml']:
        try:
            # Sniffer functions is acting funny with big files, lets try to sniff just from the first 10 lines
             temp_file = file_path+'.tmp'

             with open(file_path, 'r') as file:
                 lines = file.readlines()[:10]
             with open(temp_file, 'w') as file:
                 file.writelines(lines)
             with open(temp_file, 'r') as file:
                 delimiter = str(csv.Sniffer().sniff(file.read()).delimiter)

             os.remove(temp_file)
                
        except:
            os.remove(temp_file)
            delimiter = usedefaultdelimiter(file_path,supported_delimiters)
        
        # use dask dataframe for files larger that 0.5 gb
        if (1==0):
            df_object = dd.read_csv(file_path, sep=delimiter, encoding='unicode_escape', skipinitialspace = True, low_memory=False)
            is_dask = True
            
        else:
            df_object = pd.read_csv(file_path, sep=delimiter, encoding='unicode_escape', skipinitialspace = True, low_memory=False)
            

    elif file_extension == '.xlsx':
        df_object = pd.read_excel(file_path)
    elif file_extension == '.json':
        df_object = pd.read_json(file_path, lines=True)
    elif file_extension == '.xml':
        df_object = pd.read_xml(file_path)
    else:
        print(f"Unsupported file format: {file_extension}")
        df_object = -1

    mod_object =  add_meta_data(df_object, delimiter, file_path, numberofpartitions, profiling_start_time)

    df_object = mod_object[0]
    meta_data = mod_object[1]

    return [df_object, meta_data]

# Add Meta Data columns
def add_meta_data(df_object, delimiter, file_path, numberofpartitions, profiling_start_time):
    file_name = os.path.basename(file_path)
    profiling_end_time = datetime.now()
    is_dask = False

    profile_hk = file_name + str(profiling_end_time)
    profile_hk = profile_hk.encode('utf-8')
    profile_hk = hashlib.md5(profile_hk).hexdigest()

    # Add load_datetime and filename columns to the start of DataFrame
    if isinstance(df_object, dd.DataFrame):
        df_object = df_object.compute()
        is_dask = True

    if isinstance(df_object, pd.DataFrame):
        # Trim white spaces from all data in the DataFrame
        df_object = df_object.map(lambda x: x.strip() if isinstance(x, str) else x)

        # remove all extra spaces from column names
        df_object = df_object.rename(columns=lambda x: x.strip())

        df_object.insert(0, 'load_datetime', profiling_end_time)
        df_object.insert(0, 'filename', file_name)
        df_object.insert(0, 'datafilestagehk', profile_hk)

    if is_dask: # Convert back to dask
       df_object = dd.from_pandas(df_object, npartitions=numberofpartitions) 

    meta_data = {'profiling_end_time':profiling_end_time,
                 'profiling_start_time':profiling_start_time,
                 'profile_hk':profile_hk,
                 'delimiter':delimiter}

    return [df_object, meta_data]

# This function creates a profile in the log table for the dataframe, returns a list of hashkey,success status and errors if any.
def write_profile_data(df_object, meta_data, file_path, table_name, schemaname, connection):
    ret_val = []
    # Perform data profiling
    file_name = os.path.basename(file_path)

    t_obj = time.strptime(time.ctime(os.path.getmtime(file_path)))
    file_drop_time = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)

    if isinstance(df_object, pd.DataFrame):
        duplicates = df_object.duplicated().sum()
    else:
        duplicates = None

    profiling_entry = {
        'datafilestagehk': meta_data['profile_hk'],
        'filename': file_name,
        'delimiter': meta_data['delimiter'],
        'targettablename': table_name,
        'schemaname':schemaname,
        'numberofcolumns': len(df_object.columns)-2,
        'totalrecords': len(df_object),
        'duplicaterecords': duplicates,
        'invalidcharactersrecords': 0,
        'loadsuccessstatus': 0,
        'filecreatetime': datetime.strptime(file_drop_time, "%Y-%m-%d %H:%M:%S"),
        'loadstarttime': meta_data['profiling_start_time'],
        'loadtomemoryendtime':meta_data['profiling_end_time']
    }

    try:
        # Insert data profiling details into the DataProfiling table
        pd.DataFrame([profiling_entry]).to_sql('datafilestagelog'
                                               , con=connection[0]
                                               , schema='_admin'
                                               , if_exists='append'
                                               , index=False)
        ret_val.append(0)
    except Exception as e:
        logging.error('An exception occurred while trying to create profile entry: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val

# This function write log into error log table. Called when we cant write dataframe or if dataframe not created. 
def generate_error_log_entry(profile_hk, targettablename, error_message, connection):
    # Insert log entry into the LoadLog table
    ret_val = []
    log_entry = {
        'datafilestagehk': profile_hk,
        'targettablename': targettablename,
        'message': error_message,
        'errordatetime': datetime.now()
    }

    try:
        pd.DataFrame([log_entry]).to_sql('datafilestageerrorlog'
                                         , con=connection[0]
                                         , schema='_admin'
                                         , if_exists='append'
                                         , index=False)
        ret_val.append(0)
    except Exception as e:
        logging.error('An exception occurred while trying to write to error log: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val




# Update status of dataload after writing to target table
def set_file_processed_status(profile_hk, connection):
    # update file profile status as completed
    conn = connection[0].connect()

    try:
        conn.execute(
            sqlalchemy.text("UPDATE _admin.datafilestagelog "
                            "SET loadsuccessstatus=:loadstatus, loadendtime=:loadtime  "
                            "WHERE datafilestagehk=:id"),
            {'id': profile_hk, 'loadstatus': 1, 'loadtime': datetime.now()})
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error('An exception occurred while trying to update load status: %s', e)
        conn.close()
        
# Check if given table exists        
def check_table_exists(targettable, schema_name, connection):

    conn = connection[0].connect()
    tablelist = conn.execute(sqlalchemy.text("SELECT TABLE_NAME "
                                             "FROM INFORMATION_SCHEMA.TABLES WITH (NOLOCK)"
                                             "WHERE TABLE_SCHEMA =:schema "
                                             "AND TABLE_NAME=:id"), {'id': targettable,'schema':schema_name}).fetchall()
    conn.close()
    return len(tablelist) > 0
    
# Archive given file by moving to different location
def archive_file(file_path, archive_path):
    # Archive the file by moving it to the archive folder
    if not os.path.exists(archive_path):
        os.makedirs(archive_path)

    file_name = os.path.basename(file_path)
    archive_path = os.path.join(archive_path, file_name)
    shutil.move(file_path, archive_path)

    return

# Move file to error folder
def error_file(file_path, error_path):
    # move error files to error folder
    if not os.path.exists(error_path):
        os.makedirs(error_path)

    file_name = os.path.basename(file_path)
    error_path = os.path.join(error_path, file_name)
    shutil.move(file_path, error_path)

    return

# This code will check if file has already been loaded
def is_file_loaded(file_name, target_table, connection):

    conn = connection[0].connect()
    filelist = conn.execute(sqlalchemy.text("SELECT DISTINCT dataprofilingid "
                                             "FROM _admin.datafilestagelog "
                                             "WHERE filename=:file "
                                             "AND targettablename=:targettable "
                                             "AND loadsuccessstatus = 1"), {'targettable': target_table, 'file':file_name}).fetchall()
    conn.close()
    return len(filelist) > 0


# This function will tell us if file is being used. This is to make sure that we dont process files that are still being writen (Created)
def check_file_status(file_path):
   try:
       with open(file_path, 'r') as file: # If you can open the file without any exceptions, it's not open in write mode by another process.
           val =  True
   except PermissionError:
       val = False

   return val
