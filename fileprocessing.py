import datetime
import logging
import os
import time
import pandas as pd
import shutil
import urllib.parse
import pyodbc
import sqlalchemy
from datetime import datetime
import hashlib
import csv
import win32com.client
import contextlib


def getdbconnection(server, database):
    # Set up the SQL Server connection
    ret_val = []

    connectionstring = 'mssql+pyodbc://{}/{}?driver=ODBC+Driver+17+for+SQL+Server'
    connectionstring = connectionstring.format(server, database)

    try:
        engine = sqlalchemy.create_engine(connectionstring, fast_executemany=True)
        ret_val.append(engine)

    except Exception as e:
        logging.error('An exception occurred: %s', e)
        engine = None
        ret_val.append(engine)

    return ret_val


# This function will delete first and last lines from a file
def delete_first_and_last_lines(file_path, modified_file):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Check if there are at least two lines
    if len(lines) >= 2:
        # Remove the first and last lines
        modified_lines = lines[1:-1]
    else:
        modified_lines = []

    with open(modified_file, 'w') as file:
        file.writelines(modified_lines)


# This function tries to figure out delimiter for files by looking at couple of lines
def use_default_delimiter(file_path):
    default = '\t'
    with open(file_path, 'r') as file:
        sample_lines = file.readlines()[0:5]

    first_line = sample_lines[1]

    for delimiter in [',', '|', '\t']:
        delimiter = urllib.parse.unquote(delimiter)
        match = 0
        column_count = len(first_line.split(delimiter))
        number_of_lines = len(sample_lines) - 1
        for line in sample_lines[1:5]:
            if len(line.split(delimiter)) == column_count:
                match += 1
        if match == number_of_lines and column_count > 1:
            return delimiter
    return default


def prep_file(config_params, dir_path, file_path, targettable, connection):
    # Determine file extension
    file_extension = os.path.splitext(file_path)[1].lower()
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    temp_file = file_path + '.tmp'  # This will be used to put sample file data for sniffing delimiter
    load_file = file_path
    modified_file = os.path.dirname(file_path) + '/' + file_name + '_MOD' + file_extension
    error_records_path = os.path.dirname(dir_path) + '/' + file_name + '_ERROR_RECORDS' + file_extension

    # Load the file into a Pandas DataFrame
    df_object = pd.DataFrame()
    delimiter = None
    header_line = 0
    error_lines = 0

    # Here we determine if the table in question has pre-defined columns and file does not have header line at top
    pre_created_table = check_table_columns_defined(config_params, targettable, connection)

    if file_extension not in ['.xlsx', '.json', '.xml']:
        if len(pre_created_table[1]) == 0:
            try:
                # Sniffer functions is acting funny with big files, lets try to sniff just from the first 10 lines

                with open(file_path, 'r') as file:
                    lines = file.readlines()[:10]
                with open(temp_file, 'w') as file:
                    file.writelines(lines)
                with open(temp_file, 'r') as file:
                    delimiter = str(csv.Sniffer().sniff(file.read()).delimiter)

                os.remove(temp_file)

            except:
                os.remove(temp_file)
                delimiter = use_default_delimiter(file_path)

        else:
            # If APCD table lets remove header and trailer lines
            if pre_created_table[2] == 'APCD':
                delete_first_and_last_lines(file_path, modified_file)
                load_file = modified_file

            delimiter = pre_created_table[0]
            header_line = None

        # Try reading unicode data
        try:
            with open(error_records_path, 'w') as log:
                with contextlib.redirect_stderr(log):
                    df_object = pd.read_csv(load_file
                                            , sep=delimiter
                                            , low_memory=False
                                            , header=header_line
                                            , dtype=str
                                            , on_bad_lines='warn')
        except:
            df_object = None

        if not isinstance(df_object, pd.DataFrame):
            # Try reading none unicode data
            try:
                with open(error_records_path, 'w') as log:
                    with contextlib.redirect_stderr(log):
                        df_object = pd.read_csv(load_file
                                                , sep=delimiter
                                                , low_memory=False
                                                , encoding='unicode_escape'
                                                , on_bad_lines='warn'
                                                , header=header_line, dtype=str)

            except:
                df_object = None

        # Let's Count number of records that have errors.
        with open(error_records_path, 'r') as err_file:
            error_lines = len(err_file.readlines())
        if error_lines == 0:  # Delete error-records file if no error recorded
            os.remove(error_records_path)

        # This is APCD file that header and trailer have been stripped from
        if os.path.exists(modified_file):
            os.remove(modified_file)

        # Update header with what is stored in database table
        if len(pre_created_table[1]) > 0:
            df_object.columns = pre_created_table[1]

    elif file_extension == '.xlsx':
        df_object = pd.read_excel(load_file, dtype=str)
    elif file_extension == '.json':
        df_object = pd.read_json(load_file, lines=True, dtype=str)
    elif file_extension == '.xml':
        df_object = pd.read_xml(load_file, dtype=str)
    else:
        print(f"Unsupported file format: {file_extension}")
        df_object = None

    # Add load_datetime and filename columns to the start of DataFrame
    if isinstance(df_object, pd.DataFrame):
        df_object.insert(0, 'load_datetime', datetime.now())
        df_object.insert(0, 'filename', os.path.basename(file_path))

    return [df_object, delimiter, error_lines]


def write_profile_data(config_params, dataframe, delimiter, file_path, targettable, error_lines, engine):
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
        'DataloadXHK': hashed_var,
        'Filename': file_name,
        'Delimiter': delimiter,
        'TargetTableName': targettable,
        'NumberOfColumns': len(dataframe.columns),
        'TotalRecords': len(dataframe),
        'DuplicateRecords': dataframe.duplicated().sum(),
        'InvalidCharactersRecords': 0,
        'ErrorRecords': error_lines,
        'LoadSuccessStatus': 0,
        'FileCreateTime': datetime.strptime(file_drop_time, "%Y-%m-%d %H:%M:%S"),
        'LoadStartTime': profile_time
    }
    ret_val.append(hashed_var)
    try:
        # Insert data profiling details into the DataProfiling table
        pd.DataFrame([profiling_entry]).to_sql(config_params['log_table']
                                               , con=engine
                                               , schema=config_params['log_schema']
                                               , if_exists='append'
                                               , index=False)
        ret_val.append(0)
    except Exception as e:
        logging.error('An exception occurred: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val


def generate_error_log_entry(config_params, profile_hash, targettable, error_message, engine):
    # Insert log entry into the LoadLog table
    ret_val = []
    log_entry = {
        'DataloadXHK': profile_hash,
        'TargetTableName': targettable,
        'Message': error_message,
        'EmailSent': False,
        'ErrorDateTime': datetime.now()
    }

    try:
        pd.DataFrame([log_entry]).to_sql(config_params['error_log_table']
                                         , con=engine
                                         , schema=config_params['log_schema']
                                         , if_exists='append'
                                         , index=False)
        ret_val.append(0)
    except Exception as e:
        logging.error('An exception occurred: %s', e)
        ret_val.append(1)
        ret_val.append(e)

    return ret_val


def check_table_exists(targettable, connection):
    conn = connection[0].connect()
    tablelist = conn.execute(sqlalchemy.text("SELECT TABLE_NAME "
                                             "FROM INFORMATION_SCHEMA.TABLES "
                                             "WHERE TABLE_SCHEMA = 'dbo'"
                                             "AND TABLE_NAME=:id"), {'id': targettable}).fetchall()
    conn.close()
    return len(tablelist) > 0


def check_table_columns_defined(config_params, targettable, connection):
    delimiter = None
    columnlist = []
    tabletype = None

    conn = connection[0].connect()

    result = conn.execute(sqlalchemy.text("SELECT TableName,Delimiter,ColumnName,TableType "
                                          "FROM {}.{} "
                                          "WHERE TableName  = '{}' "
                                          "ORDER BY Position".format(config_params['log_schema'], config_params['columns_table'], targettable))).fetchall()

    for r in result:
        delimiter = r[1]
        columnlist.append(r[2])
        tabletype = r[3]

    return [delimiter, columnlist, tabletype]


def set_file_processed_status(config_params, profile_hk, connection):
    # update file profile status as completed
    conn = connection[0].connect()

    try:
        conn.execute(
            sqlalchemy.text("UPDATE {}.{} "
                            "SET LoadSuccessStatus=:loadstatus, LoadEndTime=:loadtime  "
                            "WHERE DataloadXHK=:id".format(config_params['log_schema'], config_params['log_table'])),
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


def send_notifications(config_params, error_code, profile_hk, targettable, file_path, connection):
    outlook = win32com.client.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)
    mail.To = config_params['email']

    file_name = os.path.splitext(os.path.basename(file_path))[0]
    file_ext = os.path.splitext(os.path.basename(file_path))[1]

    if error_code == 1:
        emailbody = "Failed to load file '{}{}' into table: {}".format(file_name, file_ext, targettable)
        mail.Subject = "DATALOADX: File Load Error: See log table for error details."
        mail.Body = emailbody
        try:
            mail.Send()
            update_error_log(config_params, profile_hk, connection)
        except:
            print('Could not send Error Email Notification')

    if error_code in (2, 3):

        temp_table = targettable + '_' + file_name.replace(".", "").replace("-", "")
        # If target table exists and temp table exists check schema diff
        schema_differences = check_schema_differences(temp_table, targettable, connection)
        if schema_differences is not None:
            mail.Subject = "DATALOADX: Failed to load file '{}{}' into table: {} due to schema differences.".format(
                file_name,
                file_ext,
                targettable)
            mail.HTMLbody = schema_differences
            try:
                mail.Send()
                update_error_log(config_params, profile_hk, connection)
            except:
                print('Could not send Error Email Notification')

    if error_code == 4:
        emailbody = ("Could not create dataframe from file '{}{}', make sure file is correctly configured with headers "
                     "for table: {}").format(
            file_name, file_ext, targettable)
        mail.Subject = "DATALOADX: File Load Error: Invalid file format"
        mail.Body = emailbody
        try:
            mail.Send()
            update_error_log(config_params, profile_hk, connection)
        except:
            print('Could not send Error Email Notification')


def update_error_log(config_params, profile_hk, connection):
    # update file profile status as completed
    conn = connection[0].connect()

    try:
        conn.execute(
            sqlalchemy.text("UPDATE {}.{} "
                            "SET EmailSent=:sentval  "
                            "WHERE DataloadXHK=:id".format(config_params['log_schema'], config_params['error_log_table'])),
            {'id': profile_hk, 'sentval': True})
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error('An exception occurred while updating error log: %s', e)
        conn.close()


def error_file(file_path, error_path):
    # error_type: 1: large file, 2: Could not write panda to table of copy data
    # 3: No DataFrame created Invalid File type

    # Archive the file by moving it to the archive folder
    file_name = os.path.basename(file_path)
    error_path = os.path.join(error_path, file_name)
    shutil.move(file_path, error_path)

    return


# This code will check if file has already been loaded
def is_file_loaded(config_params, file_name, targettable, connection):
    conn = connection[0].connect()
    filelist = conn.execute(sqlalchemy.text("SELECT DISTINCT dataprofilingid "
                                            "FROM {}.{} "
                                            "WHERE filename=:file "
                                            "AND targettablename=:targettable "
                                            "AND loadsuccessstatus = 1".format(config_params['log_schema'], config_params['log_table'])),
                            {'targettable': targettable, 'file': file_name}).fetchall()
    conn.close()
    return len(filelist) > 0


# This function will tell us if file is being used. This is to make sure that we dont process files that are still
# being writen (Created)
def check_file_status(file_path):
    try:
        with open(file_path,
                  'r') as file:  # If you can open the file without any exceptions, it's not open in write mode by
            # another process.
            val = True
    except PermissionError:
        val = False

    return val
