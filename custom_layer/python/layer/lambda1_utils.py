from pg8000.exceptions import DatabaseError
from botocore.exceptions import ClientError, ParamValidationError
import botocore
from pg8000.native import identifier
import logging
import json
import pandas as pd
from io import BytesIO
from datetime import datetime

bucket_name = "terrific-totes-data-team-11"


def get_tables(conn):
    logging.info("makeing sql query")
    data = conn.run(
        """ SELECT table_name 
             FROM information_schema.tables 
           
             where table_schema='public' 
             AND table_type='BASE TABLE';"""
    )
    logging.info(data)
    # logging.info('made sql query')
    tables_list = [item[0] for item in data if item[0] != "_prisma_migrations"]
    logging.info(f"Table names created from DB:  {tables_list}")
    return tables_list


def get_all_rows(conn, table, table_list):
    """Returns rows from table

    Parameters:
        Connection: PG8000 Connection to database,
        Table (str): Table name to access in database
        Table list (list) to check if valid


    Returns:
        List (list): The lists are rows from table
    """
    if table in table_list:
        data = conn.run(f"SELECT * FROM {identifier(table)};")
        logging.info(f"All rows from {table} collected")
        return data
    else:
        logging.error(f"Table {table} not found")
        return ["Table not found"]


def get_columns(conn, table, table_list):
    """Returns columns from table

    Parameters:
        Connection: PG8000 Connection to database,
        Table (str): Table name to access in database
        Table list (list) to check if valid


    Returns:
        List (list): A list of columns
    """
    if table in table_list:
        conn.run(f"SELECT * FROM {identifier(table)};")
        columns = [col["name"] for col in conn.columns]
        logging.info(f"All collectable columns from {table} collected")
        return columns
    else:
        logging.error(f"Table {table} not found")
        return ["Table not found"]


def write_to_s3(s3, bucket_name, filename, format, data):
    """Writes to s3 bucket

     Parameters:
        s3: Boto3.client's3') connection,
        Bucket Name (str): Bucket name to write to
        Filename (str): Filename to write
        Format (str): Format to write
        Data (json): JSON of data to write

    Returns:
        Dict (dict): {"result": "Failure/Success"}
    """
    try:
        s3.put_object(Bucket=bucket_name, Key=f"{filename}.{format}", Body=data)
        logging.info(f"writing to s3 ... {filename}.{format}")
    except (ClientError, ParamValidationError) as e:
        logging.error(e)
        return {"result": "Failure"}
    return {"result": "Success"}


def read_timestamp_from_s3(s3, table):
    """Reads timestamp of given table from s3 ingestion bucket

    Parameters:
        s3: Boto3.client('s3') connection
        Table Name (str)

    Returns:
        Dictionary of format {'Table Name':'Timestamp String'}
    """
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)

        # if no files return prompt to pull all table data
        if "Contents" not in response:
            return {"detail": "No timestamp exists"}

        # get files that match with table name
        matched_files = []
        for item in response["Contents"]:
            if f"{table}." in item["Key"]:
                matched_files.append(item)

        logging.info(
            f"Searching for most recent timestamp from {table} with s3 data >>> {matched_files}"
        )
        # if no matched files exist, return prompt to pull all table data
        if not matched_files:
            return {"detail": "No timestamp exists"}

        #
        most_recent_timestamp = None
        most_recent_file = None

        for item in matched_files:
            file_name = item["Key"]
            # get the 'YYYY-MM-DD HH24:MI:SS.US' part from the filename'

            timestamp_str = file_name.split("/")[0] + " " + file_name.split("/")[1]

            if most_recent_timestamp is None or timestamp_str > most_recent_timestamp:
                most_recent_timestamp = timestamp_str
                most_recent_file = file_name

        logging.info(
            f"most recent timestamp identified as {most_recent_timestamp} from s3"
        )
        return {table: most_recent_timestamp}

    except Exception as e:
        logging.error(f"Unexpected error whilst collecting timestamp: {e}.")
        return e


def get_new_rows(conn, table, timestamp, table_list):
    """Returns rows from table

    Parameters:
        Connection: PG8000 Connection to database,
        Table (str): Table name to access in database
        Timestamp (str): format 'YYYY-MM-DD HH24:MI:SS.US'
        Table list (list) to check if valid

    Returns:
        List (list): The lists are rows from table
    """
    try:
        if table in table_list:
            data = conn.run(
                f"""SELECT * FROM {identifier(table)}
                            WHERE last_updated > to_timestamp(:timestamp,
                            'YYYY-MM-DD HH24:MI:SS.US');""",
                timestamp=timestamp,
            )
            logging.info(f"{len(data)} rows collected from {table}")
            return data
        else:
            logging.error("Table not found")
    except Exception as e:
        logging.error(e)
    return []


def write_df_to_pickle(s3, df, table_name, bucket_name):
    """Takes rows, columns, and name of a table, converts it
    to pkl file format, and uploads the file to s3 Ingestion bucket.

    Parameters:
        s3: Boto3.client('s3') connection
        Pandas DataFrame
        Table_name (str): the name of the table

    Returns:
        Dict (dict): {"result": "Failure/Success"} + "detail" if successful.
    """
    try:
        timestamp = str(timestamp_from_df(df))
        with BytesIO() as pickle:
            logging.info(f"converting {table_name} dataframe to pickle")
            df.to_pickle(pickle)
            data = pickle.getvalue()
            logging.info(f"writing {timestamp}{table_name}.pkl")
            response = write_to_s3(
                s3,
                bucket_name,
                f"{timestamp}{table_name}",
                "pkl",
                data,
            )
            if response["result"] == "Success":
                logging.info(f"{timestamp}{table_name}.pkl successfully written")
                return {
                    "result": "Success",
                    "detail": "Converted to pkl, uploaded to ingestion bucket",
                    "key": f"{timestamp}{table_name}.pkl",
                }
    except Exception as e:
        logging.error(e)
    return {"result": "Failure"}


def table_to_dataframe(rows, columns):
    """Converts rows and columns into Pandas Dataframe

    Parameters:
    Rows (list): List of lists containing values
    Columns (list): List of strings

    Returns:
    Pandas Dataframe"""
    try:
        logging.info("converting to dataframe")
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        logging.error(f"dataframe conversion unsuccessful: {e}")


def split_time_stamps(dt):
    date = dt.strftime("%Y-%m-%d")
    time = dt.strftime("%H:%M:%S.%f")
    object_key = f"{date}/{time}/"
    return object_key


def timestamp_from_df(df):
    """Gets most recent timestamp as Datetime object from DataFrame

    Parameters: Pandas Dataframe

    Returns: Datetime object"""
    try:
        timestamp = df["last_updated"].max()
        logging.info(f"{split_time_stamps(timestamp)} collected from dataframe")
        return split_time_stamps(timestamp)
    except KeyError as e:
        logging.error({"column not found": e})
