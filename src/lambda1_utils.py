from pg8000.exceptions import DatabaseError
from botocore.exceptions import ClientError, ParamValidationError
import botocore
from pg8000.native import identifier
import logging
import json
import pandas as pd
from io import StringIO
from datetime import datetime


def get_tables(conn):
    data = conn.run(
        """ SELECT table_name 
             FROM information_schema.tables 
             WHERE table_schema='public' 
             AND table_type='BASE TABLE';"""
    )
    tables_list = [item[0] for item in data]
    logging.info("Table names collected from DB")
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
        logging.info(f"Columns from {table} collected")
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
        logging.info(f"{filename}.{format} written to s3")
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
        filename = f"{table}_timestamp.json"
        response = s3.get_object(Bucket="nc-terraformers-ingestion", Key=filename)
        body = response["Body"]
        timestamp = json.loads(body.read().decode())
        logging.info(f"read {timestamp} from s3")
        return timestamp
    except Exception as e:
        if (
            e.response["Error"]["Code"] == "NoSuchKey"
            or e.response["Error"]["Code"] == "AccessDenied"
        ):
            logging.info(
                f"Response whilst collecting timestamp: {e}. Will create new file"
            )
            return {"detail": "No timestamp exists"}
        logging.error(
            f"Unexpected error whilst collecting timestamp: {e}. Will create new file"
        )
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


def write_df_to_csv(s3, df, table_name):
    """Takes rows, columns, and name of a table, converts it
    to csv file format, and uploads the file to s3 Ingestion bucket.

    Parameters:
        s3: Boto3.client('s3') connection
        Pandas DataFrame
        Table_name (str): the name of the table

    Returns:
        Dict (dict): {"result": "Failure/Success"} + "detail" if successful.
    """
    try:
        timestamp = str(timestamp_from_df(df))
        with StringIO() as csv:
            logging.info(f"converting {table_name} dataframe to csv")
            df.to_csv(csv, index=False)
            data = csv.getvalue()
            logging.info(f"writing {table_name}_{timestamp}.csv")
            response = write_to_s3(
                s3,
                "nc-terraformers-ingestion",
                f"{table_name}/{table_name}_{timestamp}",
                "csv",
                data,
            )
            if response["result"] == "Success":
                logging.info(f"{table_name}_{timestamp}.csv successfully written")
                return {
                    "result": "Success",
                    "detail": "Converted to csv, uploaded to ingestion bucket",
                    "key": f"{table_name}/{table_name}_{timestamp}.csv",
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


def timestamp_from_df(df):
    """Gets most recent timestamp as Datetime object from DataFrame

    Parameters: Pandas Dataframe

    Returns: Datetime object"""
    try:
        timestamp = df["last_updated"].max()
        logging.info(f"{timestamp} collected from dataframe")
        return timestamp
    except KeyError as e:
        logging.error({"column not found": e})


def write_timestamp_to_s3(s3, df, table):
    """
    Writes timestamp to s3 as JSON dictionary file

    Parameters:
    s3: Boto3.client('s3') connection
    Pandas Dataframe
    Table (str): Table name to be used in filename

    Returns:
    Dict of {"result": "Success"/"Failure"}"""
    try:
        logging.info(f"converting {table} timestamp to JSON")
        timestamp_json = json.dumps({table: str(timestamp_from_df(df))})
        filename = f"{table}_timestamp"
        write_to_s3(s3, "nc-terraformers-ingestion", filename, "json", timestamp_json)
        return {"result": "Success", "key": f"{table}_timestamp.json"}
    except Exception as e:
        logging.error(e)
        return {"result": "Failure"}
