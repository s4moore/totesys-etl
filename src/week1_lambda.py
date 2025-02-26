from src.lambda1_connection import db_connection
from src.lambda1_utils import (
    get_all_rows,
    get_columns,
    write_to_s3,
    get_tables,
    read_timestamp_from_s3,
    get_new_rows,
    write_df_to_csv,
    table_to_dataframe,
    timestamp_from_df,
    write_timestamp_to_s3,
)
from datetime import datetime
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel("INFO")


def lambda_handler(event, context):
    """

    Arguments:
    Event: Any
    Context: Any

    Returns:
    {"response": 200,
                "csv_files_written": {table_name : csv_file_written, table_name : csv_file_written},
                "timestamp_json_files_written": timestamp_json_files_written (list)}
    """
    try:
        conn = db_connection()
        table_names = get_tables(conn)
        s3 = boto3.client("s3")
        csv_files_written = {}
        timestamp_json_files_written = []
        for table in table_names:
            timestamp_from_s3 = read_timestamp_from_s3(s3, table)
            if timestamp_from_s3 == {"detail": "No timestamp exists"}:
                rows = get_all_rows(conn, table, table_names)
            else:
                rows = get_new_rows(conn, table, timestamp_from_s3[table], table_names)
            columns = get_columns(conn, table, table_names)

            if rows != []:
                df = table_to_dataframe(rows, columns)
                csv_key = write_df_to_csv(s3, df, table)["key"]
                csv_files_written[table] = csv_key
                json_key = write_timestamp_to_s3(s3, df, table)["key"]
                timestamp_json_files_written.append(json_key)
            else:
                logging.info(f"No new data in table {table} to upload.")

        logger.info(f"Lambda executed at {datetime.now()}", exc_info=True)
        if csv_files_written == {}:
            triggerLambda2 = False
        else:
            triggerLambda2 = True
        return {
            "response": 200,
            "csv_files_written": csv_files_written,
            "timestamp_json_files_written": timestamp_json_files_written,
            "triggerLambda2": triggerLambda2,
        }

    except Exception as e:
        logging.error(e)
        return {"response": 500, "error": e}

    finally:
        if conn:
            conn.close()
