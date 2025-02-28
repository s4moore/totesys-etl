from layer import db_connection
from layer import (
    get_all_rows,
    get_columns,
    get_tables,
    read_timestamp_from_s3,
    get_new_rows,
    write_df_to_pickle,
    table_to_dataframe,
    bucket_name,
)
from datetime import datetime
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
                "pkl_files_written": {table_name : pkl_file_written,
                table_name : pkl_file_written}
    """
    try:
        conn = db_connection()
        table_names = get_tables(conn)
        s3 = boto3.client("s3")
        pkl_files_written = {}
        for table in table_names:
            timestamp_from_s3 = read_timestamp_from_s3(s3, table)
            if timestamp_from_s3 == {"detail": "No timestamp exists"}:
                rows = get_all_rows(conn, table, table_names)
            else:
                rows = get_new_rows(
                    conn, table, timestamp_from_s3[table], table_names)
            columns = get_columns(conn, table, table_names)

            if rows != []:
                df = table_to_dataframe(rows, columns)
                pkl_key = write_df_to_pickle(s3, df, table, bucket_name)["key"]
                pkl_files_written[table] = pkl_key
            else:
                logging.info(f"No new data in table {table} to upload.")

        logger.info(f"Lambda executed at {datetime.now()}", exc_info=True)
        if pkl_files_written == {}:
            triggerLambda2 = False
            logging.info("SUMMARY:  No new data found"
                         " - No new files saved to s3")
        else:
            triggerLambda2 = True
            logging.info(
                "SUMMARY: New data found - List of files " +
                f"saved to s3: {pkl_files_written}"
            )
        return {
            "response": 200,
            "pkl_files_written": pkl_files_written,
            "triggerLambda2": triggerLambda2,
        }

    except Exception as e:
        logging.error(e)
        return {"response": 500, "error": e}

    finally:
        if conn:
            conn.close()
