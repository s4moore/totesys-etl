import awswrangler as wr
import pg8000.native
from layer import db_connection
from pg8000.exceptions import DatabaseError
from botocore.errorfactory import ClientError
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def connect_to_database() -> pg8000.native.Connection:
    """Establish a database connection."""
    try:
        con = db_connection(database="read-database")

        # Prevent conflict with date format
        con.run("SET datestyle = 'ISO, YMD';")

        return con

    except DatabaseError as e:
        logger.error('DatabaseError: Failed to connect '
                     f'to the database. Error: {e}')

        return None


def get_parquet_data(table: str) -> pd.DataFrame:
    """Read Parquet data from S3."""
    try:
        return wr.s3.read_parquet_table(database="load_db", table=table)

    except ClientError as e:
        logger.error(f"ClientError: Failed to read {table} "
                     f"from S3. Error: {e}")

        return None


def get_existing_table_data(
            con: pg8000.native.Connection, table: str
        ) -> pd.DataFrame:
    """Fetch existing table data from the database."""
    try:
        existing_rows = wr.postgresql.read_sql_table(
            table=table,
            schema="project_team_11",
            con=con
        )

        return existing_rows.drop(columns=["sales_record_id"])

    except DatabaseError as e:
        logger.error(f"DatabaseError: Failed to read {table} from database."
                     f"Error: {e}")

        return None


def merge_and_remove_duplicates(
            df: pd.DataFrame, existing_rows: pd.DataFrame
        ) -> pd.DataFrame:
    """Deduplicate and merge new data with existing fact table data."""
    return pd.concat([df, existing_rows]).drop_duplicates(
        subset=["sales_order_id", "last_updated_date", "last_updated_time"]
    )


def write_to_database(
            con: pg8000.native.Connection, df: pd.DataFrame, table: str
        ) -> None:
    """Write the DataFrame to the database."""
    try:
        wr.postgresql.to_sql(
            df=df,
            con=con,
            schema="project_team_11",
            table=table,
            mode="append",
            index=True if table == "fact_sales_order" else False,
            insert_conflict_columns=[
                f"{table.split('_', 1)[1] if table
                    != 'fact_sales_order' else 'sales_record'}_id"
            ]
        )

    except DatabaseError as e:
        logger.error(f"DatabaseError: Error writing {table} to "
                     f"database. Error: {e}")


def lambda_handler(event: dict, _) -> None:
    """Lambda function entry point."""
    logger.info("Loading tables into database")
    con = None
    tables = None

    try:
        con = connect_to_database()
        if con is None:
            return

        tables = event["tables_written"]
        sorted_tables = sorted(tables, key=lambda x: x == "fact_sales_order")

        for table in sorted_tables:
            df = get_parquet_data(table)
            if df is None:
                continue

            if table == "fact_sales_order":
                existing_rows = get_existing_table_data(con, table)
                if existing_rows is not None:
                    df = merge_and_remove_duplicates(df, existing_rows)

            write_to_database(con, df, table)

    except KeyError as e:
        logger.error(f"KeyError: Malformed event input. Missing key: {e}")

    finally:
        logger.info(f"Updated database with: {tables}")
        if con:
            con.close()


if __name__ == '__main__':
    lambda_handler(None, None)
