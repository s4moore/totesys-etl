import awswrangler as wr
from layer import db_connection2
from pg8000.exceptions import DatabaseError
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def lambda_handler(event, context):
    """Load data from processed zone into database.

    Reads parquet files from processed S3 bucket and store them in
    the databse.

    Args:
        event['tables_written']:
                        The names of tables updated during transform

    Returns:
        None
    """
    logger.info('Loading tables to databse')
    con = None
    tables = None
    try:
        con = db_connection2()
        tables = event['tables_written']

        # Set date format
        cursor = con.cursor()
        cursor.execute("SET datestyle = 'ISO, YMD';")
        con.commit()

        # Make sure fact table is the last in queue to ensure foreign
        # keys are available
        if 'fact_sales_order' in tables:
            tables.remove('fact_sales_order')
            tables.append('fact_sales_order')

        for table in tables:

            # Contiue to write remaining tables in case of any failure
            try:
                df = wr.s3.read_parquet_table(database="load_db", table=table)

                # If table fact_sales_order, get table to check for duplicates
                if table == 'fact_sales_order':
                    existing_rows = wr.postgresql.read_sql_table(
                        table=table,
                        schema='project_team_11',
                        con=con
                    )
                    existing_rows = existing_rows.drop(
                        columns=['sales_record_id']
                        )

                    # Concatenate tables and drop duplicate rows
                    df = pd.concat([df, existing_rows]).drop_duplicates(
                        subset=[
                            'sales_order_id',
                            'last_updated_date',
                            'last_updated_time'
                            ]
                    )

                wr.postgresql.to_sql(
                    df=df,
                    con=con,
                    schema="project_team_11",
                    table=table,
                    mode="append",
                    index=True if table == 'fact_sales_order' else False,
                    insert_conflict_columns=[
                        f"{table.split('_', 1)[1] if table
                            != 'fact_sales_order' else 'sales_record'}_id"
                    ]
                )

            except Exception as e:
                logger.error('Load: Exception: '
                             f'Error writing table: {table} to database.\n'
                             f'Error detail: {e}')
                tables.remove(table)

    except DatabaseError as e:
        logger.error(f'Load: DataBaseError: {e}')
    except KeyError as e:
        logger.error(f'Load: KeyError: {e}')

    finally:
        logger.info(f'Updated databse with: {tables}')
        if con:
            con.close()


if __name__ == '__main__':
    lambda_handler(None, None)
