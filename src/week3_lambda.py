import awswrangler as wr
from layer import db_connection2
from pg8000.exceptions import DatabaseError
import logging

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

def lambda_handler(event, context):
    """Load data from processed zone into database.

    Reads parquet files from processed S3 bucket and store them in 
    the databse.

    Args:
        event['tables_written']:
                        The names of tables updated during transform
    """
    con = None
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
            
            # Enable continuation in case of error writing any table
            try:
                df = wr.s3.read_parquet_table(database="load_db", table=table)
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
       
            except Exception:
                logger.error('Load: Exception: '
                             f'Error writing table: {table} to database.')
                
    except DatabaseError as e:
        logger.error(f'Load: DataBaseError: {e}')
    except KeyError as e:
        logger.error(f'Load: KeyError: {e}')
        
    finally:
        if con:
            con.close()

if __name__ == '__main__':
    lambda_handler(None, None)