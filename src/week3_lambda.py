import awswrangler as wr
import pg8000
from pg8000.native import Connection
from dotenv import load_dotenv
import os

load_dotenv()

def lambda_handler(event, context):
    tables = ['fact_sales_order']
    for table in tables:
        df = wr.s3.read_parquet_table(database="load_db", table=table)
        
        con = pg8000.connect(
            database= os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            host=os.environ["PGHOST"],
            port=5432
            )

        print(con)
        
        res = wr.postgresql.to_sql(
            df=df,
            con=con,
            schema="public",
            table=table,
            mode="append",
            index=False
        )

        print(res)

if __name__ == '__main__':
    lambda_handler(None, None)