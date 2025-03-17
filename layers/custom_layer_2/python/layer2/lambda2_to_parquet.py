import awswrangler as wr
import logging

logger = logging.getLogger()
logger.setLevel("INFO")

def load_df_to_s3(df, bucket_name, db_name, table):
    '''
    This function loads parquet files to a specified s3 bucket and folder by table name,
    partitioning by 'last_updated' column. It also generates metadata in AWS Glue.

    Args:
    - df (pd.DateFrame): Pandas DataFrame to be converted to parquet and uploaded to s3
    - bucket_name (str): The name of the main s3 bucket for storing all tables in specific folders
    - db_name (str): The name of the Glue database where the metadata is stored
    - table (str): The Glue table name for metadata

    Returns:
    - None

    Logging:
    - This function logs the start of the process, and subsequent success or failure.
    '''

    logging.info(f"Transforming dataframe into parquet and loading to s3 bucket {bucket_name}/{table} in progress...")
    
    try:
        # partition = ["last_updated_date"] if table == "fact_sales_order" else None
        pq_dict = wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket_name}/{table}", # not sure if the folder is needed as maybe metadata is enough
            dataset=True,
            #dtype={"sales_order_id": "int", 
            #       "created_date": "date", 
            #       "sales_staff_id": "int", 
            #       "counterparty_id": "int", 
            #       "units_sold": "int", 
            #       "unit_price": "float", 
            #       "currency_id": "string", 
            #       "design_id": "int", 
            #       "agreed_delivery_location_id": "int"},
            #mode="overwrite_partitions", # overwrittes any duplicates by last_updated
            # partition_cols=["last_updated_date"], # NEEDS TO CHANGE
            database=db_name,
            table=table # metadata for glue
            # sanitize_columns=True
        )
    
        logging.info("Upload successful")
        return pq_dict
    except Exception as e:
        logging.error(f"Unexpected error whilst loading parquet files to s3: {e}")

    
    # add dtype with a dict of table colums and types
    # read parquet can read glue metadata - 
    # find only new dates by partition and append to tables