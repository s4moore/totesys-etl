from src.convert_to_parquet_and_upload import (
    convert_to_parquet,
    upload_to_processing_bucket,
)
from src.dim_counterparty import dim_counterparty
from src.dim_currency import dim_currency
from src.dim_date_table import dim_date
from src.dim_design import dim_design
from src.dim_location import dim_location
from src.dim_staff import create_dim_staff
from src.fact_sales_order import fact_sales_order
from src.get_latest_file_as_df import get_latest_file_as_df
from src.utils import collate_pkl_into_df, check_for_dim_date
from layer2 import get_data, load_df_to_s3, tranform_file_into_df

from datetime import datetime
import logging
import boto3

logger = logging.getLogger()
logger.setLevel("INFO")

db_name = DB_NAME
bucket_name = "totes-11-processed-data"

def lambda_handler(event, context):
    """
    Event input:
    {"response": 200,
                "pkl_files_written": {table_name : pkl_file_written, table_name : pkl_file_written},
                "triggerLambda": bool}

    Returns:
    {"response": 200,
    "parquet_files_written": {table_name: parquet_files_written,
                                table_name2: pq file 2}
                                }
    """
    try:
        pkl_files_written = event["pkl_files_written"]
        # create s3 client
        s3 = boto3.client("s3")
        # create parquet_files_written dict
        parquet_files_written = {}
        # for table in parquet_files_written:
        for table in pkl_files_written:
            match table:
                case "sales_order":
                    logging.info("sales_order data transformation beginning")
                    sales_df = tranform_file_into_df(pkl_files_written[table], bucket_name)
                    fact_sales = fact_sales_order(sales_df)
                    pq_dict = load_df_to_s3(fact_sales, bucket_name, db_name, "fact_sales_order")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "staff":
                    logging.info("staff data transformation beginning")
                    staff_df = tranform_file_into_df(pkl_files_written[table], bucket_name)
                    dept_df = tranform_file_into_df(pkl_files_written["department"], bucket_name)
                    dim_staff = create_dim_staff(staff_df, dept_df)
                    pq_dict = load_df_to_s3(dim_staff, bucket_name, db_name, "dim_staff")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "address":
                    logging.info("address data transformation beginning")
                    address_df = tranform_file_into_df(pkl_files_written[table], bucket_name)
                    dim_loc_df = dim_location(address_df)
                    pq_dict = load_df_to_s3(dim_loc_df, bucket_name, db_name, "dim_location")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "design":
                    logging.info("design data transformation beginning")
                    design_df = tranform_file_into_df(pkl_files_written[table], bucket_name)
                    dim_design_df = dim_design(design_df)
                    pq_dict = load_df_to_s3(dim_design_df, bucket_name, db_name,"dim_design")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "currency":
                    logging.info("currency data transformation beginning")
                    currency_df = tranform_file_into_df(pkl_files_written[table], bucket_name)
                    dim_currency_df = dim_currency(currency_df)
                    pq_dict = load_df_to_s3(dim_currency_df,bucket_name, db_name,"dim_currency")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "counterparty":
                    logging.info("counterparty data transformation beginning")
                    counter_df = tranform_file_into_df(pkl_files_written[table], bucket_name)
                    address_df = tranform_file_into_df(pkl_files_written["address"], bucket_name)
                    dim_counter_df = dim_counterparty(counter_df, address_df)
                    pq_dict = load_df_to_s3(dim_counter_df, bucket_name, db_name,"dim_counterparty")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case v:
                    logging.warning(f"Unexpected input in event: {v}")

        # if not check_for_dim_date(s3):
        #     logging.info("creating dim_date")
        #     dim_date_df = dim_date()
        #     dim_date_pq = convert_to_parquet(dim_date_df)
        #     pq_dict = upload_to_processing_bucket(s3, dim_date_pq, "dim_date")
        #     parquet_files_written.update(pq_dict)
        #     logging.info(f"{pq_dict} written to bucket")
        return {"response": 200, "parquet_files_written": parquet_files_written}

    except Exception as e:
        logging.error(e)
        return {"error": e}
