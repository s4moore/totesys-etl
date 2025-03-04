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

from datetime import datetime
import logging
import boto3

logger = logging.getLogger()
logger.setLevel("INFO")


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
                    sales_df = get_latest_file_as_df(s3, pkl_files_written[table])
                    fact_sales = fact_sales_order(sales_df)
                    fact_sales_pq = convert_to_parquet(fact_sales)
                    pq_dict = upload_to_processing_bucket(
                        s3, fact_sales_pq, "fact_sales_order"
                    )
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "staff":
                    logging.info("staff data transformation beginning")
                    staff_df = get_latest_file_as_df(s3, pkl_files_written[table])
                    dept_df = collate_pkl_into_df(s3, "department")
                    dim_staff = create_dim_staff(staff_df, dept_df)
                    staff_pq = convert_to_parquet(dim_staff)
                    pq_dict = upload_to_processing_bucket(s3, staff_pq, "dim_staff")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "address":
                    logging.info("address data transformation beginning")
                    address_df = get_latest_file_as_df(s3, pkl_files_written[table])
                    dim_loc_df = dim_location(address_df)
                    loc_pq = convert_to_parquet(dim_loc_df)
                    pq_dict = upload_to_processing_bucket(s3, loc_pq, "dim_location")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "design":
                    logging.info("design data transformation beginning")
                    design_df = get_latest_file_as_df(s3, pkl_files_written[table])
                    dim_design_df = dim_design(design_df)
                    design_pq = convert_to_parquet(dim_design_df)
                    pq_dict = upload_to_processing_bucket(s3, design_pq, "dim_design")
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "currency":
                    logging.info("currency data transformation beginning")
                    currency_df = get_latest_file_as_df(s3, pkl_files_written[table])
                    dim_currency_df = dim_currency(currency_df)
                    currency_pq = convert_to_parquet(dim_currency_df)
                    pq_dict = upload_to_processing_bucket(
                        s3, currency_pq, "dim_currency"
                    )
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case "counterparty":
                    logging.info("counterparty data transformation beginning")
                    counter_df = get_latest_file_as_df(s3, pkl_files_written[table])
                    address_df = collate_pkl_into_df(s3, "address")
                    dim_counter_df = dim_counterparty(counter_df, address_df)
                    dim_counter_pq = convert_to_parquet(dim_counter_df)
                    pq_dict = upload_to_processing_bucket(
                        s3, dim_counter_pq, "dim_counterparty"
                    )
                    parquet_files_written.update(pq_dict)
                    logging.info(
                        f"{pkl_files_written[table]} transformed into {pq_dict}"
                    )
                case v:
                    logging.warning(f"Unexpected input in event: {v}")

        if not check_for_dim_date(s3):
            logging.info("creating dim_date")
            dim_date_df = dim_date()
            dim_date_pq = convert_to_parquet(dim_date_df)
            pq_dict = upload_to_processing_bucket(s3, dim_date_pq, "dim_date")
            parquet_files_written.update(pq_dict)
            logging.info(f"{pq_dict} written to bucket")
        return {"response": 200, "parquet_files_written": parquet_files_written}

    except Exception as e:
        logging.error(e)
        return {"error": e}
