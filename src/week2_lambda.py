from layer2 import dim_counterparty
from layer2 import dim_currency
from layer2 import dim_design
from layer2 import dim_location
from layer2 import create_dim_staff
from layer2 import fact_sales_order
from layer2 import check_for_dim_date
from layer2 import dim_date
from layer2 import load_df_to_s3, tranform_file_into_df
import logging
import boto3

logger = logging.getLogger()
logger.setLevel("INFO")

db_name = "load_db"
data_bucket = "terrific-totes-data-team-11"
bucket_name = "totes-11-processed-data"


def lambda_handler(event, context):
    """
    Event input:
    {"response": 200,
                "pkl_files_written": {table_name : pkl_file_written,
                table_name : pkl_file_written},
                "triggerLambda": bool}

    Returns:
    {"response": 200,
    "parquet_files_written": {table_name: parquet_files_written,
                                table_name2: pq file 2}
                                }
    """
    try:
        logging.info('event data: ', event)
        pkl_files_written = event["pkl_files_written"]
        # create s3 client
        s3 = boto3.client("s3")
        # create parquet_files_written dict
        parquet_files_written = {}
        # for table in pkl_files_written:
        for table in pkl_files_written:
            match table:
                case "sales_order":
                    logging.info("sales_order data transformation beginning")
                    sales_df = tranform_file_into_df(
                        pkl_files_written[table], data_bucket
                        )
                    fact_sales = fact_sales_order(sales_df)
                    pq_dict = load_df_to_s3(
                        fact_sales, bucket_name, db_name, "fact_sales_order"
                        )
                    parquet_files_written["fact_sales_order"] = \
                        pq_dict["paths"][0]
                    logging.info(
                        f"{pkl_files_written[table]} "
                        f"transformed into {pq_dict}"
                    )
                case "staff":
                    logging.info("staff data transformation beginning")
                    staff_df = tranform_file_into_df(
                        pkl_files_written[table], data_bucket
                        )
                    dept_df = tranform_file_into_df(
                        pkl_files_written["department"], data_bucket
                        )
                    dim_staff = create_dim_staff(staff_df, dept_df)
                    pq_dict = load_df_to_s3(
                        dim_staff, bucket_name, db_name, "dim_staff"
                        )
                    parquet_files_written["dim_staff"] = pq_dict["paths"][0]
                    logging.info(
                        f"{pkl_files_written[table]} "
                        f"transformed into {pq_dict}"
                    )
                case "address":
                    logging.info("address data transformation beginning")
                    address_df = tranform_file_into_df(
                        pkl_files_written[table], data_bucket
                        )
                    dim_loc_df = dim_location(address_df)
                    pq_dict = load_df_to_s3(
                        dim_loc_df, bucket_name, db_name, "dim_location"
                        )
                    parquet_files_written["dim_location"] = \
                        pq_dict["paths"][0]
                    logging.info(
                        f"{pkl_files_written[table]} "
                        f"transformed into {pq_dict}"
                    )
                case "design":
                    logging.info("design data transformation beginning")
                    design_df = tranform_file_into_df(
                        pkl_files_written[table], data_bucket
                        )
                    dim_design_df = dim_design(design_df)
                    pq_dict = load_df_to_s3(
                        dim_design_df, bucket_name, db_name, "dim_design"
                        )
                    parquet_files_written["dim_design"] = pq_dict["paths"][0]
                    logging.info(
                        f"{pkl_files_written[table]} "
                        f"transformed into {pq_dict}"
                    )
                case "currency":
                    logging.info("currency data transformation beginning")
                    currency_df = tranform_file_into_df(
                        pkl_files_written[table], data_bucket
                        )
                    dim_currency_df = dim_currency(currency_df)
                    pq_dict = load_df_to_s3(
                        dim_currency_df, bucket_name, db_name, "dim_currency"
                        )
                    parquet_files_written["dim_currency"] = \
                        pq_dict["paths"][0]
                    logging.info(
                        f"{pkl_files_written[table]} "
                        f"transformed into {pq_dict}"
                    )
                case "counterparty":
                    logging.info("counterparty data transformation beginning")
                    counter_df = tranform_file_into_df(
                        pkl_files_written[table], data_bucket
                        )
                    address_df = tranform_file_into_df(
                        pkl_files_written["address"], data_bucket
                        )
                    dim_counter_df = dim_counterparty(counter_df, address_df)
                    pq_dict = load_df_to_s3(
                        dim_counter_df,
                        bucket_name,
                        db_name,
                        "dim_counterparty"
                        )
                    parquet_files_written["dim_counterparty"] = \
                        pq_dict["paths"][0]
                    logging.info(
                        f"{pkl_files_written[table]} "
                        f"transformed into {pq_dict}"
                    )
                case v:
                    logging.warning(f"Unexpected input in event: {v}")

        if not check_for_dim_date(s3):
            logging.info("creating dim_date")
            dim_date_df = dim_date()
            pq_dict = load_df_to_s3(
                dim_date_df, bucket_name, db_name, "dim_date"
                )
            parquet_files_written["dim_date"] = pq_dict["paths"][0]
            logging.info(f"{pq_dict} written to bucket")

        return {"response": 200,
                "parquet_files_written": parquet_files_written,
                'tables_written':
                    [k for k, _ in parquet_files_written.items()]}

    except Exception as e:
        logging.error(f"Error running transform Lambda: {e}")

        return {"error": e}
