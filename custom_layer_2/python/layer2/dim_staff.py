import pandas as pd
import logging


def create_dim_staff(staff_df, dept_df):
    """Takes a s3 client and two table name, retrieves the
    latest file of that table from the ingestion bucket,
    and returns the data as a DataFrame

    Parameters:
    Staff table (DataFrame)
    Department table (DataFrame)

    Returns:
        DataFrame: of the latest data of the given table
    """
    try:
        staff_department = staff_df.merge(
            dept_df, how="left", left_on="department_id", right_on="department_id"
        )
        star_schema_staff_df = staff_department[
            [
                "staff_id",
                "first_name",
                "last_name",
                "department_name",
                "location",
                "email_address",
            ]
        ].copy()
        return star_schema_staff_df

    except Exception as e:
        logging.error(e)
