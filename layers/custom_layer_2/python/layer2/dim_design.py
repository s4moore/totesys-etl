import pandas as pd
import logging


def dim_design(old_df):
    """Takes a DataFrame in design table format
    and converts it to star_schema/dim_design format

    Paramaters:
        old_df: a pandas DataFrame

    Returns:
        DataFrame: in dim_design format, if successful
        OR
        Dict (dict): {"result": "Failure"}, if unsuccessful
    """
    if isinstance(old_df, pd.DataFrame):
        try:
            new_df = old_df[
                ["design_id", "design_name", "file_location", "file_name"]
            ].copy()
            return new_df
        except Exception as e:
            logging.error(e)
    else:
        logging.error("Given paramater should be a DataFrame.")
    return {"result": "Failure"}
