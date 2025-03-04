import pandas as pd
import logging


def dim_location(address_df):
    """Takes a DataFrame in address table format
    and converts it to star_schema/dim_location format

    Paramaters:
        address_df: a pandas DataFrame

    Returns:
        DataFrame: in dim_location format, if successful
        OR
        Dict (dict): {"result": "Failure"}, if unsuccessful
    """
    if isinstance(address_df, pd.DataFrame):
        try:
            new_df = address_df[
                [
                    "address_id",
                    "address_line_1",
                    "address_line_2",
                    "district",
                    "city",
                    "postal_code",
                    "country",
                    "phone",
                ]
            ].copy()
            new_df.drop_duplicates()
            new_df = new_df.rename(columns={"address_id": "location_id"})
            return new_df
        except Exception as e:
            logging.error(e)
    else:
        logging.error("Given paramater should be a DataFrame.")
    return {"result": "Failure"}
