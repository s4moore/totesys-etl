import pandas as pd
import logging


def dim_counterparty(counterparty_df, address_df):
    """Takes two DataFrames in counterparty and address
    table format and converts it to star_schema/dim_counterparty format

    Paramaters:
        counterparty_df: a pandas DataFrame
        address_df: a pandas DataFrame

    Returns:
        DataFrame: in dim_counterparty format, if successful
        OR
        Dict (dict): {"result": "Failure"}, if unsuccessful
    """
    if isinstance(counterparty_df, pd.DataFrame) and isinstance(
        address_df, pd.DataFrame
    ):
        try:
            # Join address table ON address_ID
            dim_counterparty_df = counterparty_df.merge(
                address_df,
                how="left",
                left_on="legal_address_id",
                right_on="address_id",
            )
            # Rename to dim column names
            rename_dict = {
                "address_line_1": "counterparty_legal_address_line_1",
                "address_line_2": "counterparty_legal_address_line_2",
                "district": "counterparty_legal_district",
                "city": "counterparty_legal_city",
                "postal_code": "counterparty_legal_postal_code",
                "country": "counterparty_legal_country",
                "phone": "counterparty_legal_phone_number",
            }
            dim_counterparty_df.rename(columns=rename_dict, inplace=True)
            # Drop columns not needed
            drop_columns = [
                "legal_address_id",
                "commercial_contact",
                "delivery_contact",
                "created_at_x",
                "last_updated_x",
                "address_id",
                "created_at_y",
                "last_updated_y",
            ]
            dim_counterparty_df.drop(columns=drop_columns, inplace=True)
            return dim_counterparty_df
        except Exception as e:
            logging.error(e)
    else:
        logging.error("Given parameter should be a DataFrame.")
    return {"result": "Failure"}
