import pandas as pd
from iso4217 import Currency
import logging


def dim_currency(currency_df):
   """Takes a DataFrame in currency table format
   and converts it to star_schema/dim_currency format

   Paramaters:
       currency_df: a pandas DataFrame

   Returns:
       DataFrame: in dim_currency format, if successful
       OR
       Dict (dict): {"result": "Failure"}, if unsuccessful
   """
   if isinstance(currency_df, pd.DataFrame):
       try:
           new_df = currency_df[["currency_id", "currency_code"]].copy()
           currency_codes = new_df["currency_code"].to_list()
           currency_names = [Currency(code).currency_name for code in currency_codes]
           new_df["currency_name"] = currency_names
           return new_df
       except Exception as e:
           logging.error(e)
   else:
       logging.error("Given paramater should be a DataFrame.")
   return {"result": "Failure"}
