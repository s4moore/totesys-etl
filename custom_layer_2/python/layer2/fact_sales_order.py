import pandas as pd
import logging


def fact_sales_order(sales_df):
    """Convert Staff DataFrame into Dim_staff schema

    Args:
        sales_df (Pandas Dataframe): Sales Dataframe:
        columns == [
        'sales_order_id',
        'created_at',
        'last_updated',
        'design_id',
        'staff_id',
        'counterparty_id',
        'units_sold',
        'unit_price',
        'currency_id',
        'agreed_delivery_date',
        'agreed_payment_date',
        'agreed_delivery_location_id']

    Returns:
        fact_sales_order (Pandas Dataframe): fact_sales_order schema:
        columns == [
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_payment_time",
            "agreed_delivery_location_id"]
    """
    try:
        output_df = pd.DataFrame()
        output_df["sales_order_id"] = sales_df["sales_order_id"]
        output_df['created_date'] = pd.to_datetime(sales_df['created_at']).dt.date
        output_df['created_time'] = pd.to_datetime(sales_df['created_at']).dt.strftime('%H:%M:%S')
        output_df['last_updated_date'] = pd.to_datetime(sales_df['last_updated']).dt.date
        output_df['last_updated_time'] = pd.to_datetime(sales_df['last_updated']).dt.strftime('%H:%M:%S')
        output_df["sales_staff_id"] = sales_df["staff_id"]
        output_df["counterparty_id"] = sales_df["counterparty_id"]
        output_df["units_sold"] = sales_df["units_sold"]
        output_df["unit_price"] = sales_df["unit_price"]
        output_df["currency_id"] = sales_df["currency_id"]
        output_df["design_id"] = sales_df["design_id"]
        output_df["agreed_payment_date"] = sales_df["agreed_payment_date"]
        output_df["agreed_delivery_date"] =sales_df["agreed_delivery_date"]
        output_df["agreed_delivery_location_id"] = sales_df[
            "agreed_delivery_location_id"
        ]
        return output_df
    except Exception as e:
        logging.error(f"Error running transform Lambda: {e}")
        return f"{e} error during {sales_df} dataframe transformation"
