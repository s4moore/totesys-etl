from layer2 import fact_sales_order
from testfixtures import LogCapture
import pandas as pd


class TestFactSalesOrder:
    def test_returns_dataframe(self, test_sales_df):
        output = fact_sales_order(test_sales_df)
        assert isinstance(output, pd.DataFrame)

    def test_correct_columns(self, test_sales_df):
        output = fact_sales_order(test_sales_df)
        assert list(output.columns) == [
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
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]

    def test_function_handles_df_with_invalid_columns_error(self, test_df1):
        with LogCapture() as log:
            output = fact_sales_order(test_df1)
            assert (
                output
                == "'sales_order_id' error during   col1 col2\n0   a1   b1\n1   a2   b2\n2   a3   b3 dataframe transformation"
            )
            assert "ERROR" in str(log)
