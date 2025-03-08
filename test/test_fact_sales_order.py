from layer2 import fact_sales_order
from testfixtures import LogCapture
import pandas as pd


class TestFactSalesOrder:
    def test_returns_dataframe(self, test_df):
        output = fact_sales_order(test_df)
        assert isinstance(output, pd.DataFrame)

    def test_correct_columns(self, test_df):
        output = fact_sales_order(test_df)
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

    def test_function_handles_df_with_invalid_columns_error(self, test_df):
        with LogCapture() as log:
            test_df = test_df.drop("last_updated", axis=1)
            fact_sales_order(test_df)
            assert "ERROR" in str(log)
