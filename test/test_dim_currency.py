from layer2 import dim_currency
import pytest
import pandas as pd
from testfixtures import LogCapture


@pytest.fixture(scope="function")
def currency_df():
    currency_columns = [
        "currency_id", "currency_code", "created_at", "last_updated"
        ]
    data = [
        [1, "GBP", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962"],
        [2, "USD", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962"],
        [3, "EUR", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962"],
    ]
    df = pd.DataFrame(data, columns=currency_columns)
    yield df


class TestDimCurrency:
    def test_function_returns_a_df(self, currency_df):
        output = dim_currency(currency_df)
        assert isinstance(output, pd.DataFrame)

    def test_function_makes_dim_currency_table(self, currency_df):
        output = dim_currency(currency_df)
        assert list(output.columns) == [
            "currency_id", "currency_code", "currency_name"
            ]
        assert list(output.iloc[0]) == [1, "GBP", "Pound Sterling"]
        assert list(output.iloc[1]) == [2, "USD", "US Dollar"]
        assert list(output.iloc[2]) == [3, "EUR", "Euro"]

    def test_function_handles_no_df_error(self):
        with LogCapture() as log:
            output = dim_currency("")
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "Given paramater should be a DataFrame." in str(log)

    def test_function_handles_df_with_invalid_columns_error(self, test_df):
        with LogCapture() as log:
            output = dim_currency(test_df)
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)

    def test_function_handles_df_with_invalid_currency_error(
            self, currency_df
            ):
        new_row = {
            "currency_id": "4",
            "currency_code": "",
            "created_at": "DATE",
            "last_updated": "DATE",
        }
        test_df = currency_df._append(new_row, ignore_index=True)
        with LogCapture() as log:
            output = dim_currency(test_df)
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "is not a valid Currency" in str(log)
