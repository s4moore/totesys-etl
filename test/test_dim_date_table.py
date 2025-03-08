import pandas as pd
from layer2 import dim_date


class TestDateTable:
    def test_dim_date_table_type(self):
        test = dim_date()
        assert isinstance(test, pd.DataFrame)
        expected_columns = [
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ]
        assert all(col in test.columns for col in expected_columns)

    def test_dim_date_table_length(self):
        test = dim_date()
        assert len(test) == 9497

    def test_dim_date_range(self):
        start = "2022-01-01"
        end = "2025-12-31"
        df = dim_date(start, end)
        assert df.date_id.min() == pd.Timestamp(start)
        assert df.date_id.max() == pd.Timestamp(end)
