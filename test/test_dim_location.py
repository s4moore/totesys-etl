from layer2 import dim_location
import pytest
import pandas as pd
from testfixtures import LogCapture


@pytest.fixture(scope="function")
def address_df():
    address_columns = [
        "address_id",
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone",
        "created_at",
        "last_updated",
    ]
    data = [
        [
            1,
            "6826 Herzog Via",
            "",
            "Avon",
            "New Patienceburgh",
            "28441",
            "Turkey",
            "1803 637401",
            "2022-11-03 14:20:49.962",
            "2022-11-03 14:20:49.962",
        ],
        [
            2,
            "179 Alexie Cliffs",
            "",
            "",
            "Aliso Viejo",
            "99305-7380",
            "San Marino",
            "9621 880720",
            "2022-11-03 14:20:49.962",
            "2022-11-03 14:20:49.962",
        ],
        [
            3,
            "148 Sincere Fort",
            "",
            "",
            "Lake Charles",
            "89360",
            "Samoa",
            "0730 783349",
            "2022-11-03 14:20:49.962",
            "2022-11-03 14:20:49.962",
        ],
    ]
    df = pd.DataFrame(data, columns=address_columns)
    yield df


class TestDimLocation:
    def test_function_returns_a_df(self, address_df):
        output = dim_location(address_df)
        assert isinstance(output, pd.DataFrame)

    def test_function_makes_dim_location_table(self, address_df):
        output = dim_location(address_df)
        assert list(output.columns) == [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
        assert list(output.iloc[0]) == [
            1,
            "6826 Herzog Via",
            "",
            "Avon",
            "New Patienceburgh",
            "28441",
            "Turkey",
            "1803 637401",
        ]
        assert list(output.iloc[1]) == [
            2,
            "179 Alexie Cliffs",
            "",
            "",
            "Aliso Viejo",
            "99305-7380",
            "San Marino",
            "9621 880720",
        ]
        assert list(output.iloc[2]) == [
            3,
            "148 Sincere Fort",
            "",
            "",
            "Lake Charles",
            "89360",
            "Samoa",
            "0730 783349",
        ]

    def test_function_handles_no_df_error(self):
        with LogCapture() as log:
            output = dim_location("")
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "Given paramater should be a DataFrame." in str(log)

    def test_function_handles_df_with_invalid_columns_error(self, test_df):
        with LogCapture() as log:
            output = dim_location(test_df)
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
