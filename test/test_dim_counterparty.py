from layer2 import dim_counterparty
import pandas as pd
import pytest
from testfixtures import LogCapture


@pytest.fixture(scope="function")
def counterparty_df():
    cp_columns = [
        "counterparty_id",
        "counterparty_legal_name",
        "legal_address_id",
        "commercial_contact",
        "delivery_contact",
        "created_at",
        "last_updated",
    ]
    data = [
        [
            1,
            "Fahey and Sons",
            3,
            "Micheal Toy",
            "Mrs. Lucy Runolfsdottir",
            "2022-11-03 14:20:51.563",
            "2022-11-03 14:20:51.563",
        ],
        [
            2,
            '"Leannon, Predovic and Morar"',
            1,
            "Melba Sanford",
            "Jean Hane III",
            "2022-11-03 14:20:51.563",
            "2022-11-03 14:20:51.563",
        ],
        [
            3,
            "Armstrong Inc",
            2,
            "Jane Wiza",
            "Myra Kovacek",
            "2022-11-03 14:20:51.563",
            "2022-11-03 14:20:51.563",
        ],
    ]
    df = pd.DataFrame(data, columns=cp_columns)
    yield df


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


class TestDimCounterparty:
    def test_function_returns_a_df(self, counterparty_df, address_df):
        output = dim_counterparty(counterparty_df, address_df)
        assert isinstance(output, pd.DataFrame)

    def test_function_makes_dim_counterparty_table(
            self, counterparty_df, address_df
            ):
        output = dim_counterparty(counterparty_df, address_df)
        assert list(output.columns) == [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]
        assert list(output.iloc[0]) == [
            1,
            "Fahey and Sons",
            "148 Sincere Fort",
            "",
            "",
            "Lake Charles",
            "89360",
            "Samoa",
            "0730 783349",
        ]
        assert list(output.iloc[1]) == [
            2,
            '"Leannon, Predovic and Morar"',
            "6826 Herzog Via",
            "",
            "Avon",
            "New Patienceburgh",
            "28441",
            "Turkey",
            "1803 637401",
        ]
        assert list(output.iloc[2]) == [
            3,
            "Armstrong Inc",
            "179 Alexie Cliffs",
            "",
            "",
            "Aliso Viejo",
            "99305-7380",
            "San Marino",
            "9621 880720",
        ]

    def test_function_handles_no_df_error(self, address_df, counterparty_df):
        with LogCapture() as log:
            output = dim_counterparty("", "")
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "Given parameter should be a DataFrame." in str(log)

        with LogCapture() as log:
            output = dim_counterparty(address_df, "")
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "Given parameter should be a DataFrame." in str(log)

        with LogCapture() as log:
            output = dim_counterparty("", counterparty_df)
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "Given parameter should be a DataFrame." in str(log)

    def test_function_handles_dfs_with_invalid_columns_error(
        self, address_df, counterparty_df
    ):
        with LogCapture() as log:
            output = dim_counterparty(address_df, counterparty_df)
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
