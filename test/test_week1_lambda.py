import json
import boto3
import pytest
import pandas as pd
from testfixtures import LogCapture
from moto import mock_aws
from unittest.mock import patch, Mock
from datetime import datetime
from src.week1_lambda import lambda_handler
from src.lambda1_connection import db_connection, get_db_creds
from src.lambda1_utils import (
    get_all_rows,
    get_columns,
    write_to_s3,
    get_tables,
    read_timestamp_from_s3,
    get_new_rows,
    write_df_to_csv,
    table_to_dataframe,
    timestamp_from_df,
    split_time_stamps
)
from pg8000.native import Connection


@pytest.fixture(scope="function")
def test_df():
    test_rows = [
        [True, datetime(2022, 11, 3, 14, 20, 51, 563000)],
        [True, datetime(2023, 11, 3, 14, 20, 51, 563000)],
    ]
    test_columns = ["column1", "last_updated"]
    return pd.DataFrame(test_rows, columns=test_columns)


@pytest.fixture(scope="function")
def test_staff_df():
    rows = [
        [
            1,
            "Jeremie",
            "Franey",
            2,
            "jeremie.franey@terrifictotes.com",
            datetime(2022, 11, 3, 14, 20, 51, 563000),
            datetime(2022, 11, 3, 14, 20, 51, 563000),
        ],
        [
            2,
            "Deron",
            "Beier",
            6,
            "deron.beier@terrifictotes.com",
            datetime(2022, 11, 3, 14, 20, 51, 563000),
            datetime(2022, 11, 3, 14, 20, 51, 563000),
        ],
        [
            3,
            "Jeanette",
            "Erdman",
            6,
            "jeanette.erdman@terrifictotes.com",
            datetime(2022, 11, 3, 14, 20, 51, 563000),
            datetime(2022, 11, 3, 14, 20, 51, 563000),
        ],
    ]
    cols = [
        "staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address",
        "created_at",
        "last_updated",
    ]
    return pd.DataFrame(rows, columns=cols)


@pytest.fixture(scope="function")
def test_tables(conn_fixture):
    return get_tables(conn_fixture)


class TestGetDBCreds:
    def test_correct_keys_in_dict(self):
        creds = get_db_creds()
        keys = list(creds.keys())
        assert "username" in keys
        assert "password" in keys
        assert "host" in keys
        assert "port" in keys
        assert "dbname" in keys

    def test_values_are_strings(self):
        creds = get_db_creds()
        for cred in creds:
            assert isinstance(creds[cred], str)


class TestConnection:
    def test_connection_formed(self):
        conn = db_connection()
        assert isinstance(conn, Connection)


class TestGetTables:
    def test_get_tables_returns_list(self):
        conn = db_connection()
        tables = get_tables(conn)
        assert isinstance(tables, list)

    def test_get_tables_returns_tables(self):
        conn = db_connection()
        tables = get_tables(conn)
        
        assert tables == [
            'transaction',
            'department',
             'sales_order',
              'design', 
            'currency',
             'payment_type',
              'counterparty', 
              'payment', 
              'address', 
            'purchase_order',
              'staff'
        ]


class TestGetRows:
    def test_returns_list(self, test_tables):
        conn = db_connection()
        assert isinstance(get_all_rows(conn, "staff", test_tables), list)

    def test_contains_lists(self, test_tables):
        conn = db_connection()
        result = get_all_rows(conn, "staff", test_tables)
        for row in result:
            assert isinstance(row, list)

    def test_correct_no_of_columns(self, test_tables):
        conn = db_connection()
        result = get_all_rows(conn, "staff", test_tables)
        for row in result:
            assert len(row) == 7


class TestGetColumns:
    def test_returns_list(self, test_tables):
        conn = db_connection()
        assert isinstance(get_columns(conn, "staff", test_tables), list)

    def test_correct_no_of_columns(self, test_tables):
        conn = db_connection()
        result = get_columns(conn, "staff", test_tables)
        assert len(result) == 7


class TestLogger:
    @mock_aws
    @patch("src.week1_lambda.db_connection")
    @patch("src.week1_lambda.datetime")
    def test_lambda_executed_timestamp_logger(
        self,
        mock_datetime,
        mock_db_connection,
        conn_fixture,
        empty_nc_terraformers_ingestion_s3,
    ):
        mock_db_connection.return_value = conn_fixture
        mock_datetime.now.return_value = "timestamp"
        # Timestamp mocked, AWS Mocked, s3_ingestion_bucket supplied, DB_connection patched in to bypass AWS secrets access
        with LogCapture() as l:
            lambda_handler([], {})
            assert ("root INFO\n  Lambda executed at timestamp") in str(l)


class TestWriteToS3:
    def test_returns_dict(self, empty_nc_terraformers_ingestion_s3):
        s3 = empty_nc_terraformers_ingestion_s3
        data = json.dumps({"test": "data"})
        assert isinstance(
            write_to_s3(s3, "nc-terraformers-ingestion", "test-file", "csv", data), dict
        )

    def test_writes_file(self, empty_nc_terraformers_ingestion_s3):
        s3 = empty_nc_terraformers_ingestion_s3
        data = json.dumps({"test": "data"})
        output = write_to_s3(s3, "nc-terraformers-ingestion", "test-file", "csv", data)
        objects = s3.list_objects(Bucket="nc-terraformers-ingestion")
        assert objects["Contents"][0]["Key"] == "test-file.csv"
        assert output["result"] == "Success"

    @mock_aws
    def test_handles_no_such_bucket_error(self):
        s3 = boto3.client("s3")
        data = json.dumps({"test": "data"})

        with LogCapture() as l:
            output = write_to_s3(s3, "non-existant-bucket", "test-file", "csv", data)
            assert output["result"] == "Failure"
            assert """root ERROR
  An error occurred (NoSuchBucket) when calling the PutObject operation: The specified bucket does not exist""" in (
                str(l)
            )

    def test_handles_filename_error(self, empty_nc_terraformers_ingestion_s3):
        data = True
        s3 = empty_nc_terraformers_ingestion_s3
        with LogCapture() as l:
            output = write_to_s3(s3, "test-bucket", "test-file", "csv", data)
            assert output["result"] == "Failure"
            assert """root ERROR
  Parameter validation failed:
Invalid type for parameter Body, value: True, type: <class 'bool'>, valid types: <class 'bytes'>, <class 'bytearray'>, file-like object""" in str(
                l
            )


class TestTimeStamp:
    def test_split_stamps_returns_correct_path_format(self):
        assert split_time_stamps(datetime(2025, 2, 12, 18, 5, 9, 793000)) == "2025-02-12/18:05:09.793000/"


class TestGetNewRows:
    def test_returns_list_of_lists(self, test_tables):
        conn = db_connection()
        output = get_new_rows(conn, "staff", "2013-11-14 10:19:09.990000", test_tables)
        assert isinstance(output, list)
        for item in output:
            assert isinstance(item, list)

    def test_handles_incorrect_timestamp(self, test_tables):
        conn = db_connection()
        with LogCapture() as l:
            output = get_new_rows(conn, "staff", "incorrect timestamp", test_tables)
            assert (
                """{'S': 'ERROR', 'V': 'ERROR', 'C': '22007', 'M': 'invalid value "inco" for "YYYY"', 'D': 'Value must be an integer.', 'F': 'formatting.c', 'L': '2416', 'R': 'from_char_parse_int_len'}"""
                in str(l)
            )
        assert output == []

    def test_returns_data_after_timestamp(self, test_tables):
        timestamp = "2024-11-14 12:37:09.990000"
        conn = db_connection()
        output = get_new_rows(conn, "sales_order", timestamp, test_tables)
        columns = get_columns(conn, "sales_order", test_tables)
        df = pd.DataFrame(output, columns=columns)
        format_string = "%Y-%m-%d %H:%M:%S.%f"
        min_time = df["last_updated"].min().to_pydatetime()
        assert min_time >= datetime.strptime(timestamp, format_string)
        assert type(min_time) == type(datetime.strptime(timestamp, format_string))

    def test_handles_invalid_table_name(self, test_tables):
        conn = db_connection()
        table = "invalid"
        timestamp = "2024-11-14 12:37:09.990000"
        with LogCapture() as l:
            output = get_new_rows(conn, table, timestamp, test_tables)
            assert output == []
            assert "root ERROR\n  Table not found" in str(l)

    def test_handles_error(self, test_tables):
        conn = db_connection()
        table = "staff"
        timestamp = "hello"
        with LogCapture() as l:
            output = get_new_rows(conn, table, timestamp, test_tables)
            assert output == []
            assert "root ERROR" in str(l)


class TestWriteDfToCsv:
    def test_returns_a_dict_with_result_key(
        self, empty_nc_terraformers_ingestion_s3, test_df
    ):
        test_name = "staff"
        client = empty_nc_terraformers_ingestion_s3
        output = write_df_to_csv(client, test_df, test_name)
        assert isinstance(output, dict)
        assert isinstance(output["result"], str)

    def test_converts_data_to_csv_and_uploads_to_s3_bucket(
        self, empty_nc_terraformers_ingestion_s3, test_staff_df
    ):
        test_name = "staff"
        client = empty_nc_terraformers_ingestion_s3
        test_bucket = "nc-terraformers-ingestion"
        write_df_to_csv(client, test_staff_df, test_name)
        response = client.list_objects_v2(Bucket=test_bucket).get("Contents")
        bucket_files = [file["Key"] for file in response]
        if len(bucket_files) > 1:
            get_file = client.get_object(Bucket=test_bucket, Key=test_name)
            assert get_file["ContentType"] == "csv"

    def test_uploads_to_s3_bucket(
        self, test_staff_df, empty_nc_terraformers_ingestion_s3
    ):
        test_name = "staff"
        client = empty_nc_terraformers_ingestion_s3
        output = write_df_to_csv(client, test_staff_df, test_name)
        assert output == {
            "result": "Success",
            "detail": "Converted to csv, uploaded to ingestion bucket",
            "key": f"staff/staff_{timestamp_from_df(test_staff_df)}.csv",
        }
        response = client.list_objects_v2(Bucket="nc-terraformers-ingestion").get(
            "Contents"
        )
        bucket_files = [file["Key"] for file in response]
        for file in bucket_files:
            assert "staff/staff" in file
            assert ".csv" in file

    def test_handles_error(self, empty_nc_terraformers_ingestion_s3):
        test_df = ""
        test_name = ""
        s3 = empty_nc_terraformers_ingestion_s3
        with LogCapture() as l:
            output = write_df_to_csv(s3, test_df, test_name)
            assert output == {"result": "Failure"}
            assert "string indices must be integers, not 'str'" in str(l)

    def test_writes_last_updated_timestamp_from_df(
        self, test_df, empty_nc_terraformers_ingestion_s3
    ):
        s3 = empty_nc_terraformers_ingestion_s3
        last_updated_from_df = timestamp_from_df(test_df)
        write_df_to_csv(s3, test_df, "test")
        response = s3.list_objects_v2(Bucket="nc-terraformers-ingestion").get(
            "Contents"
        )
        bucket_files = [file["Key"] for file in response]
        assert f"test/test_{str(last_updated_from_df)}.csv" in bucket_files


class TestTableToDataframe:
    def test_makes_data_frame_from_rows_and_columns(self, test_tables):
        conn = db_connection()
        test_rows = get_all_rows(conn, "staff", test_tables)
        test_columns = get_columns(conn, "staff", test_tables)
        output = table_to_dataframe(test_rows, test_columns)
        assert isinstance(output, pd.DataFrame)

    def test_handles_error(self):
        test_rows = ""
        test_columns = ""
        with LogCapture() as l:
            output = table_to_dataframe(test_rows, test_columns)
            assert output == None
            assert "DataFrame constructor not properly called!" in str(l)


class TestTimestampFromDf:
    def test_calculates_max_last_updated_timestamp_in_dataframe(self, test_df):
        expected_as_datetime = datetime(2023, 11, 3, 14, 20, 51, 563000)
        output = timestamp_from_df(test_df)
        assert output.to_pydatetime() == expected_as_datetime

    def test_handles_column_not_present(self):
        rows = [[1, 2, 3, 4, 5], [2, 1, "hi", 6, False]]
        columns = ["a", "b", "c", "d", "e"]
        df = pd.DataFrame(rows, columns=columns)
        with LogCapture() as l:
            output = timestamp_from_df(df)
            assert output == None
            assert (
                "root ERROR\n  {'column not found': KeyError('last_updated')}" in str(l)
            )



class TestLambdaHandler:
    @mock_aws
    @patch("src.week1_lambda.db_connection")
    def test_returns_200_response_and_list_of_filenames(
        self, mock_db_connection, conn_fixture, empty_nc_terraformers_ingestion_s3
    ):
        # Pass in DB connection
        mock_db_connection.return_value = conn_fixture
        # Pass in mocked AWS S3 client
        s3 = empty_nc_terraformers_ingestion_s3
        # Get Tables list
        db_tables = get_tables(conn_fixture)
        # Assert Lambda returns 200 and filenames in dict
        output = lambda_handler({}, {})
        assert output["response"] == 200
        assert list(output["csv_files_written"].keys()) == [
            "sales_order",
            "transaction",
            "department",
            "staff",
            "purchase_order",
            "counterparty",
            "payment",
            "currency",
            "payment_type",
            "address",
            "design",
        ]
        assert len(output["timestamp_json_files_written"]) == 11
        # List contents of mocked bucket
        response = s3.list_objects(Bucket="nc-terraformers-ingestion")
        content_list = [item["Key"] for item in response["Contents"]]
        # Assert Timestamp JSON files written for each table
        for table in db_tables:
            assert f"{table}_timestamp.json" in content_list
        assert len(content_list) == len(db_tables) * 2
