import json
import boto3
import pytest
import pandas as pd
from testfixtures import LogCapture
from moto import mock_aws
from unittest import mock
from datetime import datetime
from week1_lambda import lambda_handler
from layer import db_connection, get_db_creds
from layer import (
    get_all_rows,
    get_columns,
    write_to_s3,
    get_tables,
    get_new_rows,
    write_df_to_pickle,
    table_to_dataframe,
    timestamp_from_df,
    split_time_stamps,
)
from pg8000.native import Connection


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
            "transaction",
            "department",
            "sales_order",
            "design",
            "currency",
            "payment_type",
            "counterparty",
            "payment",
            "address",
            "purchase_order",
            "staff",
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


class TestWriteToS3:
    def test_returns_dict(self, empty_nc_terraformers_ingestion_s3):
        s3 = empty_nc_terraformers_ingestion_s3
        data = json.dumps({"test": "data"})
        assert isinstance(
            write_to_s3(
                s3,
                "nc-terraformers-ingestion-123",
                "test-file",
                "pkl",
                data,
            ),
            dict,
        )

    def test_writes_file(self, empty_nc_terraformers_ingestion_s3):
        s3 = empty_nc_terraformers_ingestion_s3
        data = json.dumps({"test": "data"})
        output = write_to_s3(
            s3,
            "nc-terraformers-ingestion-123",
            "test-file",
            "pkl",
            data,
        )
        objects = s3.list_objects(Bucket="nc-terraformers-ingestion-123")
        assert objects["Contents"][0]["Key"] == "test-file.pkl"
        assert output["result"] == "Success"

    @mock_aws
    def test_handles_no_such_bucket_error(self):
        s3 = boto3.client("s3")
        data = json.dumps({"test": "data"})

        with LogCapture() as log:
            output = write_to_s3(
                s3, "non-existant-bucket", "test-file", "pkl", data
                )
            assert output["result"] == "Failure"
            assert (
                "root ERROR\n  An error occurred (NoSuchBucket) when "
                + "calling the PutObject operation: The specified bucket"
                + " does not exist"
                in (str(log))
            )

    def test_handles_filename_error(self, empty_nc_terraformers_ingestion_s3):
        data = True
        s3 = empty_nc_terraformers_ingestion_s3
        with LogCapture() as log:
            output = write_to_s3(s3, "test-bucket", "test-file", "pkl", data)
            assert output["result"] == "Failure"
            assert (
                "root ERROR\n  Parameter validation failed:\nInvalid "
                + "type for parameter Body, value: True, type: <class "
                + "'bool'>, valid types: <class 'bytes'>, <class "
                + "'bytearray'>, file-like object"
                in str(log)
            )


class TestTimeStamp:
    def test_split_stamps_returns_correct_path_format(self):
        assert (
            split_time_stamps(datetime(2025, 2, 12, 18, 5, 9, 793000))
            == "2025-02-12/18:05:09.793000/"
        )


class TestGetNewRows:
    def test_returns_list_of_lists(self, test_tables):
        conn = db_connection()
        output = get_new_rows(
            conn, "staff", "2013-11-14 10:19:09.990000", test_tables
            )
        assert isinstance(output, list)
        for item in output:
            assert isinstance(item, list)

    def test_handles_incorrect_timestamp(self, test_tables):
        conn = db_connection()
        with LogCapture() as log:
            output = get_new_rows(
                conn, "staff", "incorrect timestamp", test_tables
                )
            assert (
                "{'S': 'ERROR', 'V': 'ERROR', 'C': '22007', 'M': "
                '\'invalid value "inco"'
            ) in str(log)
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
        assert isinstance(min_time, datetime)

    def test_handles_invalid_table_name(self, test_tables):
        conn = db_connection()
        table = "invalid"
        timestamp = "2024-11-14 12:37:09.990000"
        with LogCapture() as log:
            output = get_new_rows(conn, table, timestamp, test_tables)
            assert output == []
            assert "root ERROR\n  Table not found" in str(log)

    def test_handles_error(self, test_tables):
        conn = db_connection()
        table = "staff"
        timestamp = "hello"
        with LogCapture() as log:
            output = get_new_rows(conn, table, timestamp, test_tables)
            assert output == []
            assert "root ERROR" in str(log)


class TestWriteDfToPickle:
    def test_returns_a_dict_with_result_key(
        self, empty_nc_terraformers_ingestion_s3, test_df
    ):
        test_name = "staff"
        client = empty_nc_terraformers_ingestion_s3
        test_bucket = "nc-terraformers-ingestion-123"
        output = write_df_to_pickle(client, test_df, test_name, test_bucket)
        assert isinstance(output, dict)
        assert isinstance(output["result"], str)

    def test_converts_data_to_pkl(
        self, empty_nc_terraformers_ingestion_s3, test_staff_df
    ):
        test_name = "staff"
        client = empty_nc_terraformers_ingestion_s3
        test_bucket = "nc-terraformers-ingestion-123"
        write_df_to_pickle(client, test_staff_df, test_name, test_bucket)
        response = client.list_objects_v2(Bucket=test_bucket).get("Contents")
        bucket_files = [file["Key"] for file in response]
        if len(bucket_files) >= 1:
            get_file = client.get_object(
                Bucket=test_bucket, Key=bucket_files[0]
                )
            assert get_file["ContentType"] == "binary/octet-stream"

    def test_uploads_to_s3_bucket(
        self, test_staff_df, empty_nc_terraformers_ingestion_s3
    ):
        test_name = "staff"
        client = empty_nc_terraformers_ingestion_s3
        test_bucket = "nc-terraformers-ingestion-123"
        output = write_df_to_pickle(
            client, test_staff_df, test_name, test_bucket
            )
        assert output == {
            "result": "Success",
            "detail": "Converted to pkl, uploaded to ingestion bucket",
            "key": f"{timestamp_from_df(test_staff_df)}staff.pkl",
        }
        response = client.list_objects_v2(Bucket=test_bucket).get("Contents")
        bucket_files = [file["Key"] for file in response]
        for file in bucket_files:
            assert "2022-11-03/14:20:51.563000" in file
            assert ".pkl" in file

    def test_handles_error(self, empty_nc_terraformers_ingestion_s3):
        test_df = ""
        test_name = ""
        s3 = empty_nc_terraformers_ingestion_s3
        with LogCapture() as log:
            test_bucket = "nc-terraformers-ingestion-123"
            output = write_df_to_pickle(s3, test_df, test_name, test_bucket)
            assert output == {"result": "Failure"}
            assert "string indices must be integers, not 'str'" in str(log)

    def test_writes_last_updated_timestamp_from_df(
        self, test_df, empty_nc_terraformers_ingestion_s3
    ):
        s3 = empty_nc_terraformers_ingestion_s3
        last_updated_from_df = timestamp_from_df(test_df)
        test_bucket = "nc-terraformers-ingestion-123"
        write_df_to_pickle(s3, test_df, "test", test_bucket)
        response = s3.list_objects_v2(Bucket=test_bucket).get("Contents")
        bucket_files = [file["Key"] for file in response]
        assert f"{str(last_updated_from_df)}test.pkl" in bucket_files


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
        with LogCapture() as log:
            output = table_to_dataframe(test_rows, test_columns)
            assert output is None
            assert "DataFrame constructor not properly called!" in str(log)


class TestTimestampFromDf:

    def test_handles_column_not_present(self):
        rows = [[1, 2, 3, 4, 5], [2, 1, "hi", 6, False]]
        columns = ["a", "b", "c", "d", "e"]
        df = pd.DataFrame(rows, columns=columns)
        with LogCapture() as log:
            output = timestamp_from_df(df)
            assert output is None
            assert (
                "root ERROR\n  {'column not found': KeyError('last_updated')}"
                in str(log)
            )


class TestLambdaHandler:

    def test_return_response_200(self):
        output = lambda_handler({}, [])
        assert output["response"] == 200

    def test_lambda_conatints_dict_with_list_of_cvs_files(self):
        output = lambda_handler({}, [])
        assert isinstance(output["pkl_files_written"], dict)
        assert "pkl_files_written" in output.keys()

    @mock.patch("week1_lambda.get_tables")
    def test_lambda_raise_exception_error_message(self, mock_conn):
        mock_conn.side_effect = Exception("error")
        result = lambda_handler({}, [])["response"]
        expected = 500
        assert result == expected
