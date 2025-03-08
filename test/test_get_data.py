from layer2 import get_data, tranform_file_into_df
import logging
import boto3
from moto import mock_aws
import pytest
import botocore.exceptions
from unittest.mock import patch
import pandas as pd


@pytest.fixture
def mock_s3_bucket():
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "testy-buckety"
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        test_files = ["test1.pkl", "test2.pkl", "test3.pkl"]
        for file in test_files:
            s3.put_object(Bucket=bucket_name, Key=file)

        with open("./address.pkl", "rb") as f:
            body = f
            s3.put_object(Bucket=bucket_name, Key="address.pkl", Body=body)

        yield bucket_name


class TestGetData:

    def test_get_data_list_all_objects(self, mock_s3_bucket):

        response = get_data(mock_s3_bucket)

        for key, value in response.items():
            assert isinstance(response, dict)
            assert "Files_extracted" in key
            assert len(response["Files_extracted"]) > 0
            assert isinstance(value, list)
            assert "test1.pkl" in response["Files_extracted"]

    LOGGER = logging.getLogger(__name__)

    def test_logs_if_there_is_data_to_extract(self, caplog, mock_s3_bucket):
        with caplog.at_level(logging.INFO):
            get_data(mock_s3_bucket)
        assert "Extracting data from bucket" in caplog.text

    def test_log_with_total_number_of_files_extrcated(
            self, caplog, mock_s3_bucket
            ):
        with caplog.at_level(logging.INFO):
            get_data(mock_s3_bucket)
        assert "Total files extracted:" in caplog.text

    def test_logs_error(self, caplog):
        expected_error_messsage = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
            }
        }
        expected_error = botocore.exceptions.ClientError(
            expected_error_messsage, "ListObjectsV2"
        )

        with patch("boto3.client") as mock_boto_client:
            mock_s3 = mock_boto_client.return_value
            mock_s3.list_objects_v2.side_effect = expected_error

            with caplog.at_level(logging.ERROR):
                get_data("not-a-bucket")
            assert "An error has occured with the client:" in caplog.text
            assert get_data("not-a-bucket") == {"Error": str(expected_error)}


class TestTranformFileIntoDF:
    def test_tranfrom_file_into_df_returns_df(self, mock_s3_bucket):

        response = tranform_file_into_df("address.pkl", mock_s3_bucket)
        assert isinstance(response, pd.DataFrame)
