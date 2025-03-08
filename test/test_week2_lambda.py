from src.week2_lambda import lambda_handler
import pytest
import logging
import boto3


@pytest.fixture
def mock_event():
    return {
        "response": 200,
        "pkl_files_written": {
            "sales_order": "2025-03-05/12:25:10.410000/sales_order.pkl"
        },
        "triggerLambda2": True,
    }


@pytest.fixture
def integration_event():
    return {
        "response": 200,
        # needs to have a path to a file currently in real s3 bucket
        "pkl_files_written": {
            "sales_order": "2025-03-07/10:58:10.330000/sales_order.pkl"
        },
        "triggerLambda2": True,
    }


# integration
class TestLambdaHandlerTransformIntegration:
    # def test_returns_dict_with_files_written(self, integration_event):
    #     output = lambda_handler(integration_event, {})
    #     assert output["response"] == 200
    #     assert 'fact_sales_order' in output["parquet_files_written"]

    def test_logs_error(self, caplog):
        with caplog.at_level(logging.INFO):
            lambda_handler({"triggerLambda2": True}, {})
            assert "Error running transform Lambda:" in caplog.text

    def test_saves_pq_files_to_s3(self, integration_event):
        s3 = boto3.client("s3")
        lambda_handler(integration_event, {})
        response = s3.list_objects_v2(
            Bucket="totes-11-processed-data").get("Contents")
        bucket_files = [file["Key"] for file in response]
        for file in bucket_files:
            assert ".parquet" in file


# mocking
class TestLambdaHandlerTransformMocking:
    def test_uploads_file_to_s3(
            self, mock_event, mock_aws_with_buckets_and_glue
            ):
        s3, glue = mock_aws_with_buckets_and_glue
        lambda_handler(mock_event, {})
        response = s3.list_objects_v2(Bucket="totes-11-processed-data")
        tables_written = [
            file["Key"].split("/")[0] for file in response["Contents"]
            ]
        assert "fact_sales_order" in tables_written

    def test_returns_dict_with_files_written(
        self, mock_event, mock_aws_with_buckets_and_glue
    ):
        output = lambda_handler(mock_event, {})
        assert output["response"] == 200
        assert (
            "/fact_sales_order/"
            in output["parquet_files_written"]["fact_sales_order"]
        )

    def test_logs_errors(self, mock_event, mocked_aws, caplog):
        with caplog.at_level(logging.ERROR):
            lambda_handler(mock_event, {})
        assert "Error running transform Lambda:" in caplog.text
