from src.transform_lambda import lambda_handler
import pytest
import logging
import boto3
from unittest.mock import patch, MagicMock


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
            Bucket="processed123321").get("Contents")
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
        response = s3.list_objects_v2(Bucket="processed123321")
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

    @patch("src.transform_lambda.tranform_file_into_df")
    @patch("src.transform_lambda.check_for_dim_date")
    @patch("src.transform_lambda.create_dim_staff")
    @patch("src.transform_lambda.dim_location")
    @patch("src.transform_lambda.dim_design")
    @patch("src.transform_lambda.dim_currency")
    @patch("src.transform_lambda.dim_counterparty")
    @patch("src.transform_lambda.load_df_to_s3")
    def test_loads_all_tables_to_s3(
        self,
        mock_load,
        mock_counterparty,
        mock_currency,
        mock_design,
        mock_location,
        mock_staff,
        mock_check_dim_date,
        mock_transform,
        mock_aws_with_buckets_and_glue,
    ):
        mock_check_dim_date.return_value = True
        mock_transform.return_value = MagicMock()
        mock_staff.return_value = MagicMock()
        mock_location.return_value = MagicMock()
        mock_design.return_value = MagicMock()
        mock_currency.return_value = MagicMock()
        mock_counterparty.return_value = MagicMock()
        mock_load.return_value = MagicMock()

        event = {
            "response": 200,
            "pkl_files_written": {
                "staff": "pkl_file_1.pkl",
                "counterparty": "pkl_file_2.pkl",
                "currency": "pkl_file_3.pkl",
                "address": "pkl_file_4.pkl",
                "design": "pkl_file_5.pkl",
                "department": "pkl_file_6.pkl",
            },
            "triggerLambda": True,
        }

        output = lambda_handler(event, {})

        assert output["tables_written"] == [
            "dim_staff",
            "dim_counterparty",
            "dim_currency",
            "dim_location",
            "dim_design",
        ]
