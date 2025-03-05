from src.week2_lambda import lambda_handler
import pytest
import logging
import boto3

@pytest.fixture
def event():
    return {
  "response": 200,
  "pkl_files_written": {
    "sales_order": "2025-03-05/12:25:10.410000/sales_order.pkl"
  },
  "triggerLambda2": True
}

# integration testing
class TestLambdaHandlerTransform:
    def test_returns_dict_with_files_written(self, event):
        output = lambda_handler(event, {})
        parquet_files_written = {} # <<<INSERT FILE NAMES>>> #
        expected = {"response": 200, "parquet_files_written": parquet_files_written}
        assert output == expected
    
    def test_logs_error(self, caplog):
        with caplog.at_level(logging.INFO):
            output = lambda_handler({"triggerLambda2": True}, {})
            assert "Error running transform Lambda:" in caplog.text
    
    def test_saves_pq_files_to_s3(self, event):
        s3 = boto3.client("s3")
        lambda_handler(event, {})
        response = s3.list_objects_v2(Bucket="totes-11-processed-data").get("Contents") # NONE
        bucket_files = [file["Key"] for file in response]
        for file in bucket_files:
            assert "last_updated" in file
            assert ".parquet" in file