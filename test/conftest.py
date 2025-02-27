import os
import pytest
import boto3
from moto import mock_aws
from src.layer import db_connection


@pytest.fixture(scope="function")
def aws_cred():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def mocked_aws(aws_cred):
    """
    Mock all AWS interactions
    Requires you to create your own boto3 clients
    """
    with mock_aws():
        yield


@pytest.fixture(scope="function")
def empty_nc_terraformers_ingestion_s3(mocked_aws):
    s3 = boto3.client("s3")
    test_bucket = "nc-terraformers-ingestion-123"
    s3.create_bucket(
        Bucket=test_bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3

# @pytest.fixture(scope="function")
# def s3_bkt():
#     s3 = boto3.client("s3")
#     test_bucket = "nc-terraformers-ingestion-123"
#     s3.create_bucket(
#         Bucket=test_bucket,
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#     )
#     yield s3


@pytest.fixture(scope="function")
def conn_fixture():
    return db_connection()
