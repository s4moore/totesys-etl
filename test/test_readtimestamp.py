from src.layer.lambda1_utils import read_timestamp_from_s3
import pytest 
import boto3
import os
from moto import mock_aws
import logging

s3client = boto3.client('s3')

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


def test_returns_time_stamp(aws_credentials):
    with mock_aws():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=bucket_name)
        s3.put_object(Bucket=bucket_name, Key ='2025-02-25/14:23:15.00/customers.csv')
        s3.put_object(Bucket=bucket_name, Key ='2025-02-25/14:20:15.00/customers.csv')
        s3.put_object(Bucket=bucket_name, Key ='2025-02-25/14:25:15.00/customers.csv')
        s3.put_object(Bucket=bucket_name, Key ='2025-02-25/14:29:15.00/designs.csv')
 
        x = read_timestamp_from_s3(s3client, table='customers')

    assert x == {'customers':'2025-02-25 14:25:15.00'}

def test_returns_no_time_stamp_found(aws_credentials):
    with mock_aws():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=bucket_name)
        s3.put_object(Bucket=bucket_name, Key ='2025-02-25/14:23:15.00/customers.csv')
        s3.put_object(Bucket=bucket_name, Key ='2025-02-25/14:20:15.00/customers.csv')
        s3.put_object(Bucket=bucket_name, Key ='2025-02-25/14:25:15.00/customers.csv')
        s3.put_object(Bucket=bucket_name, Key ='2025-02-25/14:29:15.00/designs.csv')
 
        x = read_timestamp_from_s3(s3client, table='anything')

    assert x == {"detail": "No timestamp exists"} 

def test_returns_no_data_found(aws_credentials):
    with mock_aws():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=bucket_name)
 
        x = read_timestamp_from_s3(s3client, table='customers')

    assert x == {"detail": "No timestamp exists"} 

logger = logging.getLogger(__name__)

def test_returns_logging(aws_credentials):
    with mock_aws():
        logger.info('testing now')
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=bucket_name)
 
        x = read_timestamp_from_s3(s3client, table='customers')

    assert x == {"detail": "No timestamp exists"}   