from src.write_to_s3 import write_file_to_s3, split_time_stamps
import os
from moto import mock_aws
import pytest
import boto3
from unittest.mock import Mock
import datetime

# set mock credentials for aws testing with boto3
@pytest.fixture()
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

class TestWriteToS3:
    
    
    
    #test that a correct message of success or error is returned
    def test_write_file_to_s3_returns_success(aws_credentials):
        with mock_aws():
            client = boto3.client('s3')
            client.create_bucket(Bucket='test_bucket', 
                                CreateBucketConfiguration={
                                'LocationConstraint': 'eu-west-2'})
            
            result = write_file_to_s3(client, 'test/test_file.txt', 'test_bucket','object_key')
        expected = 'Success: object_key added to bucket'
        
        assert result == expected

    # def test_write_file_to_s3_returns_error(aws_credentials):
    #     with mock_aws():
    #         client = boto3.client('s3')
    #         client.create_bucket(Bucket='test_bucket', 
    #                             CreateBucketConfiguration={
    #                             'LocationConstraint': 'eu-west-2'})
            
    #         result = write_file_to_s3(client, 'path_to_file', 'bucket_doesnt_exist','object_key')
    #     expected = 'Error: Unable to add object_key to bucket. Error'

    #     assert result == expected

    
    #def test_data_are_written_in_correct_bucket_and_correct_format(self):
    #    pass


    def test_ClientError_is_handled_with_appropriate_error_message(self):
        with mock_aws():
            client = boto3.client('s3')
            client.create_bucket(Bucket='test_bucket', 
                                CreateBucketConfiguration={
                                'LocationConstraint': 'eu-west-2'})
            
            result = write_file_to_s3(client, 'wrong_path', 'bucket_doesnt_exist','object_key')
        expected = 'Error: Unable to add object_key to bucket. Error'

        assert result == expected


    def test_split_stamps_returns_correct_path_format(self):
        assert split_time_stamps(datetime.datetime(2025, 2, 12, 18, 5, 9, 793000)) == "02122025/180509793000/"



