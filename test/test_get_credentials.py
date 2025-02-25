from boto3 import client
import pytest
from moto import mock_aws
import os
from src.helper_functions.get_credentials import get_credentials
import json
from botocore.exceptions import ClientError

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

def test_get_credentials_returns_secret_when_present(aws_credentials):
    with mock_aws():
        smclient = client('secretsmanager')
        test_name = 'MySuperSecret'
        test_secret = {"cohort_id": "de_2024_12_02",
        "user": "project_team_11", 
        "password": "fake_password", 
        "host": "fake_project_rds.amazonaws.com",
        "database": "totesys", 
        "port": 5432}
        json_secret = json.dumps(test_secret)
        smclient.create_secret(Name=test_name, SecretString=json_secret)

        output = get_credentials(test_name)
        expected = test_secret

        assert output == expected

def test_get_credentials_raises_error_when_secret_not_found(aws_credentials):
    with mock_aws():
        test_name = 'MySuperSecret'
        with pytest.raises(ClientError):
            get_credentials(test_name)
    

