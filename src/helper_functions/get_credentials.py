import boto3
import json
from botocore.exceptions import ClientError


def get_credentials(secret_name):
    """A function to return the aws rds credentials,
    if secret successfully obtained then the secret is returned
    as a python dictionary with keys of
    cohort_id, user, password, host, database and port
    otherwise an error is raised
    A parameter is the name of the secret stored in secrets manager"""

    client = boto3.client("secretsmanager", region_name="eu-west-2")

    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = response["SecretString"]
        credentials = json.loads(secret)

        return credentials
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
