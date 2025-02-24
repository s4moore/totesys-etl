import boto3
import json
from botocore.exceptions import ClientError

def get_credentials(secret_name):
    '''A function to return the aws rds credentials as a dictionary with keys of
    cohort_id, user, password, host, database and port
    takes the name of the secret stored in secrets manager'''

    client = boto3.client('secretsmanager', region_name='eu-west-2')
    response = client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString']
    credentials = json.loads(secret)

    return credentials


name = 'MySuperSecret'
print(get_credentials(name))
