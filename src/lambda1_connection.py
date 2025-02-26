import boto3
import json
from pg8000.native import Connection
from botocore.exceptions import ClientError


def get_db_creds():
    """
    Returns dict of {username:pg_user,
    password:pg_password,
    host:pg_host,
    dbname:pg_database,
    port:pg_port}
    """

    secret_name = "totesys-conn"
    region_name = "eu-west-2"
    client = boto3.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


def db_connection():
    db_creds = get_db_creds()
    conn = Connection(
        db_creds["username"],
        database=db_creds["dbname"],
        password=db_creds["password"],
        host=db_creds["host"],
        port=db_creds["port"],
    )
    return conn
