import boto3
import json
import pg8000
from pg8000.native import Connection
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)


def get_db_creds(secret_name="totesys-conn"):
    """
    Returns dict of {username:pg_user,
    password:pg_password,
    host:pg_host,
    dbname:pg_database,
    port:pg_port}
    """

    region_name = "eu-west-2"
    logger.info("client connecting")
    client = boto3.client(service_name="secretsmanager", region_name=region_name)
    logger.info("client connected")
    try:
        logger.info("getting secret")
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        logger.info("got secret")
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


def db_connection():
    logger.info("getting creds")
    db_creds = get_db_creds()
    logger.info("got creds")
    conn = Connection(
        db_creds["username"],
        database=db_creds["dbname"],
        password=db_creds["password"],
        host=db_creds["host"],
        port=db_creds["port"],
    )
    logger.info("got connection")
    return conn

def db_connection2(secret_name="wrangled-database"):
    logger.info("getting creds")
    db_creds = get_db_creds(secret_name)
    logger.info("got creds")
    conn = pg8000.connect(
        user=db_creds["username"],
        database=db_creds["dbname"],
        password=db_creds["password"],
        host=db_creds["host"],
        port=db_creds["port"],
    )
    logger.info("got connection")
    return conn