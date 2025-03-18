import boto3
import json
import pg8000
from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)


def get_db_creds(database):
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
        get_secret_value_response = client.get_secret_value(SecretId=database)
        logger.info("got secret")

    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


def db_connection(database="totesys-conn"):
    try:
        db_creds = get_db_creds(database)
        logger.info("Retrieved db credentials from AWS secrets manager")
        conn = Connection(
            user=db_creds["username"],
            database=db_creds["dbname"],
            password=db_creds["password"],
            host=db_creds["host"],
            port=db_creds["port"],
        )
        logger.info("Established connection to database")
        return conn
    
    except ClientError as e:
        logger.error('Error retrieving DB credentials from AWS secrets '
                     f'manager.\nError detail {e}')
        
    except DatabaseError as e:
        logger.error(f'Error connecting to Database.\n Error detail {e}')

    return None
