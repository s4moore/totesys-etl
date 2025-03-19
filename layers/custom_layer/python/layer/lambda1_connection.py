import boto3
import json
import pg8000
from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
from botocore.exceptions import ClientError
import logging

import pg8000.native

logger = logging.getLogger(__name__)


def get_db_creds(database):
    """Retrieve database credentials from AWS Secrets Manager.

    Args:
        database (str): 
                        The name of the credentials record stored
                        in AWS Secrets Manager.

    Returns:
        dict: 
                        A dictionary containing the database credentials,
                        with the keys: 'username', 'password', 'host',
                        'dbname', and 'port'.

    Raises:
        ClientError:
                        If there is an issue retrieving the secret from
                        Secrets Manager.
    """

    region_name = "eu-west-2"
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


def db_connection(database: str = "totesys-conn") -> pg8000.native.Connection:
    """
    Establishe a connection to a PostgreSQL database using credentials
    retrieved from AWS Secrets Manager.

    This function fetches the database credentials (username, password, host,
    dbname, and port) from AWS Secrets Manager using the specified database
    name. It then attempts to establish a connection to the PostgreSQL
    database using the `pg8000` library.

    Args:
        database (str):
                        The name or identifier of the secret stored in AWS
                        Secrets Manager that contains the database
                        credentials. Defaults to "totesys-conn".

    Returns:
        pg8000.native.Connection:
                        A connection object for the PostgreSQL database.

    Raises:
        ClientError:
                        If there is an issue retrieving the secret
                        from AWS Secrets Manager.
        DatabaseError:
                        If there is an issue establishing the connection
                        to the PostgreSQL database.
    """
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
