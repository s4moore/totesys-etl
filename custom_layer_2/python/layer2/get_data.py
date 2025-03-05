import boto3
import logging
import botocore.exceptions
import pandas as pd
import io
import pickle


def get_data(bucket_name="terrific-totes-data-team-11"):
    """
    Get data from s3 bucket
        Given an s3 bucket this funtion reads all the items and returns a dictionary with the file names
    - access the bucket
    - get all data from bucket once
    - get latest file from bucket after that
    - tranform each file into dataframa
    """

    s3 = boto3.client("s3")
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)

        if "Contents" in response:
            logging.info("Extracting data from bucket")
            list_of_files = []
            for items in response["Contents"]:
                list_of_files.append(items["Key"])
            logging.info(f"Total files extracted: {len(list_of_files)}")
            return {"Files_extracted": [file for file in list_of_files]}

    except botocore.exceptions.ClientError as e:
        logging.error("An error has occured with the client: %s", e)
        return {"Error": str(e)}


def tranform_file_into_df(file_name,bucket_name):
    """TRanform pkl file into dataframa
    Each of the files passed will be transform into a dataframe rather than pikle
    POpulate a new dataframe based on the template provided

    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name,Key=file_name)
    file_content = response['Body'].read()
    
    file_stream = io.BytesIO(file_content)
    df= pd.read_pickle(file_stream)

    return df

