from boto3 import client
#from src.helpers_tasks import get_parameter
from pprint import pprint
import datetime

# split the received time stamps  from datetime.datetime(2025, 2, 12, 18, 5, 9, 793000) to (date, time)
def split_time_stamps(dt):
    date = dt.strftime("%m%d%Y")
    time = dt.strftime("%H%M%S%f")
    print(date)
    print(time)
    object_key =f"{date}/{time}/"
    print(object_key)
    return object_key
#split_time_stamps(datetime.datetime(2025, 2, 12, 18, 5, 9, 793000))

def write_file_to_s3(s3_client, path_to_file, bucket_name, object_key, **kwargs):
    """Writes a file to the given S3 bucket.

    Given a path to local file, writes the file to the given S3 bucket and key.

    Args:
      s3_client: a boto3 client to interact with the AWS S3 API.
      path_to_file: a local file path, either absolute or relative to the root
                    of this repository.
      bucket_name: name of the bucket to write to.
      object_key: intended key of the object within the bucket.
      (optional) kwargs

    Returns:
      A string indicating success or an informative error message.

    """
    try:
      # response = s3_client.put_object(Bucket=bucket_name, Body=path_to_file, Key=object_key)
      response = s3_client.upload_file(path_to_file, bucket_name, object_key)
      list = s3_client.list_objects_v2(Bucket=bucket_name)
    
      if list['Contents'][0]['Key'] == object_key:
        return f'Success: {object_key} added to bucket'
      else:
        return f'Error: Unable to add {object_key} to bucket'
    except Exception as ClientError:
       return f'Error: Unable to add {object_key} to bucket. Error'
 


""""
if __name__ == "__main__":
    ssm_client = client("ssm")
    # s3_bucket_name = get_parameter(ssm_client, PARAMETER_NAME)

    s3_client = client("s3")
    msg = write_file_to_s3(
        s3_client, "path_to_file", bucket_name, "object_key"
    )
    print(msg)
"""
