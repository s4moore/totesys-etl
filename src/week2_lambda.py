def lambda_handler(event, context):
    return f'this is a test to show the list of files {event['pkl_files_written']}'