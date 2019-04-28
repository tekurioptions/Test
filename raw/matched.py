from boto3.boto3_helper import upload_file_to_s3

def ingest_to_raw(filename, key, bucket_name):
    upload_file_to_s3(filename, key, bucket_name)