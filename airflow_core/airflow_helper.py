import boto3
import pandas as pd
import io
from io import StringIO

s3 = boto3.resource('s3')

def s3_client():
    return boto3.client('s3')

def upload_file_to_s3(filename, key, bucket_name):
    s3.Bucket(bucket_name).upload_file(filename, key)

def read_csv_from_s3(bucket_name, input_key, columns):
    obj = s3_client().get_object(Bucket= bucket_name , Key = input_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='Windows-1252')
    df.columns = columns
    return df.fillna('')

def save_df_as_csv_to_s3(df, bucket_name, output_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3.Object(bucket_name, output_key).put(Body=csv_buffer.getvalue())