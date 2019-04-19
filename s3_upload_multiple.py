from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
import boto3
import pandas as pd
import io
from io import StringIO

s3 = boto3.resource('s3')

def s3_client():
    return boto3.client('s3')

def upload_file_to_s3(filename, key, bucket_name):
    s3.Bucket(bucket_name).upload_file(filename, key)

def read_csv_from_s3(bucket_name, input_key):
    obj = s3_client().get_object(Bucket= bucket_name , Key = input_key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='Windows-1252')
    # print("Total number of rows: " + str(df.shape[0]))

def save_df_csv_s3(df, bucket_name, output_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3.Object(bucket_name, output_key).put(Body=csv_buffer.getvalue())

def read_save_csv_s3(bucket_name, input_key, output_key):
    df = read_csv_from_s3(bucket_name, input_key)
    save_df_csv_s3(df, bucket_name, output_key)

DAG_DEFAULT_ARGS = {
        'owner': 'Nagendra',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
}

dag = DAG('s3_upload_multiple', description='Upload two files in parallel',
          start_date=datetime(2019, 4, 4),
          schedule_interval='@once',
          default_args=DAG_DEFAULT_ARGS, catchup=False)

start_task = DummyOperator(task_id='start_dummy_task', retries=3, dag=dag)

s3_upload_file1 = PythonOperator(
    task_id='s3_upload_file1',
    python_callable=upload_file_to_s3,
    op_kwargs={
        'filename': '/home/kumar.tekurinagendra/airflow_files/input/authors.csv',
        'key': 'ca4i-fr-data/airflow/mvp/input/authors.csv',
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
    },
    dag=dag)

s3_upload_file2 = PythonOperator(
    task_id='s3_upload_file2',
    python_callable=upload_file_to_s3,
    op_kwargs={
        'filename': '/home/kumar.tekurinagendra/airflow_files/input/investigators.csv',
        'key': 'ca4i-fr-data/airflow/mvp/input/investigators.csv',
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
    },
    dag=dag)

s3_read_file1 = PythonOperator(
    task_id='s3_read_file1',
    python_callable=read_save_csv_s3,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'input_key': 'ca4i-fr-data/airflow/mvp/input/authors.csv',
        'output_key': 'ca4i-fr-data/airflow/mvp/output/authors_op.csv'
    },
    dag=dag)

s3_read_file2 = PythonOperator(
    task_id='s3_read_file2',
    python_callable=read_save_csv_s3,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'input_key': 'ca4i-fr-data/airflow/mvp/input/investigators.csv',
        'output_key': 'ca4i-fr-data/airflow/mvp/output/investigators_op.csv'
    },
    dag=dag)

end_task = DummyOperator(task_id='end_dummy_task', retries=3, dag=dag)

# start_task >> s3_upload_file1
start_task.set_downstream(s3_upload_file1)
# start_task >> s3_upload_file2
start_task.set_downstream(s3_upload_file2)
# s3_upload_file1 >> s3_read_file1
s3_upload_file1.set_downstream(s3_read_file1)
# s3_upload_file2 >> s3_read_file2
s3_upload_file2.set_downstream(s3_read_file2)
# s3_read_file1 >> end_task
s3_read_file1.set_downstream(end_task)
# s3_read_file2 >> end_task
s3_read_file2.set_downstream(end_task)