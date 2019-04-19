from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
import boto3

s3 = boto3.resource('s3')

def upload_file_to_S3(filename, key, bucket_name):
    s3.Bucket(bucket_name).upload_file(filename, key)

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
    python_callable=upload_file_to_S3,
    op_kwargs={
        'filename': '/home/kumar.tekurinagendra/airflow_files/input/authors.csv',
        'key': 'ca4i-fr-data/airflow/mvp/input/authors.csv',
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
    },
    dag=dag)

s3_upload_file2 = PythonOperator(
    task_id='s3_upload_file2',
    python_callable=upload_file_to_S3,
    op_kwargs={
        'filename': '/home/kumar.tekurinagendra/airflow_files/input/investigators.csv',
        'key': 'ca4i-fr-data/airflow/mvp/input/investigators.csv',
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
    },
    dag=dag)

end_task = DummyOperator(task_id='end_dummy_task', retries=3, dag=dag)

start_task >> s3_upload_file1
start_task >> s3_upload_file2
s3_upload_file1 >> end_task
s3_upload_file2 >> end_task