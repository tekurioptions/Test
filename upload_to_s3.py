from datetime import date, timedelta, datetime
from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
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

dag = DAG('upload_to_s3', description='Simple S3 Test',
          schedule_interval='@once',
          default_args=DAG_DEFAULT_ARGS, catchup=False)

start_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

upload_to_S3_task = PythonOperator(
    task_id='upload_to_S3',
    python_callable=upload_file_to_S3,
    op_kwargs={
        'filename': '/root/airflow-files/input/data.csv',
        'key': 'my_S3_data.csv',
        'bucket_name': 'nagdeep',
    },
    dag=dag)

start_task >> upload_to_S3_task