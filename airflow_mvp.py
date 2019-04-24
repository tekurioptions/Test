from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import boto3
import pandas as pd
import io
from io import StringIO

author_columns = ['author_id', 'first_name', 'middle_name', 'lastname', 'coauthor_lastnames',
                      'topics', 'cities', 'countries', 'orcid']
investigator_columns = ['investigator_id', 'first_name', 'middle_name', 'lastname', 'coinvestigator_lastnames',
                        'topics', 'cities', 'countries', 'orcid']
output_columns = ['author_id', 'investigator_id']

s3 = boto3.resource('s3')

def s3_client():
    return boto3.client('s3')

def upload_file_to_s3(filename, key, bucket_name):
    s3.Bucket(bucket_name).upload_file(filename, key)

def read_csv_from_s3(bucket_name, input_key, columns):
    obj = s3_client().get_object(Bucket= bucket_name , Key = input_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='Windows-1252')
    df.columns = columns
    return df

def save_df_as_csv_to_s3(df, bucket_name, output_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3.Object(bucket_name, output_key).put(Body=csv_buffer.getvalue())

def get_matched_by_orc_id(authors, investigators):
    result = pd.merge(authors, investigators, on='orcid')
    result = result[output_columns]
    return result

def read_files_from_s3(bucket_name, key1, key2):
    df1 = read_csv_from_s3(bucket_name, key1)
    df2 = read_csv_from_s3(bucket_name, key2)
    return get_matched_by_orc_id(df1[(df1['orcid'].notnull()) & (df1['orcid'] != '')],
                                 df2[(df2['orcid'].notnull()) & (df2['orcid'] != '')])

def read_transform_save(bucket_name, input_key1, input_key2, output_key):
    output = read_files_from_s3(bucket_name, input_key1, input_key2)
    save_df_as_csv_to_s3(output, bucket_name, output_key)

DAG_DEFAULT_ARGS = {
        'owner': 'Nagendra',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
}

dag = DAG('airflow_mvp', description='Airflow Demo',
          start_date=datetime(2019, 4, 4),
          schedule_interval='@once',
          default_args=DAG_DEFAULT_ARGS, catchup=False)

start_task = DummyOperator(task_id='start_dummy_task', retries=3, dag=dag)

s3_upload_file1 = PythonOperator(
    task_id='s3_upload_file1',
    python_callable=upload_file_to_s3,
    op_kwargs={
        'filename': '/home/kumar.tekurinagendra/airflow_files/input/authors1.csv',
        'key': 'ca4i-fr-data/airflow/mvp/input/authors1.csv',
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
    },
    dag=dag)

s3_upload_file2 = PythonOperator(
    task_id='s3_upload_file2',
    python_callable=upload_file_to_s3,
    op_kwargs={
        'filename': '/home/kumar.tekurinagendra/airflow_files/input/investigators1.csv',
        'key': 'ca4i-fr-data/airflow/mvp/input/investigators1.csv',
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
    },
    dag=dag)

s3_read_transform_save = PythonOperator(
    task_id='s3_read_file1',
    python_callable=read_transform_save,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'input_key1': 'ca4i-fr-data/airflow/mvp/input/authors1.csv',
        'input_key2': 'ca4i-fr-data/airflow/mvp/input/investigators1.csv',
        'output_key': 'ca4i-fr-data/airflow/mvp/output/matched.csv'
    },
    dag=dag)

end_task = DummyOperator(task_id='end_dummy_task', retries=3, dag=dag)

start_task >> s3_upload_file1
start_task >> s3_upload_file2
s3_upload_file1 >> s3_read_transform_save
s3_upload_file2 >> s3_read_transform_save
s3_read_transform_save >> end_task