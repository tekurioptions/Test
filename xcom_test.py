from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
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
    # print("Total number of rows: " + str(df.shape[0]))

def save_df_csv_s3(df, bucket_name, output_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3.Object(bucket_name, output_key).put(Body=csv_buffer.getvalue())

def read_save_csv_s3(bucket_name, input_key, output_key):
    df = read_csv_from_s3(bucket_name, input_key)
    save_df_csv_s3(df, bucket_name, output_key)

def get_matched_by_orc_id(authors, investigators):
    result = pd.merge(authors, investigators, on='orcid')
    result = result[output_columns]
    return result

def find_matched(df1, df2):
    return get_matched_by_orc_id(df1[(df1['orcid'].notnull()) & (df1['orcid'] != '')],
                                 df2[(df2['orcid'].notnull()) & (df2['orcid'] != '')])

def transform_save_csv_s3(bucket_name, output_key, **kwargs):
    ti = kwargs['ti']
    df1 = ti.xcom_pull(task_ids='s3_read_file1')
    df2 = ti.xcom_pull(task_ids='s3_read_file2')
    output = find_matched(df1, df2)
    save_df_csv_s3(output, bucket_name, output_key)

DAG_DEFAULT_ARGS = {
        'owner': 'Nagendra',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'provide_context': True
}

dag = DAG('cross_communication_test', description='test',
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
    python_callable=read_csv_from_s3,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'input_key': 'ca4i-fr-data/airflow/mvp/input/authors.csv',
        'columns': author_columns
    },
    dag=dag)

s3_read_file2 = PythonOperator(
    task_id='s3_read_file2',
    python_callable=read_csv_from_s3,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'input_key': 'ca4i-fr-data/airflow/mvp/input/investigators.csv',
        'columns': investigator_columns
    },
    dag=dag)

transform_save_s3 = PythonOperator(
    task_id='transform_save_s3',
    python_callable=transform_save_csv_s3,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'output_key': 'ca4i-fr-data/airflow/mvp/output/matched.csv'
    },
    dag=dag)

end_task = DummyOperator(task_id='end_dummy_task', retries=3, dag=dag)

start_task >> s3_upload_file1
start_task >> s3_upload_file2
s3_upload_file1 >> s3_read_file1
s3_upload_file2 >> s3_read_file2
s3_read_file1 >> transform_save_s3
s3_read_file2 >> transform_save_s3
transform_save_s3 >> end_task