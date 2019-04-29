from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from matched_raw import ingest_to_raw
from matched_clean import clean_authors, clean_investigators
from matched_master import create_master_matched
from datalake import Raw,Cleaned, Master

def master_transform_save(bucket_name, authors_clean_key, investigators_clean_key, output_key):
    create_master_matched(bucket_name, authors_clean_key, investigators_clean_key, output_key)

def clean_save_authors(bucket_name, authors_raw_key, authors_clean_key):
    clean_authors(bucket_name, authors_raw_key, authors_clean_key)

def clean_save_investigators(bucket_name, investigators_raw_key, investigators_clean_key):
    clean_investigators(bucket_name, investigators_raw_key, investigators_clean_key)

DAG_DEFAULT_ARGS = {
        'owner': 'UCB',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
}

dag = DAG('airflow_demo_mvp', description='Airflow Demo',
          start_date=datetime(2019, 4, 4),
          schedule_interval='@once',
          default_args=DAG_DEFAULT_ARGS, catchup=False)

start_task = DummyOperator(task_id='start_dummy_task', retries=3, dag=dag)

ingest_authors_to_raw = PythonOperator(
    task_id='ingest_authors_to_raw',
    python_callable=ingest_to_raw,
    op_kwargs={
        'filename': '/home/kumar.tekurinagendra/airflow_files/input/authors1.csv',
        'key': Raw.Authors.value,
        'bucket_name': 'ucb-qb-ca-eu-west-1-data'
    },
    dag=dag)

ingest_investigators_to_raw = PythonOperator(
    task_id='ingest_investigators_to_raw',
    python_callable=ingest_to_raw,
    op_kwargs={
        'filename': '/home/kumar.tekurinagendra/airflow_files/input/investigators1.csv',
        'key': 'ca4i-fr-data/airflow/mvp/raw/investigators.csv',
        'bucket_name': 'ucb-qb-ca-eu-west-1-data'
    },
    dag=dag)

clean_authors_save_s3 = PythonOperator(
    task_id='clean_authors_save_s3',
    python_callable=clean_save_authors,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'authors_raw_key': 'ca4i-fr-data/airflow/mvp/raw/authors.csv',
        'authors_clean_key': 'ca4i-fr-data/airflow/mvp/clean/authors.csv'
    },
    dag=dag)

clean_investigators_save_s3 = PythonOperator(
    task_id='clean_investigators_save_s3',
    python_callable=clean_save_investigators,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'investigators_raw_key': 'ca4i-fr-data/airflow/mvp/raw/investigators.csv',
        'investigators_clean_key': 'ca4i-fr-data/airflow/mvp/clean/investigators.csv'
    },
    dag=dag)

master_transform_save_s3 = PythonOperator(
    task_id='master_transform_save_s3',
    python_callable=master_transform_save,
    op_kwargs={
        'bucket_name': 'ucb-qb-ca-eu-west-1-data',
        'authors_clean_key': 'ca4i-fr-data/airflow/mvp/clean/authors.csv',
        'investigators_clean_key': 'ca4i-fr-data/airflow/mvp/clean/investigators.csv',
        'output_key': 'ca4i-fr-data/airflow/mvp/master/matched.csv'
    },
    dag=dag)

end_task = DummyOperator(task_id='end_dummy_task', retries=3, dag=dag)

# start_task.set_downstream()

start_task >> ingest_authors_to_raw
start_task >> ingest_investigators_to_raw
ingest_authors_to_raw >> clean_authors_save_s3
ingest_investigators_to_raw >> clean_investigators_save_s3
clean_authors_save_s3 >> master_transform_save_s3
clean_investigators_save_s3 >> master_transform_save_s3
master_transform_save_s3 >> end_task