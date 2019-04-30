from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from matched_raw import ingest_to_raw
from matched_clean import clean_authors, clean_investigators
from matched_master import create_master_matched
from datalake import Raw,Cleaned, Master, Source, Common

def master_transform_save(bucket_name, authors_clean_key, investigators_clean_key, output_key, **kwargs):
    output = create_master_matched(bucket_name, authors_clean_key, investigators_clean_key, output_key)
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='output', value=output)

def save_to_postgres(**kwargs):
    task_instance = kwargs['ti']
    output = task_instance.xcom_pull(key='output', task_ids='master_transform_save_s3')
    print(type(output))
    print(output.count())

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
        'filename': Source.Authors.value,
        'key': Raw.Authors.value,
        'bucket_name': Common.Bucket.value
    },
    dag=dag)

ingest_investigators_to_raw = PythonOperator(
    task_id='ingest_investigators_to_raw',
    python_callable=ingest_to_raw,
    op_kwargs={
        'filename': Source.Investigators.value,
        'key': Raw.Investigators.value,
        'bucket_name': Common.Bucket.value
    },
    dag=dag)

clean_authors_save_s3 = PythonOperator(
    task_id='clean_authors_save_s3',
    python_callable=clean_save_authors,
    op_kwargs={
        'bucket_name': Common.Bucket.value,
        'authors_raw_key': Raw.Authors.value,
        'authors_clean_key': Cleaned.Authors.value
    },
    dag=dag)

clean_investigators_save_s3 = PythonOperator(
    task_id='clean_investigators_save_s3',
    python_callable=clean_save_investigators,
    op_kwargs={
        'bucket_name': Common.Bucket.value,
        'investigators_raw_key': Raw.Investigators.value,
        'investigators_clean_key': Cleaned.Investigators.value
    },
    dag=dag)

master_transform_save_s3 = PythonOperator(
    task_id='master_transform_save_s3',
    python_callable=master_transform_save,
    op_kwargs={
        'bucket_name': Common.Bucket.value,
        'authors_clean_key': Cleaned.Authors.value,
        'investigators_clean_key': Cleaned.Investigators.value,
        'output_key': Master.Matched.value
    },
    dag=dag, provide_context=True)

export_master_to_database = PythonOperator(
    task_id='export_master_to_database', dag=dag, python_callable=save_to_postgres, provide_context=True)

end_task = DummyOperator(task_id='end_dummy_task', retries=3, dag=dag)

# start_task.set_downstream()

start_task >> ingest_authors_to_raw
start_task >> ingest_investigators_to_raw
ingest_authors_to_raw >> clean_authors_save_s3
ingest_investigators_to_raw >> clean_investigators_save_s3
clean_authors_save_s3 >> master_transform_save_s3
clean_investigators_save_s3 >> master_transform_save_s3
master_transform_save_s3 >> export_master_to_database
export_master_to_database >> end_task