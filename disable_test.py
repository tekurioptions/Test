from __future__ import print_function
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    'disable_test',
    schedule_interval="@once",
    default_args=args)


def push(**kwargs):
    task_instance = kwargs['ti']
    value_1 = [1, 2, 3, 4]
    task_instance.xcom_push(key='key1', value=value_1)


def puller(**kwargs):
    ti = kwargs['ti']
    v1 = ti.xcom_pull(key='key1', task_ids='push')
    print(v1)
    return v1

start_task = DummyOperator(task_id='start_dummy_task', retries=3, dag=dag)

push1 = PythonOperator(
    task_id='push', dag=dag, python_callable=push, provide_context=True)

pull = BashOperator(
    task_id='also_run_this',
    bash_command='echo {{ ti.xcom_pull(task_ids="push") }}',
    dag=dag)

pull2 = PythonOperator(
    task_id='pull2', dag=dag, python_callable=puller, provide_context=True)

start_task >> push1 >> pull >> pull2