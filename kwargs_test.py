from __future__ import print_function
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    'example_xcom',
    schedule_interval="@once",
    default_args=args)

value_1 = [1, 2, 3]


def push(**kwargs):
    # pushes an XCom without a specific target
    kwargs['ti'].xcom_push(key='key1', value=value_1)


def puller(**kwargs):
    ti = kwargs['ti']
    v1 = ti.xcom_pull(key='key1', task_ids='push')
    print(v1)
    return v1

push1 = PythonOperator(
    task_id='push', dag=dag, python_callable=push, provide_context=True)

pull = BashOperator(
    task_id='also_run_this',
    bash_command='echo {{ ti.xcom_pull(task_ids="push") }}',
    dag=dag)

pull2 = PythonOperator(
    task_id='pull2', dag=dag, python_callable=puller, provide_context=True)

# pull.set_upstream(push1)
push1 >> pull >> pull2