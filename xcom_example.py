from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

SCHEDULE_INTERVAL = '@once'

DAG_DEFAULT_ARGS = {
        'owner': 'Nagendra',
        'start_date': datetime(2019, 4, 24),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
}

DAG_VERSION = 'xcom_example_1.0'

dag = DAG(DAG_VERSION,
          default_args=DAG_DEFAULT_ARGS,
          schedule_interval=SCHEDULE_INTERVAL,
          concurrency=1,
          max_active_runs=1,
          catchup=False)

def push(**kwargs):
    task_instance = kwargs['ti']
    value_1 = [1, 2, 3, 4, 5]
    task_instance.xcom_push(key='key1', value=value_1)

def puller(**kwargs):
    ti = kwargs['ti']
    v1 = ti.xcom_pull(key='key1', task_ids='push')
    print(v1)
    return v1

# def data_passed(**kwargs):
#     task_instance = kwargs['ti']
#     list_passed = [1,2,3]
#     task_instance.xcom_push(key='my_data', value=list_passed)
#
# def data_received(**kwargs):
#     task_instance = kwargs['ti']
#     my_list = task_instance.xcom_pull(key='my_data', task_ids="sending_task")
#     print(my_list)
#     return my_list

start_task = DummyOperator(task_id='start_dummy_task', retries=3, dag=dag)

sending_task = PythonOperator(task_id='sending_task',
                              python_callable=push,
                              retries=0,
                              provide_context=True,
                              dag=dag)

receiving_task = PythonOperator(task_id='receiving_task',
                              python_callable=puller,
                              retries=0,
                              provide_context=True,
                              dag=dag)
end_task = DummyOperator(task_id='end_dummy_task', retries=3, dag=dag)

start_task >> sending_task >> receiving_task >> end_task