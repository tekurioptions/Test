from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

def print_context(x, ds, *args, **kwargs):
    print(args)
    print(kwargs)
    print(ds)
    print(x)
    return 'Whatever you return gets printed in the logs'


DAG_DEFAULT_ARGS = {
        'owner': 'Nagendra',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
}

dag = DAG('args_test', description='arguments test',
          start_date=datetime(2019, 4, 4),
          schedule_interval='@once',
          default_args=DAG_DEFAULT_ARGS, catchup=False)

start_task = DummyOperator(task_id='start_dummy_task', retries=3, dag=dag)

test_arguments_pass = PythonOperator(
    task_id='test_arguments_pass',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
    op_args=['1', '2'])

end_task = DummyOperator(task_id='end_dummy_task', retries=3, dag=dag)

start_task >> test_arguments_pass >> end_task