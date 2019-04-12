# load the dependencies
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import date, timedelta, datetime

import fetching_tweet
import cleaning_tweet

# default_args are the default arguments applied to the Dag's tasks
DAG_DEFAULT_ARGS = {
	'owner': 'Nagendra',
	'depends_on_past': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=1)
}

with DAG('my_dag', start_date=datetime(2019, 04, 12), schedule_interval="@daily", default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:
	waiting_file_task = FileSensor(task_id="waiting_file_task", fs_conn_id="fs_default", filepath="/home/ubuntu/airflow-files/data.csv", poke_interval=5)

	fetching_tweet_task = PythonOperator(task_id="fetching_tweet_task", python_callable=fetching_tweet.main)

	cleaning_tweet_task = PythonOperator(task_id="cleaning_tweet_task", python_callable=cleaning_tweet.main)

	waiting_file_task >> fetching_tweet_task >> cleaning_tweet_task
