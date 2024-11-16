from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src import transformations

default_args = {
    'owner':'Abdelrahman',
    'start_date': datetime(2024,8,26),
}

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

extract = PythonOperator(
    task_id='extraction',
    dag=dag,
    python_callable=transformations.extract,
    )

flatten = PythonOperator(
    task_id='transformation',
    dag=dag,
    python_callable=transformations.flatten
)



extract >> flatten