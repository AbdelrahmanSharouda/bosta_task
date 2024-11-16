from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src import transformations

def failure_notification(context):
    task_id = context['task_instance'].task_id
    error = context.get('exception', 'No error information available.')
    
    slack_message = f"ETL Job Failure: Task `{task_id}`\nError: `{error}`"

    slack_alert = SlackAPIPostOperator(
        task_id="slack_alert",
        token="your_slack_api_token",
        text=slack_message,
        channel="#your-channel"
    )
    slack_alert.execute(context)

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
    on_failure_callback=failure_notification
    )

transform_load = PythonOperator(
    task_id='transform and loads',
    dag=dag,
    python_callable=transformations.transform_load,
    on_failure_callback=failure_notification
    )

extract >> transform_load