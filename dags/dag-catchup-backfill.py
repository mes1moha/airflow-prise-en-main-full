from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'bsp',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dag-catchup-backfill',
    default_args=default_args,
    start_date=datetime(2024, 3, 21),
    schedule_interval='@daily',
    catchup=True,
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo un simple affichage'
    )