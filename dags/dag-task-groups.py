from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup


default_args = {
    'owner': 'bsp',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dag-task-groups',
    default_args=default_args,
    start_date=datetime(2024, 3, 21),
    schedule_interval='@daily',
    catchup=True,
) as dag:

    a = DummyOperator(
        task_id='a',
    )

    with TaskGroup('B', tooltip='Task group for b1, b12 and b22.') as B:
        b1 = DummyOperator(
            task_id='b1',
        )
    
        b21 = DummyOperator(
            task_id='b21',
        )
    
        b22 = DummyOperator(
            task_id='b22',
        )

        b21 >> b22

    c = DummyOperator(
        task_id="c",
    )

    a >> B >> c