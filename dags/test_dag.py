from airflow   import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

with DAG(
    dag_id='test_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    t1 = BashOperator(
        task_id='test_task',
        bash_command='echo "Hello World!"',
    )

    t2 = BashOperator(
        task_id='test_task2',
        bash_command='echo "Hello World2!"',
    )

    t3 = BashOperator(
        task_id='test_task3',
        bash_command='echo "Hello World3!"',
    )

    t1 >> t2 >> t3