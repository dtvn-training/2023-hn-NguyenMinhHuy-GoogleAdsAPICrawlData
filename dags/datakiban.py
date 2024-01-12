from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

crawl_bash_path = '/home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/dags/crawl_data.sh'
transform_bash_path = '/home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/dags/transform_data.sh'
load_to_database_bash_path = '/home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/dags/load_to_database.sh'

with DAG(
    dag_id='datakiban',
    # schedule_interval=timedelta(days=1),
    # start_date=datetime(2024, 11, 1),
    # catchup=False, 
    # tags=['test'],
) as dag:
    # write a bash script to run run_bash.sh to crawl data from Google Ads API
    t1 = BashOperator(
        task_id='crawl_data',
        bash_command='bash ' + crawl_bash_path + ' ', # add space at the end for JINJA to work
        dag=dag
    )

    # write a bash script to run ktr file to transform data
    t2 = BashOperator(
        task_id='transform_data',
        bash_command='bash ' + transform_bash_path + ' ', # add space at the end for JINJA to work
        dag=dag
    )

    t3 = BashOperator(
        task_id= 'load_to_database',
        bash_command='bash ' + load_to_database_bash_path + ' ',
        dag=dag
    )

    t1 >> t2 >> t3
