from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pytz import timezone

with DAG("price_score_stream_log_tbl_dag_1d",
         default_args={
            'depends_on_past': False,
            'email': ['ydy1412@naver.co.kr'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='load index ETL process DAG',
        schedule_interval='30 0 * * *',
        start_date=datetime(2023, 2, 13, 12, 0, tzinfo=timezone('Asia/Seoul')),
        catchup=False,
        tags=['example']
         ) as dag :
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag
    )
    t2 = BashOperator(
        task_id='load_price_score_data_process',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/process/load_price_data_process_1d.py',
        dag=dag
    )
    
    
    
    # t3 = BashOperator(
    #     task_id='container_traffic_volume_tbl_hscode_process_task',
    #     depends_on_past=False,
    #     bash_command='python /opt/airflow/dags/process/container_traffic_volume_tbl_hscode_process.py',
    #     dag=dag
    # )
    t1 >> t2 