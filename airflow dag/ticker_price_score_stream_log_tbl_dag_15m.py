from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pytz import timezone

# with DAG("price_score_stream_log_tbl_dag_15m",
#          default_args={
#             'depends_on_past': False,
#             'email': ['ydy1412@naver.co.kr'],
#             'email_on_failure': True,
#             'email_on_retry': False,
#             'retries': 1,
#             'retry_delay': timedelta(minutes=5),
#         },
#         description='load index ETL process DAG',
#         schedule_interval='1,16,31,46 * * * *',
#         start_date=datetime(2023, 4, 18, 12, 0, tzinfo=timezone('Asia/Seoul')),
#         catchup=False,
#         tags=['example']
#          ) as dag :
#     t1 = BashOperator(
#         task_id='print_date',
#         bash_command='date',
#         dag=dag
#     )
#     t2 = BashOperator(
#         task_id='load_price_score_data_process',
#         bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/process/load_price_score_data_process_15m.py',
#         dag=dag
#     )

#     t1 >> t2 