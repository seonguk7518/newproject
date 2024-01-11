from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pytz import timezone

with DAG("price_score_stream_log_tbl_dag",
         default_args={
            'depends_on_past': False,
            'email': ['ydy1412@naver.co.kr'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='load index ETL process DAG',
        schedule_interval='5 * * * *',
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
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/process/load_price_score_data_process.py',
        dag=dag
    )
    
    t3 = BashOperator(
        task_id='load_index_data_process',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/process/load_index_data_process.py',
        dag=dag
    )
    
    t4 = BashOperator(
        task_id='telegram_alert_process_algorithm_1',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/telegram_jobs/algorithm_1_telegram_job.py',
        dag=dag
    )
    
    t5 = BashOperator(
        task_id='telegram_alert_process_algorithm_2',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/telegram_jobs/algorithm_2_telegram_job.py',
        dag=dag
    )
    
    t6 = BashOperator(
        task_id='telegram_alert_process_algorithm_5',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/telegram_jobs/algorithm_5_telegram_job.py',
        dag=dag
    )
    
    t7 = BashOperator(
        task_id='telegram_alert_process_algorithm_6',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/telegram_jobs/algorithm_6_telegram_job.py',
        dag=dag
    )
    
    # t8 = BashOperator(
    #     task_id='telegram_alert_process_algorithm_9',
    #     bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/telegram_jobs/algorithm_9_telegram_job.py',
    #     dag=dag
    # )
    
    t9 = BashOperator(
        task_id='telegram_alert_process_algorithm_10',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/telegram_jobs/algorithm_10_telegram_job.py',
        dag=dag
    )
    
    t10 = BashOperator(
        task_id='telegram_alert_process_algorithm_15',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/test/algorithm_15_telegram_job.py',
        dag=dag
    )
    
    t11 = BashOperator(
        task_id='telegram_alert_process_algorithm_16',
        bash_command='/home/ubuntu/etl/bin/python3 /var/lib/airflow/dags/test/algorithm_16_telegram_job.py',
        dag=dag
    )
    # t3 = BashOperator(
    #     task_id='container_traffic_volume_tbl_hscode_process_task',
    #     depends_on_past=False,
    #     bash_command='python /opt/airflow/dags/process/container_traffic_volume_tbl_hscode_process.py',
    #     dag=dag
    # )
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t9 >> t10 >> t11