from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # Operator để gọi DAG khác
from datetime import datetime, timedelta

default_args = {'owner': 'admin', 'start_date': datetime(2020, 9, 24)}

with DAG('ingest_bronze_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # 1. Chạy script nạp dữ liệu từ Raw lên Bronze, truyền tham số ngày chạy (execution_date) để làm partition
    task_ingest = BashOperator(
        task_id='run_ingest_raw_to_bronze',
        bash_command='docker exec -u root jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/ingest_raw_to_bronze.py {{ ds }}'
    )

    # tự động gọi DAG Silver
    task_trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_dag',
        trigger_dag_id='transform_silver_dag' 
    )

    task_ingest >> task_trigger_silver