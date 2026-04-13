from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'start_date': datetime(2020, 9, 24),
    'retries': 1,
}

dag = DAG(
    'transform_gold_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['transformation', 'gold', 'star_schema']
)

run_gold = BashOperator(
    task_id='run_silver_to_gold',
    bash_command='docker exec -u root jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/transform_silver_to_gold.py',
    dag=dag
)