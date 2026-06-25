from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'transform_gold_dag',
    default_args=default_args,
    schedule_interval="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=['transformation', 'gold']
) as dag:

    run_silver_to_gold = BashOperator(
        task_id='run_silver_to_gold',
        bash_command='docker exec jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/transform_silver_to_gold.py',
        pool='spark_heavy',
    )
