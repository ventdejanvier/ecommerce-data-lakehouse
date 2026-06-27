from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "live_gold_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["transformation", "gold", "live"],
) as dag:
    run_live_silver_to_gold = BashOperator(
        task_id="run_live_silver_to_gold",
        bash_command=(
            "docker exec "
            "-e LIVE_SILVER_INPUT='s3a://silver/tracking_events_v2/' "
            "-e LIVE_GOLD_OUTPUT='s3a://gold/fact_live_events_v2/' "
            "-e LIVE_GOLD_TABLE='gold_db.fact_live_events_v2' "
            "jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/transform_live_silver_to_gold.py"
        ),
        pool="spark_heavy",
        execution_timeout=timedelta(minutes=30),
    )
