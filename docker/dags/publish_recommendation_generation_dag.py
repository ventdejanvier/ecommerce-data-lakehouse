from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


GENERATION_ID_TEMPLATE = "{{ dag_run.conf.get('generation_id', '') }}"

dag = DAG(
    "publish_recommendation_generation_dag",
    description="Approval-controlled publication of one explicit READY generation",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["analytics", "ml", "publication", "manual"],
)

verify_candidate_task = BashOperator(
    task_id="verify_ready_candidate",
    bash_command=(
        "python /opt/airflow/scripts/verify_model_publication_v2.py "
        '--generation-id "$MODEL_GENERATION_ID"'
    ),
    env={"MODEL_GENERATION_ID": GENERATION_ID_TEMPLATE},
    append_env=True,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

publish_candidate_task = BashOperator(
    task_id="publish_ready_candidate",
    bash_command=(
        "python /opt/airflow/scripts/publish_recommendation_generation.py "
        '"$MODEL_GENERATION_ID"'
    ),
    env={"MODEL_GENERATION_ID": GENERATION_ID_TEMPLATE},
    append_env=True,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

verify_candidate_task >> publish_candidate_task
