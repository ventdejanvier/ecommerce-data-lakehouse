import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


SPARK_PACKAGES = (
    "io.delta:delta-core_2.12:2.1.0,"
    "org.apache.hadoop:hadoop-aws:3.3.2,"
    "org.postgresql:postgresql:42.7.3"
)
GENERATION_ID_XCOM = "{{ ti.xcom_pull(task_ids='create_generation') or '' }}"
AIRFLOW_RUN_ID_TEMPLATE = "{{ run_id }}"
GENERATION_TASK_ENV = {
    "MODEL_GENERATION_ID": GENERATION_ID_XCOM,
}
FAILURE_TASK_ENV = {
    "MODEL_GENERATION_ID": GENERATION_ID_XCOM,
    "MODEL_AIRFLOW_RUN_ID": AIRFLOW_RUN_ID_TEMPLATE,
}
DOCKER_PUBLICATION_ENV = (
    "-e MODEL_PUBLICATION_V2=true "
    "-e MODEL_GENERATION_ID "
    "-e MODEL_PUBLICATION_JDBC_URL "
    "-e MODEL_PUBLICATION_DB_USER "
    "-e MODEL_PUBLICATION_DB_PASSWORD "
    "-e MODEL_PUBLICATION_JDBC_DRIVER "
    "-e MODEL_PUBLICATION_LOCK_TIMEOUT_MS "
    "-e MODEL_PUBLICATION_STATEMENT_TIMEOUT_MS "
    "-e MINIO_ACCESS_KEY "
    "-e MINIO_SECRET_KEY "
)


def _strict_flag(name: str) -> bool:
    value = os.getenv(name, "false").strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    raise ValueError(f"{name} must be an explicit true/false value")


def choose_v2_path() -> str:
    enabled = _strict_flag("MODEL_PUBLICATION_V2_ENABLED")
    _strict_flag("MODEL_PUBLICATION_AUTO_PUBLISH")
    return "deployment_preflight" if enabled else "v2_disabled"


def choose_publication_path() -> str:
    return (
        "publish_generation"
        if _strict_flag("MODEL_PUBLICATION_AUTO_PUBLISH")
        else "ready_only_complete"
    )


default_args = {
    "owner": "admin",
    "start_date": datetime(2020, 9, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "user_analytics_dag",
    default_args=default_args,
    description="Manual V2 recommendation generation and publication workflow",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["analytics", "ml", "kltn"],
)

select_v2_mode_task = BranchPythonOperator(
    task_id="select_v2_mode",
    python_callable=choose_v2_path,
    dag=dag,
)

v2_disabled_task = EmptyOperator(
    task_id="v2_disabled",
    dag=dag,
)

deployment_preflight_task = BashOperator(
    task_id="deployment_preflight",
    bash_command=(
        "python /opt/airflow/scripts/model_publication_migration.py verify"
    ),
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

create_generation_task = BashOperator(
    task_id="create_generation",
    bash_command=(
        "python /opt/airflow/scripts/create_recommendation_generation.py "
        '--airflow-run-id "$MODEL_AIRFLOW_RUN_ID"'
    ),
    env={"MODEL_AIRFLOW_RUN_ID": AIRFLOW_RUN_ID_TEMPLATE},
    append_env=True,
    do_xcom_push=True,
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

run_kmeans_task = BashOperator(
    task_id="run_kmeans_clustering",
    bash_command=(
        f'docker exec -u root jupyter-notebook spark-submit --packages "{SPARK_PACKAGES}" '
        "/home/jovyan/scripts/analyze_user_clusters.py"
    ),
    pool="spark_heavy",
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

generate_recommendations_task = BashOperator(
    task_id="generate_cluster_recommendations",
    bash_command=(
        f"docker exec -u root {DOCKER_PUBLICATION_ENV}jupyter-notebook "
        f'spark-submit --packages "{SPARK_PACKAGES}" '
        "/home/jovyan/scripts/generate_cluster_recommendations.py"
    ),
    pool="spark_heavy",
    env=GENERATION_TASK_ENV,
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

export_als_task = BashOperator(
    task_id="export_als_recommendations",
    bash_command=(
        f"docker exec -u root {DOCKER_PUBLICATION_ENV}jupyter-notebook "
        f'spark-submit --packages "{SPARK_PACKAGES}" '
        "/home/jovyan/scripts/export_als_recommendations.py"
    ),
    pool="spark_heavy",
    env=GENERATION_TASK_ENV,
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

export_content_based_task = BashOperator(
    task_id="export_content_based",
    bash_command=(
        f"docker exec -u root {DOCKER_PUBLICATION_ENV}jupyter-notebook "
        f'spark-submit --packages "{SPARK_PACKAGES}" '
        "/home/jovyan/scripts/export_content_based.py"
    ),
    pool="spark_heavy",
    env=GENERATION_TASK_ENV,
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

export_item_based_task = BashOperator(
    task_id="export_item_based",
    bash_command=(
        f"docker exec -u root {DOCKER_PUBLICATION_ENV}jupyter-notebook "
        f'spark-submit --packages "{SPARK_PACKAGES}" '
        "/home/jovyan/scripts/export_item_based.py"
    ),
    pool="spark_heavy",
    env=GENERATION_TASK_ENV,
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

validate_generation_task = BashOperator(
    task_id="validate_generation",
    bash_command=(
        "python /opt/airflow/scripts/validate_recommendation_generation.py "
        '"$MODEL_GENERATION_ID"'
    ),
    env=GENERATION_TASK_ENV,
    append_env=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

select_publication_mode_task = BranchPythonOperator(
    task_id="select_publication_mode",
    python_callable=choose_publication_path,
    dag=dag,
)

ready_only_complete_task = EmptyOperator(
    task_id="ready_only_complete",
    dag=dag,
)

publish_generation_task = BashOperator(
    task_id="publish_generation",
    bash_command=(
        "python /opt/airflow/scripts/publish_recommendation_generation.py "
        '"$MODEL_GENERATION_ID"'
    ),
    env=GENERATION_TASK_ENV,
    append_env=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

failure_finalizer_task = BashOperator(
    task_id="fail_generation",
    bash_command=(
        "python /opt/airflow/scripts/fail_recommendation_generation.py "
        '--generation-id "$MODEL_GENERATION_ID" '
        '--airflow-run-id "$MODEL_AIRFLOW_RUN_ID" '
        "--reason 'Airflow recommendation pipeline task failed'"
    ),
    env=FAILURE_TASK_ENV,
    append_env=True,
    trigger_rule=TriggerRule.ONE_FAILED,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

select_v2_mode_task >> [v2_disabled_task, deployment_preflight_task]
(
    deployment_preflight_task
    >> create_generation_task
    >> run_kmeans_task
    >> generate_recommendations_task
    >> export_als_task
    >> export_content_based_task
    >> export_item_based_task
    >> validate_generation_task
    >> select_publication_mode_task
    >> [ready_only_complete_task, publish_generation_task]
)

for failure_observed_task in (
    deployment_preflight_task,
    create_generation_task,
    run_kmeans_task,
    generate_recommendations_task,
    export_als_task,
    export_content_based_task,
    export_item_based_task,
    validate_generation_task,
):
    failure_observed_task >> failure_finalizer_task
