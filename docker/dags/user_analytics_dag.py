from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

SPARK_PACKAGES = (
    'io.delta:delta-core_2.12:2.1.0,'
    'org.apache.hadoop:hadoop-aws:3.3.2,'
    'org.postgresql:postgresql:42.7.3'
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
V2_DOCKER_ENV = (
    "-e MODEL_PUBLICATION_V2=true "
    '-e MODEL_GENERATION_ID="$MODEL_GENERATION_ID" '
)

default_args = {
    'owner': 'admin',
    'start_date': datetime(2020, 9, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'user_analytics_dag',  
    default_args=default_args,
    description='Luồng phân tích hành vi và phân cụm khách hàng',
    schedule_interval=None, # Chạy khi được gọi
    catchup=False,
    max_active_runs=1,
    tags=['analytics', 'ml', 'kltn'],
)

create_generation_task = BashOperator(
    task_id='create_generation',
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

# Chạy script K-Means
run_kmeans_task = BashOperator(
    task_id='run_kmeans_clustering',
    bash_command=f'docker exec -u root jupyter-notebook spark-submit --packages "{SPARK_PACKAGES}" /home/jovyan/scripts/analyze_user_clusters.py',
    pool='spark_heavy',
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

generate_recommendations_task = BashOperator(
    task_id='generate_cluster_recommendations',
    bash_command=(
        f'docker exec -u root {V2_DOCKER_ENV}jupyter-notebook '
        f'spark-submit --packages "{SPARK_PACKAGES}" '
        '/home/jovyan/scripts/generate_cluster_recommendations.py'
    ),
    pool='spark_heavy',
    env=GENERATION_TASK_ENV,
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

export_als_task = BashOperator(
    task_id='export_als_recommendations',
    bash_command=(
        f'docker exec -u root {V2_DOCKER_ENV}jupyter-notebook '
        f'spark-submit --packages "{SPARK_PACKAGES}" '
        '/home/jovyan/scripts/export_als_recommendations.py'
    ),
    pool='spark_heavy',
    env=GENERATION_TASK_ENV,
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

export_content_based_task = BashOperator(
    task_id='export_content_based',
    bash_command=(
        f'docker exec -u root {V2_DOCKER_ENV}jupyter-notebook '
        f'spark-submit --packages "{SPARK_PACKAGES}" '
        '/home/jovyan/scripts/export_content_based.py'
    ),
    pool='spark_heavy',
    env=GENERATION_TASK_ENV,
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

export_item_based_task = BashOperator(
    task_id='export_item_based',
    bash_command=(
        f'docker exec -u root {V2_DOCKER_ENV}jupyter-notebook '
        f'spark-submit --packages "{SPARK_PACKAGES}" '
        '/home/jovyan/scripts/export_item_based.py'
    ),
    pool='spark_heavy',
    env=GENERATION_TASK_ENV,
    append_env=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

validate_generation_task = BashOperator(
    task_id='validate_generation',
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

publish_generation_task = BashOperator(
    task_id='publish_generation',
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
    task_id='fail_generation',
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

(
    create_generation_task
    >> run_kmeans_task
    >> generate_recommendations_task
    >> export_als_task
    >> export_content_based_task
    >> export_item_based_task
    >> validate_generation_task
    >> publish_generation_task
)

# The model tasks and validation are a linear chain. ONE_FAILED runs only after
# validation is terminal; model failures propagate there as UPSTREAM_FAILED,
# while a successful validation skips this finalizer and remains publishable.
validate_generation_task >> failure_finalizer_task
