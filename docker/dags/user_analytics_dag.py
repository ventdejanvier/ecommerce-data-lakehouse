from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

SPARK_PACKAGES = (
    'io.delta:delta-core_2.12:2.1.0,'
    'org.apache.hadoop:hadoop-aws:3.3.2,'
    'org.postgresql:postgresql:42.7.3'
)
GENERATION_ID_XCOM = "{{ ti.xcom_pull(task_ids='create_generation') }}"
V2_DOCKER_ENV = (
    "-e MODEL_PUBLICATION_V2=true "
    f"-e MODEL_GENERATION_ID='{GENERATION_ID_XCOM}' "
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
        "--airflow-run-id '{{ run_id }}'"
    ),
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
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

validate_generation_task = BashOperator(
    task_id='validate_generation',
    bash_command=(
        "python /opt/airflow/scripts/validate_recommendation_generation.py "
        f"'{GENERATION_ID_XCOM}'"
    ),
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

publish_generation_task = BashOperator(
    task_id='publish_generation',
    bash_command=(
        "python /opt/airflow/scripts/publish_recommendation_generation.py "
        f"'{GENERATION_ID_XCOM}'"
    ),
    trigger_rule=TriggerRule.ALL_SUCCESS,
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
