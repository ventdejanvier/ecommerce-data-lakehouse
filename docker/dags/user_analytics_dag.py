from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SPARK_PACKAGES = (
    'io.delta:delta-core_2.12:2.1.0,'
    'org.apache.hadoop:hadoop-aws:3.3.2,'
    'org.postgresql:postgresql:42.7.3'
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
    tags=['analytics', 'ml', 'kltn'],
)

# Chạy script K-Means
run_kmeans_task = BashOperator(
    task_id='run_kmeans_clustering',
    bash_command=f'docker exec -u root jupyter-notebook spark-submit --packages "{SPARK_PACKAGES}" /home/jovyan/scripts/analyze_user_clusters.py',
    dag=dag,
)

generate_recommendations_task = BashOperator(
    task_id='generate_cluster_recommendations',
    bash_command=f'docker exec -u root jupyter-notebook spark-submit --packages "{SPARK_PACKAGES}" /home/jovyan/scripts/generate_cluster_recommendations.py',
    dag=dag,
)

export_als_task = BashOperator(
    task_id='export_als_recommendations',
    bash_command=f'docker exec -u root jupyter-notebook spark-submit --packages "{SPARK_PACKAGES}" /home/jovyan/scripts/export_als_recommendations.py',
    dag=dag,
)

export_content_based_task = BashOperator(
    task_id='export_content_based',
    bash_command=f'docker exec -u root jupyter-notebook spark-submit --packages "{SPARK_PACKAGES}" /home/jovyan/scripts/export_content_based.py',
    dag=dag,
)

export_item_based_task = BashOperator(
    task_id='export_item_based',
    bash_command=f'docker exec -u root jupyter-notebook spark-submit --packages "{SPARK_PACKAGES}" /home/jovyan/scripts/export_item_based.py',
    dag=dag,
)

run_kmeans_task >> [
    generate_recommendations_task,
    export_als_task,
    export_content_based_task,
    export_item_based_task,
]
