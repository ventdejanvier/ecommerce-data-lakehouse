from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    bash_command='docker exec -u root jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/analyze_user_clusters.py',
    dag=dag,
)

run_kmeans_task