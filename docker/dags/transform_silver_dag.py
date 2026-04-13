from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from datetime import datetime, timedelta

# CẤU HÌNH THÔNG SỐ MẶC ĐỊNH
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 24), 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# KHỞI TẠO DAG
dag = DAG(
    'transform_silver_dag',          
    default_args=default_args,
    description='Luồng làm sạch, nội suy dữ liệu thiếu và chuẩn hóa tầng Silver',
    schedule_interval=None, # Để None vì sẽ được gọi tự động bởi luồng nạp Bronze
    catchup=False,
    tags=['transformation', 'silver', 'spark'],
)

#ĐỊNH NGHĨA TASK THỰC THI
# Task này sẽ sử dụng 'docker exec' để ra lệnh cho container jupyter-notebook 
# thực hiện spark-submit file xử lý logic
transform_task = BashOperator(
    task_id='run_transform_bronze_to_silver',
    bash_command='docker exec -u root jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/transform_bronze_to_silver.py',
    dag=dag,
)

#tự động gọi DAG Gold sau khi hoàn thành xong Silver
trigger_gold = TriggerDagRunOperator(
    task_id='trigger_gold_dag',
    trigger_dag_id='transform_gold_dag', 
    dag=dag
)

transform_task >> trigger_gold