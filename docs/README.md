# Frontend (terminal 1)
```bash
pnpm install
pnpm dev
```
# Backend (terminal 2)
```bash
uv venv
uv init
uv sync

$env:REDIS_HOST="127.0.0.1"
uv run uvicorn main:app --reload --port 8000
```

# Docker (terminal 3)
docker exec -it -e MINIO_ACCESS_KEY="admin" -e MINIO_SECRET_KEY="password" -e TRACKING_OUTPUT_PATH="s3a://silver/tracking_events_v2/" -e TRACKING_CHECKPOINT_PATH="s3a://silver/checkpoints/tracking_events_v2/" -e TRACKING_QUARANTINE_PATH="s3a://silver/quarantine/tracking_events_v2/" -e TRACKING_QUARANTINE_CHECKPOINT_PATH="s3a://silver/quarantine/checkpoints/tracking_events_v2/" jupyter-notebook spark-submit --packages io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.2 /home/jovyan/scripts/spark_streaming_bronze_to_silver.py

# Backend (terminal 4)
python scripts/kafka_to_minio.py

