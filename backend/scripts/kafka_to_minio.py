#!/usr/bin/env python
import os
import sys
import time
import json
import datetime
import tempfile
import logging
from confluent_kafka import Consumer
from minio import Minio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("kafka_to_minio")

def main():
    logger.info("Starting Kafka to MinIO Bronze Layer Ingestion Consumer...")

    # Load configurations from environment variables with safe fallbacks
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "minio-ingest-consumer")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "product_events")
    
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() in ("true", "1", "yes")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
    
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
    TIME_WINDOW = float(os.getenv("TIME_WINDOW", "30.0")) # in seconds

    logger.info("Configuration loaded:")
    logger.info(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Kafka Topic:     {KAFKA_TOPIC}")
    logger.info(f"  Kafka Group ID:  {KAFKA_GROUP_ID}")
    logger.info(f"  MinIO Endpoint:  {MINIO_ENDPOINT} (Secure: {MINIO_SECURE})")
    logger.info(f"  MinIO Bucket:    {MINIO_BUCKET}")
    logger.info(f"  Batching Config: BATCH_SIZE={BATCH_SIZE}, TIME_WINDOW={TIME_WINDOW}s")

    # Connect/Initialize MinIO client
    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        if not minio_client.bucket_exists(MINIO_BUCKET):
            logger.info(f"Bucket '{MINIO_BUCKET}' does not exist. Creating bucket...")
            minio_client.make_bucket(MINIO_BUCKET)
        else:
            logger.info(f"Bucket '{MINIO_BUCKET}' already exists.")
    except Exception as e:
        logger.exception("Initialization error with MinIO client")
        sys.exit(1)

    # Initialize Kafka Consumer
    try:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        })
        consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Successfully subscribed to topic: '{KAFKA_TOPIC}'")
    except Exception as e:
        logger.exception("Initialization error with Kafka Consumer")
        sys.exit(1)

    buffer = []
    last_flush_time = time.time()

    try:
        while True:
            # Poll for new messages (1 second timeout)
            msg = consumer.poll(1.0)
            
            # Check elapsed time
            current_time = time.time()
            elapsed_time = current_time - last_flush_time

            if msg is not None:
                if msg.error():
                    logger.error(f"Kafka consume error: {msg.error()}")
                else:
                    try:
                        # Decode message value
                        payload = msg.value().decode("utf-8")
                        event_data = json.loads(payload)
                        buffer.append(event_data)
                        logger.info(f"Received event. Buffer size: {len(buffer)}/{BATCH_SIZE}")
                    except Exception as e:
                        logger.error(f"Failed to parse event message JSON: {e}")

            # Flush condition logic
            should_flush = len(buffer) > 0 and (len(buffer) >= BATCH_SIZE or elapsed_time >= TIME_WINDOW)

            if should_flush:
                logger.info(f"Triggering buffer flush. Buffer size: {len(buffer)}, Elapsed: {elapsed_time:.2f}s")
                temp_file_path = None
                try:
                    # Write buffer to a local temporary file in JSONL format
                    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as temp_file:
                        temp_file_path = temp_file.name
                        for item in buffer:
                            temp_file.write(json.dumps(item) + "\n")

                    # Generate absolute partitioned path in MinIO
                    now = datetime.datetime.now()
                    partition_date = now.strftime("%Y-%m-%d")
                    time_stamp = now.strftime("%H%M%S")
                    object_name = f"tracking_events/{partition_date}/events_{time_stamp}.jsonl"

                    logger.info(f"Uploading batch file to MinIO: '{MINIO_BUCKET}/{object_name}'")
                    minio_client.fput_object(
                        bucket_name=MINIO_BUCKET,
                        object_name=object_name,
                        file_path=temp_file_path,
                        content_type="application/x-jsonlines"
                    )
                    logger.info("Micro-batch uploaded successfully.")

                    # Clear buffer and reset flush timer
                    buffer.clear()
                    last_flush_time = time.time()

                except Exception as upload_error:
                    logger.error(f"Failed to ingest micro-batch to MinIO: {upload_error}")
                finally:
                    # Always clean up the local temp file
                    if temp_file_path and os.path.exists(temp_file_path):
                        try:
                            os.remove(temp_file_path)
                        except OSError as cleanup_error:
                            logger.warning(f"Failed to delete temp file '{temp_file_path}': {cleanup_error}")

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Exiting consumer.")
    finally:
        logger.info("Closing Kafka consumer.")
        consumer.close()
        logger.info("Consumer shutdown completed.")

if __name__ == "__main__":
    main()
