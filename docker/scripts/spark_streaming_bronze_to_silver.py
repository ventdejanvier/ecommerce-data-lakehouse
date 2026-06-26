#!/usr/bin/env python
import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, concat_ws, current_timestamp, from_json, to_timestamp, when

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("spark_streaming_bronze_to_silver")

def apply_trigger(writer, stream_trigger_once):
    if stream_trigger_once.lower() == "true":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime="10 seconds")

def main():
    logger.info("Initializing Spark Structured Streaming session...")

    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables are required")

    tracking_input_path = os.getenv("TRACKING_INPUT_PATH", "s3a://bronze/tracking_events/*/*.jsonl")
    tracking_output_path = os.getenv("TRACKING_OUTPUT_PATH", "s3a://silver/tracking_events/")
    tracking_checkpoint_path = os.getenv("TRACKING_CHECKPOINT_PATH", "s3a://silver/checkpoints/tracking_events/")
    tracking_quarantine_path = os.getenv("TRACKING_QUARANTINE_PATH", "s3a://silver/quarantine/tracking_events/")
    tracking_quarantine_checkpoint_path = os.getenv(
        "TRACKING_QUARANTINE_CHECKPOINT_PATH",
        "s3a://silver/quarantine/checkpoints/tracking_events/"
    )
    stream_trigger_once = os.getenv("STREAM_TRIGGER_ONCE", "false")

    spark = None
    try:
        # Initialize Spark Session with Delta and S3 (MinIO) config
        spark = SparkSession.builder \
            .appName("SparkStreamingBronzeToSilver") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()

        # Define schema for the raw frontend tracking event contract in JSONL
        json_schema = StructType([
            StructField("eventId", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("eventType", StringType(), True),
            StructField("productId", StringType(), True),
            StructField("userId", StringType(), True),
            StructField("sessionId", StringType(), True),
            StructField("productPrice", DoubleType(), True)
        ])

        logger.info(f"Reading stream from Bronze path: {tracking_input_path}")
        # Read stream as text to preserve the exact raw JSON payload for audit and quarantine
        raw_stream = spark.readStream \
            .format("text") \
            .option("maxFilesPerTrigger", 1) \
            .load(tracking_input_path) \
            .withColumnRenamed("value", "raw_json")

        parsed_stream = raw_stream.select(
            col("raw_json"),
            from_json(col("raw_json"), json_schema).alias("event")
        )

        # Map frontend camelCase event contract to canonical Silver snake_case schema
        canonical_df = parsed_stream.select(
            col("event.eventId").alias("event_id"),
            to_timestamp(col("event.timestamp")).alias("event_time"),
            col("event.eventType").alias("event_type"),
            col("event.productId").alias("product_id"),
            col("event.userId").alias("user_id"),
            col("event.sessionId").alias("session_id"),
            col("event.productPrice").cast("double").alias("product_price"),
            col("raw_json")
        )

        mandatory_fields_present = (
            col("event_id").isNotNull()
            & col("event_time").isNotNull()
            & col("event_type").isNotNull()
            & col("user_id").isNotNull()
            & col("session_id").isNotNull()
        )

        validation_errors = concat_ws(
            ",",
            when(col("event_id").isNull(), "event_id"),
            when(col("event_time").isNull(), "event_time"),
            when(col("event_type").isNull(), "event_type"),
            when(col("user_id").isNull(), "user_id"),
            when(col("session_id").isNull(), "session_id"),
        )

        valid_df = canonical_df.filter(mandatory_fields_present)
        invalid_df = canonical_df.filter(~mandatory_fields_present) \
            .withColumn("validation_errors", validation_errors) \
            .withColumn("quarantined_at", current_timestamp())

        logger.info(f"Writing valid tracking events to Silver path: {tracking_output_path}")
        valid_writer = valid_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", tracking_checkpoint_path)

        valid_query = apply_trigger(valid_writer, stream_trigger_once) \
            .start(tracking_output_path)

        logger.info(f"Writing invalid tracking events to quarantine path: {tracking_quarantine_path}")
        quarantine_writer = invalid_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", tracking_quarantine_checkpoint_path)

        quarantine_query = apply_trigger(quarantine_writer, stream_trigger_once) \
            .start(tracking_quarantine_path)

        logger.info("Streaming job active. Awaiting new files...")
        if stream_trigger_once.lower() == "true":
            valid_query.awaitTermination()
            quarantine_query.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    main()
