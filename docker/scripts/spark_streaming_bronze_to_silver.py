#!/usr/bin/env python
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("spark_streaming_bronze_to_silver")

def main():
    logger.info("Initializing Spark Structured Streaming session...")

    # Initialize Spark Session with Delta and S3 (MinIO) config
    spark = SparkSession.builder \
        .appName("SparkStreamingBronzeToSilver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    # Define schema for the raw tracking events JSONL files in the Bronze layer
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_session", StringType(), True)
    ])

    logger.info("Reading stream from Bronze bucket (s3a://bronze/tracking_events/)...")
    # Read stream from Bronze bucket with light triggers (maxFilesPerTrigger=1) to prevent JVM OOM
    bronze_stream = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .json("s3a://bronze/tracking_events/*/*.jsonl")

    # Cast event_time from String to Timestamp type
    silver_df = bronze_stream.select(
        to_timestamp(col("event_time")).alias("event_time"),
        col("event_type"),
        col("product_id"),
        col("user_id"),
        col("price"),
        col("user_session")
    )

    logger.info("Writing stream to Silver bucket as a Delta table...")
    # Write Structured Stream as Delta Table to the Silver bucket in append mode
    query = silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/checkpoints/tracking_events/") \
        .trigger(processingTime="10 seconds") \
        .start("s3a://silver/tracking_events/")

    logger.info("Streaming job active. Awaiting new files...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
