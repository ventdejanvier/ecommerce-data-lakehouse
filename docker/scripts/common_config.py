import os

from pyspark.sql import SparkSession

SPARK_PACKAGES = ",".join([
    "io.delta:delta-core_2.12:2.1.0",
    "org.apache.hadoop:hadoop-aws:3.3.2",
    "org.postgresql:postgresql:42.7.3",
])

def get_spark_session(app_name):
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise RuntimeError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables are required"
        )
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", SPARK_PACKAGES) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .enableHiveSupport() \
        .getOrCreate()
