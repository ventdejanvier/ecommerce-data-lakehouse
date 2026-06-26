import sys
import os
from common_config import get_spark_session
from pyspark.sql.functions import current_timestamp, lit, to_date

def ingest(): 
    execution_date = sys.argv[1] if len(sys.argv) > 1 else "manual"
    
    spark = get_spark_session("IngestToBronze")
    
    # Đọc dữ liệu thô
    raw_df = spark.read.csv("/home/jovyan/data/electronics_events.csv", header=True, inferSchema=True)

    required_columns = {"brand", "category_code", "category_id", "event_time", "event_type", "price", "product_id", "user_id", "user_session"}
    missing_columns = sorted(required_columns - set(raw_df.columns))
    if missing_columns:
        raise RuntimeError(f"Raw source is missing required columns: {', '.join(missing_columns)}")

    row_count = raw_df.count()
    if row_count == 0:
        raise RuntimeError("Raw source is empty; refusing to overwrite Bronze target")
    
    bronze_df = raw_df.withColumn("ingestion_at", current_timestamp()) \
                      .withColumn("execution_date", lit(execution_date))

    target_path = os.getenv("BRONZE_OUTPUT_PATH", "s3a://bronze/electronics_events")
    
    # GHI DỮ LIỆU THEO PARTITION
    bronze_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("execution_date") \
        .save(target_path)
    
    print(f"Done: Ingested data for date {execution_date}")

if __name__ == "__main__":
    ingest()
