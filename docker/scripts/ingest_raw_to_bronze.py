import sys
from common_config import get_spark_session
from pyspark.sql.functions import current_timestamp, lit, to_date

def ingest(): 
    execution_date = sys.argv[1] if len(sys.argv) > 1 else "manual"
    
    spark = get_spark_session("IngestToBronze")
    
    # Đọc dữ liệu thô
    raw_df = spark.read.csv("/home/jovyan/data/electronics_events.csv", header=True, inferSchema=True)
    
    bronze_df = raw_df.withColumn("ingestion_at", current_timestamp()) \
                      .withColumn("execution_date", lit(execution_date))
    
    # GHI DỮ LIỆU THEO PARTITION
    bronze_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("execution_date") \
        .save("s3a://bronze/electronics_events")
    
    print(f"Done: Ingested data for date {execution_date}")

if __name__ == "__main__":
    ingest()