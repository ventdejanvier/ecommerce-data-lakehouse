from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.path.append("/home/jovyan/scripts")
from common_config import get_spark_session

spark = get_spark_session("ExportUnifiedData")

# Đọc dữ liệu từ tầng Gold 
fact_events = spark.read.table("gold_db.fact_events")
dim_products = spark.read.table("gold_db.dim_products")

#Tính Global Popularity
global_popular = fact_events.filter(F.col("event_type") == "purchase") \
    .groupBy("product_id") \
    .agg(F.count("product_id").alias("total_sales")) \
    .orderBy(F.desc("total_sales")) \
    .limit(10) \
    .join(dim_products, "product_id")

# Chuẩn bị kết nối DB
jdbc_url = "jdbc:postgresql://postgres:5432/data_lakehouse"
properties = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

# Ghi dữ liệu vào Postgres
# Ghi bảng Popularity
global_popular.write.jdbc(url=jdbc_url, table="public.global_popular", mode="overwrite", properties=properties)
# Ghi bảng Product để web có dữ liệu hiển thị
dim_products.write.jdbc(url=jdbc_url, table="public.dim_products", mode="overwrite", properties=properties)

print("Đã xuất dữ liệu thành công vào PostgreSQL")