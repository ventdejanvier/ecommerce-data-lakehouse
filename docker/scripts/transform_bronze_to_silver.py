import sys
import os
from pyspark.sql import functions as F

sys.path.append('/home/jovyan/scripts')
from common_config import get_spark_session

def transform_silver():
    # Khởi tạo Spark Session cho tầng Silver
    spark = get_spark_session("Bronze_To_Silver_Enrichment")
    
    #ĐỌC DỮ LIỆU TỪ TẦNG BRONZE
    print("Đọc dữ liệu từ tầng Bronze")
    bronze_df = spark.read.format("delta").load("s3a://bronze/electronics_events")

    # LOẠI BỎ TRÙNG LẶP
    # Loại bỏ các record giống hệt nhau về User, Product, Thời gian và Hành vi
    print("Loại bỏ trùng lặp")
    dedup_df = bronze_df.dropDuplicates(["event_time", "event_type", "product_id", "user_id"])

    #XỬ LÝ DỮ LIỆU THIẾU 
    print("xử lý dữ liệu thiếu cho category_code và brand")
    
    #Tạo bảng tra cứu (Lookup) cho Category từ những dòng không bị Null
    category_lookup = dedup_df.filter(F.col("category_code").isNotNull()) \
        .select("category_id", "category_code").distinct()

    #Tạo bảng tra cứu (Lookup) cho Brand từ những dòng không bị Null
    brand_lookup = dedup_df.filter(F.col("brand").isNotNull()) \
        .select("product_id", "brand").distinct()

    #Join và lấp đầy Null
    # Dùng Broadcast Join cho các bảng lookup nhỏ để tối ưu hiệu năng
    silver_step_1 = dedup_df.alias("main").join(
        F.broadcast(category_lookup).alias("cat"), 
        on="category_id", 
        how="left"
    ).join(
        F.broadcast(brand_lookup).alias("brd"), 
        on="product_id", 
        how="left"
    )

    # Áp dụng logic Nội suy đa cấp
    # Lấy giá trị gốc -> Nếu Null, lấy từ Lookup -> Nếu vẫn Null, gán nhãn đại diện
    df_imputed = silver_step_1.select(
        "main.*",
        F.coalesce(
            F.col("main.category_code"), 
            F.col("cat.category_code"), 
            F.concat(F.lit("Category_"), F.col("main.category_id").cast("string"))
        ).alias("category_fixed"),
        F.coalesce(
            F.col("main.brand"), 
            F.col("brd.brand"), 
            F.lit("Generic_Brand")
        ).alias("brand_fixed")
    ).drop("category_code", "brand")
 
    # BƯỚC 4: LÀM GIÀU DỮ LIỆU 
    print("Làm giàu dữ liệu")
    
    final_df = df_imputed.withColumn(
        "product_name", 
        F.concat(
            F.col("brand_fixed"), 
            F.lit(" "), 
            F.col("category_fixed"),
            F.lit(" #"), 
            F.col("product_id").cast("string")
        )
    ).withColumn(
        "event_date", 
        F.to_date("event_time") # Chuyển sang Date để phân vùng 
    )

    # GHI DỮ LIỆU XUỐNG TẦNG SILVER
    # Sử dụng PartitionBy theo 'event_date' để tối ưu hóa truy vấn phân tích 
    output_path = "s3a://silver/electronics_events"
    print(f"Ghi dữ liệu xuống {output_path}...")
    
    final_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("event_date") \
        .save(output_path)
    
    print("Quá trình chuyển đổi từ Bronze lên Silver hoàn tất")

if __name__ == "__main__":
    transform_silver()