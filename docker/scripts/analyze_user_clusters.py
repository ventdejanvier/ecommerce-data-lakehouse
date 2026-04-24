import sys
import os
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# Import cấu hình dùng chung
sys.path.append('/home/jovyan/scripts')
from common_config import get_spark_session

def run_user_clustering():
    # Khởi tạo Spark Session 
    spark = get_spark_session("User_Behavior_Clustering_Production")
    
    # Đọc bảng RFM từ tầng Gold 
    rfm_df = spark.table("gold_db.user_rfm")
    
    # Tiền xử lý (Feature Engineering)
    assembler = VectorAssembler(inputCols=["recency", "frequency", "monetary"], outputCol="features")
    feature_df = assembler.transform(rfm_df)
    
    # Chuẩn hóa 
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    scaler_model = scaler.fit(feature_df)
    final_data = scaler_model.transform(feature_df)
    
    # Chạy K-Means với K=4   
    kmeans = KMeans(featuresCol="scaledFeatures", k=4, seed=42)
    model = kmeans.fit(final_data)
    
    # Dự đoán nhãn phân cụm cho từng user
    predictions = model.transform(final_data)
    
    # Ánh xạ nhãn sang tên phân khúc cụ thể   
    final_clusters = predictions.withColumn("segment_name", 
        F.when(F.col("prediction") == 1, "Champions")
         .when(F.col("prediction") == 2, "Loyal")
         .when(F.col("prediction") == 0, "Recent Browsers")
         .otherwise("At Risk")
    ).select(
        "user_id", 
        F.col("prediction").alias("cluster_id"), 
        "segment_name"
    )
    
    # Lưu kết quả vào Gold Layer và đăng ký bảng vào Hive 
    final_clusters.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("path", "s3a://gold/user_clusters") \
        .saveAsTable("gold_db.user_clusters")
    
    print("Phân cụm người dùng đã được lưu vào gold_db.user_clusters")

if __name__ == "__main__":
    run_user_clustering()