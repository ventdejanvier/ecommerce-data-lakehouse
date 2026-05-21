import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
sys.path.append('/home/jovyan/scripts')
from common_config import get_spark_session

def export_to_postgres(df):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/data_lakehouse") \
        .option("dbtable", "serving_recommendations") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def generate_cluster_recs():
    spark = get_spark_session("KMeans_Recommendation_Link")
    
    # Đọc bảng tương tác và bảng phân cụm từ Gold
    interactions = spark.table("gold_db.user_interactions")
    clusters = spark.table("gold_db.user_clusters")
    products = spark.table("gold_db.dim_products")

    # Join để biết mỗi tương tác thuộc về Cụm nào
    cluster_interactions = interactions.join(clusters, "user_id")

    # Tính Top 10 sản phẩm được yêu thích nhất trong mỗi cụm (Dựa trên tổng interaction_score)
    cluster_top_products = cluster_interactions.groupBy("cluster_id", "product_id").agg(
        F.sum("interaction_score").alias("cluster_total_score")
    )

    # Dùng Window function để lấy Top 10 của từng cụm
    window_spec = Window.partitionBy("cluster_id").orderBy(F.desc("cluster_total_score"))
    
    final_cluster_recs = cluster_top_products.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") <= 10) \
        .join(products, "product_id") \
        .select("cluster_id", "product_id", "display_name", "cluster_total_score")

    # Lưu kết quả vào Gold (bảng 'Gợi ý theo phân khúc')
    final_cluster_recs.write.format("delta").mode("overwrite") \
        .option("path", "s3a://gold/recommendations_by_cluster") \
        .saveAsTable("gold_db.recommendations_by_cluster")

    export_to_postgres(final_cluster_recs)

    print("SUCCESS: Đã kết nối K-Means với Hệ gợi ý thành công!")

if __name__ == "__main__":
    generate_cluster_recs()
