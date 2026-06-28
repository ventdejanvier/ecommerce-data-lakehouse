import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
sys.path.append('/home/jovyan/scripts')
from common_config import get_spark_session
from model_publication import (
    prepare_component_retry_spark,
    record_component_complete_spark,
    resolve_export_plan,
)


def export_to_postgres(df, table_name: str, mode: str = "overwrite"):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/data_lakehouse") \
        .option("dbtable", table_name) \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()


def generate_cluster_recs():
    recommendation_plan = resolve_export_plan(
        "cluster_recommendations",
        "serving_recommendations",
    )
    cluster_plan = resolve_export_plan(
        "user_clusters",
        "serving_user_clusters",
    )
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
    if not recommendation_plan.v2_enabled:
        final_cluster_recs.write.format("delta").mode("overwrite") \
            .option("path", "s3a://gold/recommendations_by_cluster") \
            .saveAsTable("gold_db.recommendations_by_cluster")
        export_to_postgres(final_cluster_recs, recommendation_plan.target_table)
        export_to_postgres(clusters, cluster_plan.target_table)
    else:
        generation_id = recommendation_plan.generation_id
        if generation_id != cluster_plan.generation_id:
            raise RuntimeError("Cluster exports resolved different generation IDs")

        versioned_recommendations = final_cluster_recs.select(
            F.lit(generation_id).alias("generation_id"),
            F.col("cluster_id").cast("int").alias("cluster_id"),
            F.col("product_id").cast("string").alias("product_id"),
            F.col("display_name").cast("string").alias("display_name"),
            F.col("cluster_total_score").cast("double").alias("cluster_total_score"),
        )
        versioned_clusters = clusters.select(
            F.lit(generation_id).alias("generation_id"),
            F.col("user_id").cast("string").alias("user_id"),
            F.col("cluster_id").cast("int").alias("cluster_id"),
            F.col("segment_name").cast("string").alias("segment_name"),
        )
        recommendation_count = versioned_recommendations.count()
        cluster_count = versioned_clusters.count()
        if recommendation_count <= 0 or cluster_count <= 0:
            raise RuntimeError("Cluster serving components must both contain rows")

        prepare_component_retry_spark(
            spark,
            generation_id,
            ("cluster_recommendations", "user_clusters"),
        )
        export_to_postgres(
            versioned_recommendations,
            recommendation_plan.target_table,
            recommendation_plan.write_mode,
        )
        record_component_complete_spark(
            spark,
            generation_id,
            "cluster_recommendations",
            recommendation_count,
            {"source": "gold_db.user_interactions + gold_db.user_clusters"},
        )
        export_to_postgres(
            versioned_clusters,
            cluster_plan.target_table,
            cluster_plan.write_mode,
        )
        record_component_complete_spark(
            spark,
            generation_id,
            "user_clusters",
            cluster_count,
            {"source": "gold_db.user_clusters"},
        )

    print("SUCCESS: Đã kết nối K-Means với Hệ gợi ý thành công!")

if __name__ == "__main__":
    generate_cluster_recs()
