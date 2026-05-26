import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append("/home/jovyan/scripts")
from common_config import get_spark_session


POSTGRES_URL = "jdbc:postgresql://postgres:5432/data_lakehouse"


def export_to_postgres(df, table_name: str):
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()


def export_item_based_recommendations():
    spark = get_spark_session("Export_Item_Based_Recommendations")

    interactions = spark.table("gold_db.user_interactions") \
        .select("user_id", "product_id", F.col("interaction_score").cast("double").alias("interaction_score")) \
        .filter(
            F.col("user_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("interaction_score").isNotNull()
            & (F.col("interaction_score") > 0)
        )

    products_dim = spark.table("gold_db.dim_products") \
        .select("product_id", "display_name") \
        .dropDuplicates(["product_id"])

    item_norms = interactions.groupBy("product_id") \
        .agg(F.sqrt(F.sum(F.col("interaction_score") * F.col("interaction_score"))).alias("norm"))

    left = interactions.select(
        "user_id",
        F.col("product_id").alias("source_product_id"),
        F.col("interaction_score").alias("source_score"),
    )
    right = interactions.select(
        "user_id",
        F.col("product_id").alias("similar_product_id"),
        F.col("interaction_score").alias("similar_score"),
    )

    pair_scores = left.join(right, "user_id") \
        .filter(F.col("source_product_id") != F.col("similar_product_id")) \
        .groupBy("source_product_id", "similar_product_id") \
        .agg(
            F.sum(F.col("source_score") * F.col("similar_score")).alias("dot_product"),
            F.count("*").alias("co_interaction_count"),
        )

    source_norms = item_norms.select(
        F.col("product_id").alias("source_product_id"),
        F.col("norm").alias("source_norm"),
    )
    similar_norms = item_norms.select(
        F.col("product_id").alias("similar_product_id"),
        F.col("norm").alias("similar_norm"),
    )

    scored_pairs = pair_scores \
        .join(source_norms, "source_product_id") \
        .join(similar_norms, "similar_product_id") \
        .withColumn(
            "score",
            F.when(
                (F.col("source_norm") > 0) & (F.col("similar_norm") > 0),
                F.col("dot_product") / (F.col("source_norm") * F.col("similar_norm")),
            ).otherwise(F.lit(0.0)),
        ) \
        .filter(F.col("score").isNotNull() & (F.col("score") >= 0))

    rank_window = Window.partitionBy("source_product_id") \
        .orderBy(F.desc("score"), F.desc("co_interaction_count"), F.asc("similar_product_id"))

    source_products = products_dim.select(
        F.col("product_id").alias("source_product_id"),
        F.col("display_name").alias("source_display_name"),
    )
    similar_products = products_dim.select(
        F.col("product_id").alias("similar_product_id"),
        F.col("display_name").alias("similar_display_name"),
    )

    recommendations = scored_pairs \
        .withColumn("rank", F.row_number().over(rank_window)) \
        .filter(F.col("rank") <= 10) \
        .join(source_products, "source_product_id", "left") \
        .join(similar_products, "similar_product_id", "left") \
        .select(
            "source_product_id",
            "similar_product_id",
            "source_display_name",
            "similar_display_name",
            F.round("score", 6).alias("score"),
            "co_interaction_count",
            "rank",
        ) \
        .orderBy("source_product_id", "rank")

    export_to_postgres(recommendations, "public.serving_item_based")

    print("SUCCESS: Exported item-based recommendations to public.serving_item_based")


if __name__ == "__main__":
    export_item_based_recommendations()
