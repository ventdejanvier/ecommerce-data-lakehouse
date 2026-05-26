import sys

from pyspark.ml.recommendation import ALS
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


def build_index_maps(interactions):
    user_window = Window.orderBy("user_id")
    product_window = Window.orderBy("product_id")

    users = interactions.select("user_id").distinct() \
        .withColumn("user_index", (F.row_number().over(user_window) - 1).cast("int"))

    products = interactions.select("product_id").distinct() \
        .withColumn("product_index", (F.row_number().over(product_window) - 1).cast("int"))

    indexed_interactions = interactions \
        .join(users, "user_id") \
        .join(products, "product_id") \
        .select("user_id", "product_id", "user_index", "product_index", "interaction_score")

    return indexed_interactions, users, products


def export_als_recommendations():
    spark = get_spark_session("Export_ALS_Recommendations")

    interactions = spark.table("gold_db.user_interactions") \
        .select("user_id", "product_id", F.col("interaction_score").cast("double").alias("interaction_score")) \
        .filter(
            F.col("user_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("interaction_score").isNotNull()
            & (F.col("interaction_score") > 0)
        )

    products_dim = spark.table("gold_db.dim_products") \
        .select("product_id", "display_name")

    indexed_interactions, user_map, product_map = build_index_maps(interactions)

    als = ALS(
        userCol="user_index",
        itemCol="product_index",
        ratingCol="interaction_score",
        implicitPrefs=True,
        rank=20,
        maxIter=10,
        regParam=0.1,
        alpha=1.0,
        nonnegative=True,
        coldStartStrategy="drop",
        seed=42,
    )

    model = als.fit(indexed_interactions)

    recommendations = model.recommendForAllUsers(10) \
        .select("user_index", F.posexplode("recommendations").alias("position", "recommendation")) \
        .select(
            "user_index",
            (F.col("position") + 1).alias("rank"),
            F.col("recommendation.product_index").alias("product_index"),
            F.col("recommendation.rating").cast("double").alias("score"),
        ) \
        .filter(F.col("score").isNotNull() & (F.col("score") >= 0)) \
        .join(user_map, "user_index") \
        .join(product_map, "product_index") \
        .join(products_dim, "product_id", "left") \
        .select(
            "user_id",
            "product_id",
            "display_name",
            F.round("score", 6).alias("score"),
            "rank",
        ) \
        .orderBy("user_id", "rank")

    export_to_postgres(recommendations, "public.serving_als")

    print("SUCCESS: Exported ALS recommendations to public.serving_als")


if __name__ == "__main__":
    export_als_recommendations()
