import sys

from pyspark.sql import functions as F

sys.path.append("/home/jovyan/scripts")
from common_config import get_spark_session


POSTGRES_URL = "jdbc:postgresql://postgres:5432/data_lakehouse"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver",
}


def export_dataframe_to_postgres(df, table_name: str, jdbc_url: str):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_PROPERTIES["user"]) \
        .option("password", POSTGRES_PROPERTIES["password"]) \
        .option("driver", POSTGRES_PROPERTIES["driver"]) \
        .mode("overwrite") \
        .save()


def first_existing_column(df, candidates: tuple[str, ...], role: str) -> str:
    for column in candidates:
        if column in df.columns:
            return column

    raise ValueError(
        f"gold_db.recommendations_itembased is missing a column for {role}. "
        f"Expected one of: {', '.join(candidates)}"
    )


def export_item_based_recommendations():
    spark = get_spark_session("Export_Item_Based_Recommendations")

    gold_recommendations = spark.table("gold_db.recommendations_itembased")
    source_col = first_existing_column(
        gold_recommendations,
        ("source_product_id", "product_id_1", "product_id"),
        "source product",
    )
    similar_col = first_existing_column(
        gold_recommendations,
        ("similar_product_id", "recommended_product_id", "target_product_id", "product_id_2"),
        "similar product",
    )
    score_col = first_existing_column(
        gold_recommendations,
        ("score", "similarity", "similarity_score", "cosine_similarity"),
        "score",
    )

    recommendations = gold_recommendations \
        .select(
            F.col(source_col).cast("string").alias("source_product_id"),
            F.col(similar_col).cast("string").alias("similar_product_id"),
            F.col(score_col).cast("double").alias("score"),
        ) \
        .filter(
            F.col("source_product_id").isNotNull()
            & F.col("similar_product_id").isNotNull()
            & F.col("score").isNotNull()
        )

    export_dataframe_to_postgres(
        recommendations,
        "public.serving_item_based",
        POSTGRES_URL,
    )

    print("SUCCESS: Exported gold_db.recommendations_itembased to public.serving_item_based")


if __name__ == "__main__":
    export_item_based_recommendations()
