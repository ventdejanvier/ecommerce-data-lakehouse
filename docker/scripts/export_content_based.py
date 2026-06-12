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


def require_min_columns(cols: list[str], table_name: str) -> None:
    if len(cols) < 3:
        raise ValueError(
            f"{table_name} must expose at least 3 columns for dynamic export. "
            f"Found columns: {cols}"
        )


def export_content_based_recommendations():
    spark = get_spark_session("Export_Content_Based_Recommendations")

    gold_recommendations = spark.table("gold_db.recommendations_content_based")
    cols = gold_recommendations.columns
    require_min_columns(cols, "gold_db.recommendations_content_based")

    if "user_id" in cols:
        product_col = "product_id" if "product_id" in cols else cols[1]
        score_col = "score" if "score" in cols else cols[2]
        recommendations = gold_recommendations \
            .select(
                F.col("user_id").cast("string").alias("user_id"),
                F.col(product_col).cast("string").alias("product_id"),
                F.col(score_col).cast("double").alias("score"),
            ) \
            .filter(
                F.col("user_id").isNotNull()
                & F.col("product_id").isNotNull()
                & F.col("score").isNotNull()
            )
    else:
        recommendations = gold_recommendations \
            .select(
                F.col(cols[0]).cast("string").alias("source_product_id"),
                F.col(cols[1]).cast("string").alias("recommended_product_id"),
                F.col(cols[2]).cast("double").alias("score"),
            ) \
            .filter(
                F.col("source_product_id").isNotNull()
                & F.col("recommended_product_id").isNotNull()
                & F.col("score").isNotNull()
            )

    export_dataframe_to_postgres(
        recommendations,
        "public.serving_content_based",
        POSTGRES_URL,
    )

    print("SUCCESS: Exported gold_db.recommendations_content_based to public.serving_content_based")


if __name__ == "__main__":
    export_content_based_recommendations()
