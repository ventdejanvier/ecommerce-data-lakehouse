import sys

from pyspark.sql import functions as F

sys.path.append("/home/jovyan/scripts")
from common_config import get_spark_session
from model_publication import (
    prepare_component_retry_spark,
    record_component_complete_spark,
    resolve_export_plan,
)


POSTGRES_URL = "jdbc:postgresql://postgres:5432/data_lakehouse"
POSTGRES_PROPERTIES = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver",
}


def export_dataframe_to_postgres(
    df,
    table_name: str,
    jdbc_url: str,
    mode: str = "overwrite",
):
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_PROPERTIES["user"]) \
        .option("password", POSTGRES_PROPERTIES["password"]) \
        .option("driver", POSTGRES_PROPERTIES["driver"]) \
        .mode(mode) \
        .save()


def require_min_columns(cols: list[str], table_name: str) -> None:
    if len(cols) < 3:
        raise ValueError(
            f"{table_name} must expose at least 3 columns for dynamic export. "
            f"Found columns: {cols}"
        )


def export_content_based_recommendations():
    plan = resolve_export_plan("content_based", "public.serving_content_based")
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

    if not plan.v2_enabled:
        export_dataframe_to_postgres(
            recommendations,
            plan.target_table,
            POSTGRES_URL,
        )
    else:
        generation_id = plan.generation_id
        if "user_id" in recommendations.columns:
            versioned = recommendations.select(
                F.lit(generation_id).alias("generation_id"),
                "user_id",
                "product_id",
                F.lit(None).cast("string").alias("source_product_id"),
                F.lit(None).cast("string").alias("recommended_product_id"),
                "score",
            )
        else:
            versioned = recommendations.select(
                F.lit(generation_id).alias("generation_id"),
                F.lit(None).cast("string").alias("user_id"),
                F.lit(None).cast("string").alias("product_id"),
                "source_product_id",
                "recommended_product_id",
                "score",
            )
        row_count = versioned.count()
        if row_count <= 0:
            raise RuntimeError("Content-based is a required component and cannot be empty")
        prepare_component_retry_spark(spark, generation_id, ("content_based",))
        export_dataframe_to_postgres(
            versioned,
            plan.target_table,
            POSTGRES_URL,
            plan.write_mode,
        )
        record_component_complete_spark(
            spark,
            generation_id,
            "content_based",
            row_count,
            {"source": "gold_db.recommendations_content_based"},
        )

    print(
        "SUCCESS: Exported gold_db.recommendations_content_based "
        f"to {plan.target_table}"
    )


if __name__ == "__main__":
    export_content_based_recommendations()
