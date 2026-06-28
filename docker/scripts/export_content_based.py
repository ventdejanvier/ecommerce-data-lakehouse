import sys

from pyspark.sql import functions as F

sys.path.append("/home/jovyan/scripts")
from common_config import get_spark_session
from model_publication import (
    export_versioned_component_spark,
    resolve_content_v2_schema,
    resolve_export_plan,
    write_dataframe_to_postgres,
)


def export_dataframe_to_postgres(
    df,
    table_name: str,
    mode: str = "overwrite",
):
    write_dataframe_to_postgres(df, table_name, mode)


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

    if not plan.v2_enabled:
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
            plan.target_table,
        )
    else:
        generation_id = plan.generation_id
        logical_form, mapping = resolve_content_v2_schema(cols)
        if logical_form == "user_level":
            versioned = gold_recommendations.select(
                F.lit(generation_id).alias("generation_id"),
                F.col(mapping["user_id"]).cast("string").alias("user_id"),
                F.col(mapping["product_id"]).cast("string").alias("product_id"),
                F.lit(None).cast("string").alias("source_product_id"),
                F.lit(None).cast("string").alias("recommended_product_id"),
                F.col(mapping["score"]).cast("double").alias("score"),
            ).filter(
                F.col("user_id").isNotNull()
                & F.col("product_id").isNotNull()
                & F.col("score").isNotNull()
            )
        else:
            versioned = gold_recommendations.select(
                F.lit(generation_id).alias("generation_id"),
                F.lit(None).cast("string").alias("user_id"),
                F.lit(None).cast("string").alias("product_id"),
                F.col(mapping["source_product_id"])
                .cast("string")
                .alias("source_product_id"),
                F.col(mapping["recommended_product_id"])
                .cast("string")
                .alias("recommended_product_id"),
                F.col(mapping["score"]).cast("double").alias("score"),
            ).filter(
                F.col("source_product_id").isNotNull()
                & F.col("recommended_product_id").isNotNull()
                & F.col("score").isNotNull()
            )
        export_versioned_component_spark(
            spark,
            versioned,
            generation_id,
            "content_based",
            {"source": "gold_db.recommendations_content_based"},
        )

    print(
        "SUCCESS: Exported gold_db.recommendations_content_based "
        f"to {plan.target_table}"
    )


if __name__ == "__main__":
    export_content_based_recommendations()
