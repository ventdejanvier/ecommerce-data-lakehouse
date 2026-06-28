import sys

from pyspark.sql import functions as F

sys.path.append("/home/jovyan/scripts")
from common_config import get_spark_session
from model_publication import (
    export_versioned_component_spark,
    resolve_export_plan,
    write_dataframe_to_postgres,
)


def export_dataframe_to_postgres(
    df,
    table_name: str,
    mode: str = "overwrite",
):
    write_dataframe_to_postgres(df, table_name, mode)


def export_als_recommendations():
    plan = resolve_export_plan("als", "public.serving_als")
    spark = get_spark_session("Export_ALS_Recommendations")

    recommendations = spark.table("gold_db.recommendations_als") \
        .select(
            F.col("user_id").cast("string").alias("user_id"),
            F.col("product_id").cast("string").alias("product_id"),
            F.col("score").cast("double").alias("score"),
        ) \
        .filter(
            F.col("user_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("score").isNotNull()
        )

    if not plan.v2_enabled:
        export_dataframe_to_postgres(
            recommendations,
            plan.target_table,
        )
    else:
        generation_id = plan.generation_id
        versioned = recommendations.select(
            F.lit(generation_id).alias("generation_id"),
            "user_id",
            "product_id",
            "score",
        )
        export_versioned_component_spark(
            spark,
            versioned,
            generation_id,
            "als",
            {"source": "gold_db.recommendations_als"},
        )

    print(f"SUCCESS: Exported gold_db.recommendations_als to {plan.target_table}")


if __name__ == "__main__":
    export_als_recommendations()
