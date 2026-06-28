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
            POSTGRES_URL,
        )
    else:
        generation_id = plan.generation_id
        versioned = recommendations.select(
            F.lit(generation_id).alias("generation_id"),
            "user_id",
            "product_id",
            "score",
        )
        row_count = versioned.count()
        if row_count <= 0:
            raise RuntimeError("ALS is a required component and cannot be empty")
        prepare_component_retry_spark(spark, generation_id, ("als",))
        export_dataframe_to_postgres(
            versioned,
            plan.target_table,
            POSTGRES_URL,
            plan.write_mode,
        )
        record_component_complete_spark(
            spark,
            generation_id,
            "als",
            row_count,
            {"source": "gold_db.recommendations_als"},
        )

    print(f"SUCCESS: Exported gold_db.recommendations_als to {plan.target_table}")


if __name__ == "__main__":
    export_als_recommendations()
