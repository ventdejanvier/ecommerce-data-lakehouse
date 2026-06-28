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
            f"{table_name} must expose at least 3 columns: source item, similar item, score. "
            f"Found columns: {cols}"
        )


def export_item_based_recommendations():
    plan = resolve_export_plan("item_based", "public.serving_item_based")
    spark = get_spark_session("Export_Item_Based_Recommendations")

    item_similarity = spark.table("gold_db.item_similarity_matrix")
    cols = item_similarity.columns
    require_min_columns(cols, "gold_db.item_similarity_matrix")

    recommendations = item_similarity \
        .select(
            F.col(cols[0]).cast("string").alias("source_product_id"),
            F.col(cols[1]).cast("string").alias("similar_product_id"),
            F.col(cols[2]).cast("double").alias("score"),
        ) \
        .filter(
            F.col("source_product_id").isNotNull()
            & F.col("similar_product_id").isNotNull()
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
            "source_product_id",
            "similar_product_id",
            "score",
        )
        row_count = versioned.count()
        if row_count <= 0:
            raise RuntimeError("Item-based is a required component and cannot be empty")
        prepare_component_retry_spark(spark, generation_id, ("item_based",))
        export_dataframe_to_postgres(
            versioned,
            plan.target_table,
            POSTGRES_URL,
            plan.write_mode,
        )
        record_component_complete_spark(
            spark,
            generation_id,
            "item_based",
            row_count,
            {"source": "gold_db.item_similarity_matrix"},
        )

    print(f"SUCCESS: Exported gold_db.item_similarity_matrix to {plan.target_table}")


if __name__ == "__main__":
    export_item_based_recommendations()
