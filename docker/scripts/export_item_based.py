import sys

from pyspark.sql import functions as F

sys.path.append("/home/jovyan/scripts")
from common_config import get_spark_session
from model_publication import (
    build_component_source_info,
    export_versioned_component_spark,
    resolve_item_v2_schema,
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
            f"{table_name} must expose at least 3 columns: source item, similar item, score. "
            f"Found columns: {cols}"
        )


def export_item_based_recommendations():
    plan = resolve_export_plan("item_based", "public.serving_item_based")
    spark = get_spark_session("Export_Item_Based_Recommendations")

    item_similarity = spark.table("gold_db.item_similarity_matrix")
    cols = item_similarity.columns

    if not plan.v2_enabled:
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
        export_dataframe_to_postgres(
            recommendations,
            plan.target_table,
        )
    else:
        generation_id = plan.generation_id
        mapping = resolve_item_v2_schema(cols)
        versioned = item_similarity.select(
            F.lit(generation_id).alias("generation_id"),
            F.col(mapping["source_product_id"])
            .cast("string")
            .alias("source_product_id"),
            F.col(mapping["similar_product_id"])
            .cast("string")
            .alias("similar_product_id"),
            F.col(mapping["score"]).cast("double").alias("score"),
        ).filter(
            F.col("source_product_id").isNotNull()
            & F.col("similar_product_id").isNotNull()
            & F.col("score").isNotNull()
        )
        export_versioned_component_spark(
            spark,
            versioned,
            generation_id,
            "item_based",
            build_component_source_info(
                source_tables=("gold_db.item_similarity_matrix",),
                source_paths=("s3a://gold/item_similarity_matrix",),
                exporter_name="export_item_based.py",
            ),
        )

    print(f"SUCCESS: Exported gold_db.item_similarity_matrix to {plan.target_table}")


if __name__ == "__main__":
    export_item_based_recommendations()
