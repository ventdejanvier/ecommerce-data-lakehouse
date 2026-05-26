import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Normalizer, RegexTokenizer
from pyspark.ml.functions import vector_to_array
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


def export_content_based_recommendations():
    spark = get_spark_session("Export_Content_Based_Recommendations")

    products = spark.table("gold_db.dim_products") \
        .select(
            "product_id",
            "display_name",
            "brand",
            "avg_price",
            "category_main",
            "category_sub",
            "category_detail",
        ) \
        .filter(F.col("product_id").isNotNull()) \
        .dropDuplicates(["product_id"]) \
        .withColumn(
            "price_bucket",
            F.concat(
                F.lit("price_bucket_"),
                (F.floor(F.coalesce(F.col("avg_price"), F.lit(0.0)) / F.lit(50.0)) * 50).cast("string"),
            ),
        ) \
        .withColumn(
            "feature_text",
            F.concat_ws(
                " ",
                F.coalesce(F.col("display_name"), F.lit("")),
                F.coalesce(F.col("brand"), F.lit("")),
                F.coalesce(F.col("category_main"), F.lit("")),
                F.coalesce(F.col("category_sub"), F.lit("")),
                F.coalesce(F.col("category_detail"), F.lit("")),
                F.col("price_bucket"),
            ),
        )

    pipeline = Pipeline(stages=[
        RegexTokenizer(
            inputCol="feature_text",
            outputCol="tokens",
            pattern="\\W+",
            minTokenLength=2,
        ),
        HashingTF(inputCol="tokens", outputCol="raw_features", numFeatures=512),
        IDF(inputCol="raw_features", outputCol="tfidf_features"),
        Normalizer(inputCol="tfidf_features", outputCol="features", p=2.0),
    ])

    featured_products = pipeline.fit(products).transform(products) \
        .select(
            "product_id",
            "display_name",
            "brand",
            "category_main",
            vector_to_array("features").alias("features"),
        )

    left = featured_products.select(
        F.col("product_id").alias("source_product_id"),
        F.col("display_name").alias("source_display_name"),
        F.col("brand").alias("source_brand"),
        F.col("category_main").alias("source_category_main"),
        F.col("features").alias("source_features"),
    )
    right = featured_products.select(
        F.col("product_id").alias("recommended_product_id"),
        F.col("display_name").alias("recommended_display_name"),
        F.col("brand").alias("recommended_brand"),
        F.col("category_main").alias("recommended_category_main"),
        F.col("features").alias("recommended_features"),
    )

    candidate_condition = (
        (F.col("source_product_id") != F.col("recommended_product_id"))
        & (
            (
                F.col("source_category_main").isNotNull()
                & (F.col("source_category_main") == F.col("recommended_category_main"))
            )
            | (
                F.col("source_brand").isNotNull()
                & (F.col("source_brand") == F.col("recommended_brand"))
            )
        )
    )

    scored_pairs = left.join(right, candidate_condition) \
        .withColumn(
            "score",
            F.expr(
                """
                aggregate(
                    zip_with(source_features, recommended_features, (x, y) -> x * y),
                    cast(0.0 as double),
                    (acc, value) -> acc + value
                )
                """
            ),
        ) \
        .filter(F.col("score").isNotNull() & (F.col("score") >= 0))

    rank_window = Window.partitionBy("source_product_id") \
        .orderBy(F.desc("score"), F.asc("recommended_product_id"))

    recommendations = scored_pairs \
        .withColumn("rank", F.row_number().over(rank_window)) \
        .filter(F.col("rank") <= 10) \
        .select(
            "source_product_id",
            "recommended_product_id",
            "source_display_name",
            "recommended_display_name",
            F.round("score", 6).alias("score"),
            "rank",
        ) \
        .orderBy("source_product_id", "rank")

    export_to_postgres(recommendations, "public.serving_content_based")

    print("SUCCESS: Exported content-based recommendations to public.serving_content_based")


if __name__ == "__main__":
    export_content_based_recommendations()
