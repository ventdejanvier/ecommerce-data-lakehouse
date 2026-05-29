import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import BucketedRandomProjectionLSH, HashingTF, IDF, Normalizer, RegexTokenizer
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
        .limit(5000) \
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
        HashingTF(inputCol="tokens", outputCol="raw_features", numFeatures=32),
        IDF(inputCol="raw_features", outputCol="tfidf_features"),
        Normalizer(inputCol="tfidf_features", outputCol="features", p=2.0),
    ])

    featured_products = pipeline.fit(products).transform(products) \
        .select(
            "product_id",
            "display_name",
            "brand",
            "category_main",
            "category_sub",
            "features",
        )

    lsh = BucketedRandomProjectionLSH(
        inputCol="features",
        outputCol="hashes",
        bucketLength=1.0,
        numHashTables=1,
    )
    lsh_model = lsh.fit(featured_products)

    similar_pairs = lsh_model.approxSimilarityJoin(
        featured_products,
        featured_products,
        2.0,
        distCol="distance",
    ) \
        .filter(F.col("datasetA.product_id") != F.col("datasetB.product_id")) \
        .withColumn("score", 1.0 / (1.0 + F.col("distance")))

    rank_window = Window.partitionBy(F.col("datasetA.product_id")) \
        .orderBy(F.desc("score"), F.asc(F.col("datasetB.product_id")))

    recommendations = similar_pairs \
        .withColumn("rank", F.row_number().over(rank_window)) \
        .filter(F.col("rank") <= 10) \
        .select(
            F.col("datasetA.product_id").alias("source_product_id"),
            F.col("datasetB.product_id").alias("recommended_product_id"),
            F.col("datasetA.display_name").alias("source_display_name"),
            F.col("datasetB.display_name").alias("recommended_display_name"),
            F.round("score", 6).alias("score"),
            "rank",
        ) \
        .orderBy("source_product_id", "rank")

    export_to_postgres(recommendations, "public.serving_content_based")

    print("SUCCESS: Exported content-based recommendations to public.serving_content_based")


if __name__ == "__main__":
    export_content_based_recommendations()
