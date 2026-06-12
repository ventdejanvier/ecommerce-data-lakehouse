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


def export_als_recommendations():
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

    export_dataframe_to_postgres(
        recommendations,
        "public.serving_als",
        POSTGRES_URL,
    )

    print("SUCCESS: Exported gold_db.recommendations_als to public.serving_als")


if __name__ == "__main__":
    export_als_recommendations()
