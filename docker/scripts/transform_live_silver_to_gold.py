import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

sys.path.append("/home/jovyan/scripts")
from common_config import get_spark_session


LIVE_SILVER_INPUT = os.getenv(
    "LIVE_SILVER_INPUT",
    "s3a://silver/tracking_events/",
)
LIVE_GOLD_OUTPUT = os.getenv(
    "LIVE_GOLD_OUTPUT",
    "s3a://gold/fact_live_events/",
)
LIVE_GOLD_TABLE = os.getenv(
    "LIVE_GOLD_TABLE",
    "gold_db.fact_live_events",
)
ALLOW_NOOP = os.getenv("LIVE_GOLD_ALLOW_NOOP", "false").lower() == "true"

REQUIRED_COLS = {
    "event_id",
    "event_time",
    "event_type",
    "product_id",
    "user_id",
    "session_id",
    "product_price",
    "raw_json",
}
CANONICAL_COLS = (
    "event_id",
    "event_time",
    "event_type",
    "product_id",
    "user_id",
    "session_id",
    "product_price",
    "action",
)


def _validate_table_name(table_name):
    parts = table_name.split(".")
    if len(parts) not in (1, 2):
        raise RuntimeError(
            "LIVE_GOLD_TABLE must be an unqualified table name or database.table"
        )

    for part in parts:
        if (
            not part
            or part[0].isdigit()
            or not part.replace("_", "").isalnum()
        ):
            raise RuntimeError(
                "LIVE_GOLD_TABLE may contain only letters, numbers, and underscores, "
                "and each identifier must not start with a number"
            )

    return parts


def _handle_unavailable_source(error=None):
    if ALLOW_NOOP:
        print(
            "[NOOP MODE] Source empty or missing. Exiting cleanly without "
            "overwriting Gold."
        )
        sys.exit(0)

    raise RuntimeError(
        "Live Silver source is missing or contains 0 rows while "
        "LIVE_GOLD_ALLOW_NOOP=false."
    ) from error


def _is_missing_source_error(error):
    error_message = str(error).lower()
    return (
        "path does not exist" in error_message
        or "[path_not_found]" in error_message
    )


def transform_live_gold():
    spark = get_spark_session("Live_Gold_Layer")

    try:
        print(f"Loading live Silver events from: {LIVE_SILVER_INPUT}")
        try:
            df_live = spark.read.format("delta").load(LIVE_SILVER_INPUT)
        except AnalysisException as error:
            if not _is_missing_source_error(error):
                raise
            _handle_unavailable_source(error)

        required_cols = REQUIRED_COLS
        missing = required_cols - set(df_live.columns)
        if missing:
            raise RuntimeError(
                f"Schema Contract Violation! Live Silver missing columns: {missing}"
            )

        input_row_count = df_live.count()
        if input_row_count == 0:
            _handle_unavailable_source()

        df_with_action = df_live.withColumn(
            "action",
            F.lower(F.get_json_object(F.col("raw_json"), "$.action"))
        )

        deduped_df = df_with_action.select(
            *(F.col(column_name) for column_name in CANONICAL_COLS)
        ).dropDuplicates(["event_id"])
        deduplicated_row_count = deduped_df.count()
        duplicate_count = input_row_count - deduplicated_row_count

        print(
            f"Observability Metrics => Input Rows: {input_row_count} | "
            f"Deduped Rows: {deduplicated_row_count} | "
            f"Dropped Duplicates: {duplicate_count}"
        )

        table_parts = _validate_table_name(LIVE_GOLD_TABLE)
        if len(table_parts) == 2:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS `{table_parts[0]}`")

        print(
            f"Writing fact_live_events to {LIVE_GOLD_OUTPUT} "
            f"and registering {LIVE_GOLD_TABLE}"
        )
        deduped_df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("path", LIVE_GOLD_OUTPUT) \
            .saveAsTable(LIVE_GOLD_TABLE)

        print("Live Gold transformation completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    transform_live_gold()
