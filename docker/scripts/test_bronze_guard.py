#!/usr/bin/env python
import argparse
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


TARGET_BASE = "s3a://phase76-test/bronze/electronics_events_guard"
CASE_NAMES = (
    "VALID_SOURCE",
    "EMPTY_SOURCE",
    "MISSING_COLUMN",
    "FULL_SNAPSHOT_RERUN",
)

# Contract derived from transform_bronze_to_silver.py:
# - directly consumed there: event_time, event_type, product_id, user_id,
#   category_id, category_code, brand
# - carried forward by main.* and required by the existing Gold script:
#   price, user_session
REQUIRED_BRONZE_COLUMNS = sorted(
    {
        "brand",
        "category_code",
        "category_id",
        "event_time",
        "event_type",
        "price",
        "product_id",
        "user_id",
        "user_session",
    }
)

SOURCE_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),
    ]
)

EXPECTED_BRONZE_DTYPES = {
    "brand": "string",
    "category_code": "string",
    "category_id": "string",
    "event_time": "string",
    "event_type": "string",
    "execution_date": "string",
    "ingestion_at": "timestamp",
    "price": "double",
    "product_id": "string",
    "user_id": "string",
    "user_session": "string",
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Phase76BronzeGuardValidation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def new_run_target_path() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{TARGET_BASE}/run_{timestamp}_{uuid.uuid4().hex[:12]}"


def case_path(run_target_path: str, case_name: str) -> str:
    return f"{run_target_path}/{case_name}"


def validate_full_snapshot_source(df, source_name: str = "bronze_source") -> int:
    missing_columns = sorted(set(REQUIRED_BRONZE_COLUMNS) - set(df.columns))
    if missing_columns:
        raise RuntimeError(
            f"{source_name} is missing required columns: {', '.join(missing_columns)}"
        )

    row_count = df.count()
    print(f"VALIDATION {source_name}: row_count={row_count}")
    if row_count == 0:
        raise RuntimeError(f"{source_name} is empty; refusing to overwrite Bronze target")

    return row_count


def write_full_snapshot(df, target_path: str, execution_date: str, source_name: str) -> int:
    row_count = validate_full_snapshot_source(df, source_name)
    bronze_df = (
        df.withColumn("ingestion_at", F.current_timestamp())
        .withColumn("execution_date", F.lit(execution_date))
    )
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("execution_date")
        .save(target_path)
    )
    return row_count


def make_source_df(spark: SparkSession, rows: list[dict]):
    return spark.createDataFrame(rows, schema=SOURCE_SCHEMA)


def rows_for(prefix: str, count: int) -> list[dict]:
    event_types = ["view", "cart", "purchase"]
    return [
        {
            "event_time": f"2026-01-{idx + 1:02d} 00:00:00 UTC",
            "event_type": event_types[idx % len(event_types)],
            "product_id": f"{prefix}-product-{idx + 1}",
            "category_id": f"{prefix}-category-{idx + 1}",
            "category_code": f"electronics.{prefix}.item{idx + 1}",
            "brand": f"{prefix}-brand",
            "price": float(100 + idx),
            "user_id": f"{prefix}-user-{idx + 1}",
            "user_session": f"{prefix}-session-{idx + 1}",
        }
        for idx in range(count)
    ]


def sentinel_row() -> dict:
    return {
        "event_time": "2026-01-01 00:00:00 UTC",
        "event_type": "view",
        "product_id": "sentinel-product",
        "category_id": "sentinel-category",
        "category_code": "electronics.sentinel",
        "brand": "sentinel-brand",
        "price": 1.0,
        "user_id": "sentinel-user",
        "user_session": "sentinel-session",
    }


def assert_equal(label: str, actual, expected) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def assert_schema(df) -> None:
    actual_dtypes = dict(df.dtypes)
    assert_equal("schema dtypes", actual_dtypes, EXPECTED_BRONZE_DTYPES)


def assert_target_products(spark: SparkSession, target_path: str, expected_product_ids: set[str]) -> None:
    read_back = spark.read.format("delta").load(target_path)
    actual_count = read_back.count()
    actual_product_ids = {row["product_id"] for row in read_back.select("product_id").collect()}
    assert_equal("target row count", actual_count, len(expected_product_ids))
    assert_equal("target product ids", actual_product_ids, expected_product_ids)
    assert_schema(read_back)


def seed_sentinel_target(spark: SparkSession, target_path: str) -> None:
    sentinel_df = make_source_df(spark, [sentinel_row()])
    write_full_snapshot(
        sentinel_df,
        target_path=target_path,
        execution_date="sentinel",
        source_name="sentinel_seed",
    )
    assert_target_products(spark, target_path, {"sentinel-product"})


def expect_runtime_error(func, expected_message_part: str) -> None:
    try:
        func()
    except RuntimeError as exc:
        message = str(exc)
        if expected_message_part not in message:
            raise AssertionError(
                f"RuntimeError message did not contain {expected_message_part!r}: {message}"
            ) from exc
        print(f"EXPECTED RuntimeError: {message}")
        return
    raise AssertionError("Expected RuntimeError was not raised")


def test_valid_source(spark: SparkSession, run_target_path: str) -> None:
    target_path = case_path(run_target_path, "VALID_SOURCE")
    source_df = make_source_df(spark, rows_for("valid", 3))
    validated_count = write_full_snapshot(
        source_df,
        target_path=target_path,
        execution_date="2026-01-01",
        source_name="VALID_SOURCE",
    )
    assert_equal("validated row count", validated_count, 3)
    assert_target_products(
        spark,
        target_path,
        {"valid-product-1", "valid-product-2", "valid-product-3"},
    )


def test_empty_source(spark: SparkSession, run_target_path: str) -> None:
    target_path = case_path(run_target_path, "EMPTY_SOURCE")
    seed_sentinel_target(spark, target_path)
    empty_df = make_source_df(spark, [])
    expect_runtime_error(
        lambda: write_full_snapshot(
            empty_df,
            target_path=target_path,
            execution_date="2026-01-02",
            source_name="EMPTY_SOURCE",
        ),
        "is empty; refusing to overwrite",
    )
    assert_target_products(spark, target_path, {"sentinel-product"})


def test_missing_column(spark: SparkSession, run_target_path: str) -> None:
    target_path = case_path(run_target_path, "MISSING_COLUMN")
    seed_sentinel_target(spark, target_path)
    missing_column_df = make_source_df(spark, rows_for("missing", 2)).drop("brand")
    expect_runtime_error(
        lambda: write_full_snapshot(
            missing_column_df,
            target_path=target_path,
            execution_date="2026-01-03",
            source_name="MISSING_COLUMN",
        ),
        "missing required columns: brand",
    )
    assert_target_products(spark, target_path, {"sentinel-product"})


def test_full_snapshot_rerun(spark: SparkSession, run_target_path: str) -> None:
    target_path = case_path(run_target_path, "FULL_SNAPSHOT_RERUN")
    snapshot_a = make_source_df(spark, rows_for("snapshot-a", 2))
    snapshot_b = make_source_df(spark, rows_for("snapshot-b", 1))

    write_full_snapshot(
        snapshot_a,
        target_path=target_path,
        execution_date="2026-01-04",
        source_name="FULL_SNAPSHOT_RERUN_A",
    )
    assert_target_products(
        spark,
        target_path,
        {"snapshot-a-product-1", "snapshot-a-product-2"},
    )

    write_full_snapshot(
        snapshot_b,
        target_path=target_path,
        execution_date="2026-01-05",
        source_name="FULL_SNAPSHOT_RERUN_B",
    )
    assert_target_products(spark, target_path, {"snapshot-b-product-1"})


def run_case(case_name: str, case_func, spark: SparkSession, run_target_path: str) -> bool:
    try:
        case_func(spark, run_target_path)
    except Exception as exc:
        print(f"FAIL {case_name}: {type(exc).__name__}: {exc}")
        return False
    print(f"PASS {case_name}")
    return True


def verify_target(spark: SparkSession, target_path: str) -> None:
    require_target_under_base(target_path)
    print(f"READ_ONLY_VERIFY_TARGET={target_path}")
    for case_name in CASE_NAMES:
        path = case_path(target_path, case_name)
        df = spark.read.format("delta").load(path)
        products = sorted(row["product_id"] for row in df.select("product_id").collect())
        print(
            f"VERIFY {case_name}: row_count={df.count()} "
            f"columns={sorted(df.columns)} product_ids={products}"
        )


def cleanup_target(spark: SparkSession, target_path: str) -> None:
    require_target_under_base(target_path)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = spark.sparkContext._jvm.java.net.URI(target_path)
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(target_path)
    deleted = fs.delete(path, True)
    print(f"CLEANUP_TARGET={target_path}")
    print(f"CLEANUP_DELETED={deleted}")


def require_target_under_base(target_path: str) -> None:
    required_prefix = TARGET_BASE + "/"
    if not target_path.startswith(required_prefix):
        raise RuntimeError(
            f"Refusing target outside test base. target={target_path!r}, "
            f"required_prefix={required_prefix!r}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 76-C1 Bronze guard validation proof-of-concept"
    )
    parser.add_argument(
        "--verify-target",
        help="Read-only verification of a previously printed run target path.",
    )
    parser.add_argument(
        "--cleanup-target",
        help="Explicitly delete a previously printed run target path under the test base.",
    )
    parser.add_argument(
        "--confirm-cleanup",
        action="store_true",
        help="Required with --cleanup-target to avoid accidental deletion.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.cleanup_target and not args.confirm_cleanup:
        print("FAIL CLEANUP: --cleanup-target requires --confirm-cleanup")
        return 2

    spark = None
    try:
        spark = build_spark_session()

        if args.verify_target:
            verify_target(spark, args.verify_target)
            return 0

        if args.cleanup_target:
            cleanup_target(spark, args.cleanup_target)
            return 0

        run_target_path = new_run_target_path()
        print(f"PHASE76_TARGET_BASE={TARGET_BASE}")
        print(f"PHASE76_TARGET_PATH={run_target_path}")
        print("PHASE76_AUTO_CLEANUP=false")

        results = [
            run_case("VALID_SOURCE", test_valid_source, spark, run_target_path),
            run_case("EMPTY_SOURCE", test_empty_source, spark, run_target_path),
            run_case("MISSING_COLUMN", test_missing_column, spark, run_target_path),
            run_case("FULL_SNAPSHOT_RERUN", test_full_snapshot_rerun, spark, run_target_path),
        ]

        if all(results):
            print("PHASE76_BRONZE_GUARD_RESULT=PASS")
            print(f"INSPECT_TARGET_BEFORE_CLEANUP={run_target_path}")
            return 0

        print("PHASE76_BRONZE_GUARD_RESULT=FAIL")
        print(f"INSPECT_TARGET_BEFORE_CLEANUP={run_target_path}")
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
