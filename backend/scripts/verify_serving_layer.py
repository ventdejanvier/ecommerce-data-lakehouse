from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

DEFAULT_DATABASE_URL = "postgresql://user:password@localhost:5434/data_lakehouse"


@dataclass(frozen=True)
class TableSpec:
    name: str
    required_by_default: bool
    columns_by_role: dict[str, tuple[str, ...]]
    required_roles: tuple[str, ...]
    score_role: str | None = None


TABLE_SPECS = [
    TableSpec(
        name="serving_user_clusters",
        required_by_default=True,
        columns_by_role={
            "user_id": ("user_id",),
            "cluster_id": ("cluster_id",),
            "cluster_label": ("cluster_label", "segment_name"),
        },
        required_roles=("user_id", "cluster_id", "cluster_label"),
    ),
    TableSpec(
        name="serving_recommendations",
        required_by_default=True,
        columns_by_role={
            "cluster_id": ("cluster_id",),
            "product_id": ("product_id",),
            "display_name": ("display_name", "product_name"),
            "score": ("score", "cluster_total_score"),
        },
        required_roles=("cluster_id", "product_id", "display_name", "score"),
        score_role="score",
    ),
    TableSpec(
        name="serving_als_recommendations",
        required_by_default=False,
        columns_by_role={
            "user_id": ("user_id",),
            "product_id": ("product_id",),
            "score": ("score", "prediction", "rating_prediction"),
        },
        required_roles=("user_id", "product_id", "score"),
        score_role="score",
    ),
    TableSpec(
        name="serving_content_recommendations",
        required_by_default=False,
        columns_by_role={
            "source_product_id": ("source_product_id", "product_id"),
            "recommended_product_id": (
                "recommended_product_id",
                "similar_product_id",
                "target_product_id",
            ),
            "score": ("score", "similarity_score", "cosine_similarity"),
        },
        required_roles=("source_product_id", "recommended_product_id", "score"),
        score_role="score",
    ),
    TableSpec(
        name="serving_item_similarities",
        required_by_default=False,
        columns_by_role={
            "source_product_id": ("source_product_id", "product_id"),
            "similar_product_id": (
                "similar_product_id",
                "recommended_product_id",
                "target_product_id",
            ),
            "score": ("score", "similarity_score", "cosine_similarity"),
        },
        required_roles=("source_product_id", "similar_product_id", "score"),
        score_role="score",
    ),
]


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def table_ref(table_name: str) -> str:
    return f"public.{quote_identifier(table_name)}"


def serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def redact_url(url: str) -> str:
    if "@" not in url or "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    _, host = rest.rsplit("@", 1)
    return f"{scheme}://***:***@{host}"


def resolve_columns(
    spec: TableSpec,
    existing_columns: set[str],
) -> tuple[dict[str, str], list[str]]:
    resolved = {}
    missing = []

    for role, candidates in spec.columns_by_role.items():
        column = next((candidate for candidate in candidates if candidate in existing_columns), None)
        if column:
            resolved[role] = column
        elif role in spec.required_roles:
            missing.append(f"{role} ({', '.join(candidates)})")

    return resolved, missing


def scalar_int(connection: Connection, sql: str) -> int:
    value = connection.execute(text(sql)).scalar_one()
    return int(value or 0)


def validate_table(
    connection: Connection,
    spec: TableSpec,
    existing_columns: set[str],
) -> list[str]:
    errors = []
    resolved_columns, missing_columns = resolve_columns(spec, existing_columns)

    if missing_columns:
        errors.append(f"missing required column role(s): {', '.join(missing_columns)}")
        return errors

    row_count = scalar_int(connection, f"SELECT COUNT(*) FROM {table_ref(spec.name)}")
    if row_count == 0:
        errors.append("table is empty")

    for role in spec.required_roles:
        column = resolved_columns[role]
        invalid_count = scalar_int(
            connection,
            (
                f"SELECT COUNT(*) FROM {table_ref(spec.name)} "
                f"WHERE {quote_identifier(column)} IS NULL"
            ),
        )
        if invalid_count:
            errors.append(f"{role}/{column} has {invalid_count} NULL value(s)")

    if spec.score_role:
        score_column = resolved_columns[spec.score_role]
        invalid_score_count = scalar_int(
            connection,
            (
                f"SELECT COUNT(*) FROM {table_ref(spec.name)} "
                f"WHERE {quote_identifier(score_column)} IS NULL "
                f"OR {quote_identifier(score_column)} < 0"
            ),
        )
        if invalid_score_count:
            errors.append(
                f"{spec.score_role}/{score_column} has {invalid_score_count} NULL or negative value(s)"
            )

    return errors


def print_sample_rows(connection: Connection, table_name: str, limit: int) -> None:
    rows = connection.execute(
        text(f"SELECT * FROM {table_ref(table_name)} LIMIT :limit"),
        {"limit": limit},
    ).mappings()
    for row in rows:
        sample = {key: serialize_value(value) for key, value in row.items()}
        print(f"    sample: {sample}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Postgres serving tables exported by Spark ML jobs."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL),
        help="Postgres SQLAlchemy URL. Defaults to DATABASE_URL or local Docker port 5434.",
    )
    parser.add_argument(
        "--strict-multi-engine",
        action="store_true",
        help="Fail if ALS/content-based/item-based serving tables are missing.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=3,
        help="Rows to print from each existing serving table.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    engine = create_engine(args.database_url, pool_pre_ping=True)

    print(f"Connecting to {redact_url(args.database_url)}")

    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            table_names = set(inspector.get_table_names(schema="public"))
            serving_tables = sorted(name for name in table_names if name.startswith("serving_"))

            print(f"Serving tables discovered: {serving_tables or 'none'}")

            failures = []
            for spec in TABLE_SPECS:
                required = spec.required_by_default or args.strict_multi_engine
                if spec.name not in table_names:
                    level = "FAIL" if required else "WARN"
                    print(f"[{level}] {spec.name}: table is missing")
                    if required:
                        failures.append(f"{spec.name}: table is missing")
                    continue

                columns = {column["name"] for column in inspector.get_columns(spec.name, schema="public")}
                row_count = scalar_int(connection, f"SELECT COUNT(*) FROM {table_ref(spec.name)}")
                errors = validate_table(connection, spec, columns)

                if errors:
                    print(f"[FAIL] {spec.name}: {row_count} row(s)")
                    for error in errors:
                        print(f"    - {error}")
                    failures.extend(f"{spec.name}: {error}" for error in errors)
                else:
                    print(f"[OK] {spec.name}: {row_count} row(s)")
                    print_sample_rows(connection, spec.name, args.sample_limit)

            if failures:
                print("\nServing layer validation failed:")
                for failure in failures:
                    print(f"  - {failure}")
                return 1

            print("\nServing layer validation passed.")
            return 0
    except SQLAlchemyError as exc:
        print(f"Database connection/query failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
