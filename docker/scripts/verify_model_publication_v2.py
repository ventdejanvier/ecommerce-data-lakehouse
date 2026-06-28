from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from typing import Any, Iterable

from database_identity import verify_environment_identities
from model_publication import COMPONENT_SPECS, REQUIRED_COMPONENTS, validate_generation_id
from model_publication_migration import MigrationSafetyError, _query_all, _query_one, run_catalog_check


LEGACY_SERVING_TABLES = (
    "serving_user_clusters",
    "serving_recommendations",
    "serving_als",
    "serving_content_based",
    "serving_item_based",
)


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _resolve_generation(connection, explicit_generation_id: str | None) -> tuple[str, str]:
    if explicit_generation_id is not None:
        generation_id = validate_generation_id(explicit_generation_id)
        rows = _query_all(
            connection,
            """
            SELECT generation_id, status
            FROM public.recommendation_generations
            WHERE generation_id = %s
            """,
            (generation_id,),
        )
        if len(rows) != 1 or str(rows[0][1]) != "READY":
            raise MigrationSafetyError(
                "Explicit verification target must be exactly one READY generation"
            )
        return generation_id, "READY"

    rows = _query_all(
        connection,
        """
        SELECT p.generation_id, g.status
        FROM public.active_recommendation_generation p
        LEFT JOIN public.recommendation_generations g
          ON g.generation_id = p.generation_id
        WHERE p.singleton_key = 1
        """,
    )
    if len(rows) != 1 or not rows[0][0] or str(rows[0][1]) != "ACTIVE":
        raise MigrationSafetyError(
            "Active pointer must reference exactly one ACTIVE generation"
        )
    return validate_generation_id(rows[0][0]), "ACTIVE"


def verify_serving_generation(
    connection,
    *,
    generation_id: str | None = None,
) -> dict[str, Any]:
    schema_report = run_catalog_check(connection, require_complete=True)
    candidate, expected_status = _resolve_generation(connection, generation_id)
    errors: list[str] = []

    generation_row = _query_one(
        connection,
        """
        SELECT status, validation_report
        FROM public.recommendation_generations
        WHERE generation_id = %s
        """,
        (candidate,),
    )
    if str(generation_row[0]) != expected_status:
        errors.append(
            f"generation status is {generation_row[0]}; expected {expected_status}"
        )
    try:
        validation_report = _decode_json(generation_row[1])
    except (TypeError, ValueError, json.JSONDecodeError):
        validation_report = None
    if not isinstance(validation_report, Mapping) or validation_report.get("valid") is not True:
        errors.append("generation validation_report is missing or valid is not true")

    component_rows = _query_all(
        connection,
        """
        SELECT component_name, row_count, completion_status, source_info
        FROM public.recommendation_generation_components
        WHERE generation_id = %s
        """,
        (candidate,),
    )
    records = {
        str(name): {
            "recorded_row_count": int(row_count),
            "completion_status": str(status),
            "source_info": _decode_json(source_info),
        }
        for name, row_count, status, source_info in component_rows
    }
    components: dict[str, Any] = {}
    for component_name in REQUIRED_COMPONENTS:
        spec = COMPONENT_SPECS[component_name]
        record = records.get(component_name)
        actual_count = int(
            _query_one(
                connection,
                f"SELECT COUNT(*) FROM {spec.version_table} WHERE generation_id = %s",
                (candidate,),
            )[0]
        )
        invalid_count = int(
            _query_one(
                connection,
                (
                    f"SELECT COUNT(*) FROM {spec.version_table} "
                    f"WHERE generation_id = %s AND ({spec.invalid_row_predicate})"
                ),
                (candidate,),
            )[0]
        )
        key_list = ", ".join(spec.logical_key_columns)
        duplicate_count = int(
            _query_one(
                connection,
                (
                    "SELECT COUNT(*) FROM ("
                    f"SELECT {key_list} FROM {spec.version_table} "
                    "WHERE generation_id = %s "
                    f"GROUP BY {key_list} HAVING COUNT(*) > 1"
                    ") duplicate_keys"
                ),
                (candidate,),
            )[0]
        )
        components[component_name] = {
            "actual_row_count": actual_count,
            "duplicate_key_count": duplicate_count,
            "invalid_row_count": invalid_count,
            **(record or {}),
        }
        if record is None:
            errors.append(f"{component_name}: component record is missing")
        else:
            if record["completion_status"] != "COMPLETE":
                errors.append(f"{component_name}: completion status is not COMPLETE")
            if record["recorded_row_count"] != actual_count:
                errors.append(f"{component_name}: recorded and actual counts differ")
        if actual_count <= 0:
            errors.append(f"{component_name}: actual row count is not positive")
        if invalid_count:
            errors.append(f"{component_name}: invalid rows exist")
        if duplicate_count:
            errors.append(f"{component_name}: duplicate logical keys exist")

    missing_legacy = [
        table
        for table in LEGACY_SERVING_TABLES
        if not bool(
            _query_one(
                connection,
                "SELECT pg_catalog.to_regclass(%s) IS NOT NULL",
                (f"public.{table}",),
            )[0]
        )
    ]
    if missing_legacy:
        errors.append(f"legacy serving tables are missing: {', '.join(missing_legacy)}")

    return {
        "valid": not errors,
        "generation_id": candidate,
        "expected_status": expected_status,
        "schema": schema_report,
        "components": components,
        "errors": errors,
    }


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only V2 serving verifier")
    parser.add_argument(
        "--generation-id",
        help="Explicit READY candidate. Omit to verify the ACTIVE pointer target.",
    )
    return parser.parse_args(argv)


def _open_connection():
    dsn = os.getenv("MODEL_PUBLICATION_DATABASE_DSN")
    if not dsn:
        raise MigrationSafetyError("MODEL_PUBLICATION_DATABASE_DSN is required")
    import psycopg2

    return psycopg2.connect(dsn)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    generation_id = (
        validate_generation_id(args.generation_id) if args.generation_id else None
    )
    identity_report = verify_environment_identities(os.environ)
    connection = _open_connection()
    try:
        connection.set_session(readonly=True, autocommit=False)
        report = verify_serving_generation(
            connection,
            generation_id=generation_id,
        )
        connection.rollback()
    finally:
        connection.close()
    print(
        json.dumps(
            {"database_identity": identity_report, **report},
            sort_keys=True,
        )
    )
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
