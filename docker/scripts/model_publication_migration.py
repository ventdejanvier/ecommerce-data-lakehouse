from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable

from database_identity import verify_environment_identities
from model_publication_schema import (
    EXPECTED_COLUMNS,
    EXPECTED_INDEX_FRAGMENTS,
    GUARD_FUNCTION,
    GUARD_TRIGGER,
    MIGRATION_ID,
    VERSION_TABLES,
    CatalogSnapshot,
    evaluate_catalog,
)


DEFAULT_MIGRATION_PATH = Path(
    os.getenv(
        "MODEL_PUBLICATION_MIGRATION_FILE",
        "/opt/model-publication/migrations/001_atomic_model_publication_v2.sql",
    )
)


class MigrationSafetyError(RuntimeError):
    pass


def migration_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _query_all(connection, sql: str, parameters: tuple[Any, ...] = ()) -> list[tuple]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql, parameters)
        return list(cursor.fetchall())
    finally:
        cursor.close()


def _query_one(connection, sql: str, parameters: tuple[Any, ...] = ()) -> tuple:
    rows = _query_all(connection, sql, parameters)
    if len(rows) != 1:
        raise MigrationSafetyError("Catalog query did not return exactly one row")
    return rows[0]


def _existing_tables(connection) -> dict[str, str]:
    rows = _query_all(
        connection,
        """
        SELECT c.relname, pg_catalog.pg_get_userbyid(c.relowner)
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relname = ANY(%s)
        """,
        (list(EXPECTED_COLUMNS),),
    )
    return {str(table): str(owner) for table, owner in rows}


def collect_catalog_snapshot(connection) -> CatalogSnapshot:
    (
        version_num,
        current_user,
        schema_usage,
        schema_create,
        database_connect,
        database_temp,
    ) = _query_one(
        connection,
        """
        SELECT current_setting('server_version_num')::INTEGER,
               CURRENT_USER,
               has_schema_privilege(CURRENT_USER, 'public', 'USAGE'),
               has_schema_privilege(CURRENT_USER, 'public', 'CREATE'),
               has_database_privilege(CURRENT_USER, current_database(), 'CONNECT'),
               has_database_privilege(CURRENT_USER, current_database(), 'TEMP')
        """,
    )
    tables = _existing_tables(connection)
    columns: dict[str, dict[str, tuple[str, bool]]] = {}
    constraints: dict[str, list[tuple[str, str]]] = {}
    indexes: dict[str, str] = {}
    triggers: dict[str, str] = {}

    if tables:
        for table, column, data_type, nullable in _query_all(
            connection,
            """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            ORDER BY table_name, ordinal_position
            """,
            (list(tables),),
        ):
            columns.setdefault(str(table), {})[str(column)] = (
                str(data_type),
                str(nullable) == "YES",
            )

        for table, constraint_type, definition in _query_all(
            connection,
            """
            SELECT rel.relname, con.contype, pg_catalog.pg_get_constraintdef(con.oid, true)
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = rel.relnamespace
            WHERE n.nspname = 'public'
              AND rel.relname = ANY(%s)
            """,
            (list(tables),),
        ):
            constraints.setdefault(str(table), []).append(
                (str(constraint_type), str(definition))
            )

    for index_name, definition in _query_all(
        connection,
        """
        SELECT indexname, indexdef
        FROM pg_catalog.pg_indexes
        WHERE schemaname = 'public'
          AND indexname = ANY(%s)
        """,
        (list(EXPECTED_INDEX_FRAGMENTS),),
    ):
        indexes[str(index_name)] = str(definition)

    for table, trigger_name, definition in _query_all(
        connection,
        """
        SELECT c.relname, t.tgname, pg_catalog.pg_get_triggerdef(t.oid, true)
        FROM pg_catalog.pg_trigger t
        JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = ANY(%s)
          AND t.tgname = %s
          AND NOT t.tgisinternal
        """,
        (list(VERSION_TABLES), GUARD_TRIGGER),
    ):
        triggers[f"public.{table}.{trigger_name}"] = str(definition)

    function_rows = _query_all(
        connection,
        """
        SELECT pg_catalog.pg_get_functiondef(p.oid)
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname = %s
          AND pg_catalog.pg_get_function_identity_arguments(p.oid) = ''
        """,
        (GUARD_FUNCTION.split(".", 1)[1],),
    )
    function_definition = str(function_rows[0][0]) if len(function_rows) == 1 else None

    lifecycle_statuses: set[str] = set()
    duplicate_active_count = 0
    duplicate_airflow_run_id_count = 0
    pointer_row_count = 0
    pointer_status_consistent = True
    if "recommendation_generations" in tables:
        lifecycle_statuses = {
            str(row[0])
            for row in _query_all(
                connection,
                "SELECT DISTINCT status FROM public.recommendation_generations",
            )
        }
        active_count = int(
            _query_one(
                connection,
                "SELECT COUNT(*) FROM public.recommendation_generations WHERE status = 'ACTIVE'",
            )[0]
        )
        duplicate_active_count = max(0, active_count - 1)
        duplicate_airflow_run_id_count = int(
            _query_one(
                connection,
                """
                SELECT COUNT(*) FROM (
                    SELECT airflow_run_id
                    FROM public.recommendation_generations
                    WHERE airflow_run_id IS NOT NULL
                    GROUP BY airflow_run_id
                    HAVING COUNT(*) > 1
                ) duplicates
                """,
            )[0]
        )

    if "active_recommendation_generation" in tables:
        pointer_row_count = int(
            _query_one(
                connection,
                "SELECT COUNT(*) FROM public.active_recommendation_generation",
            )[0]
        )
    if {
        "recommendation_generations",
        "active_recommendation_generation",
    }.issubset(tables):
        pointer_status_consistent = bool(
            _query_one(
                connection,
                """
                WITH active AS (
                    SELECT generation_id
                    FROM public.recommendation_generations
                    WHERE status = 'ACTIVE'
                ), pointer AS (
                    SELECT generation_id
                    FROM public.active_recommendation_generation
                    WHERE singleton_key = 1
                )
                SELECT CASE
                    WHEN (SELECT COUNT(*) FROM active) = 0
                     AND (SELECT COUNT(*) FROM pointer) = 1
                     AND (SELECT generation_id FROM pointer) IS NULL THEN TRUE
                    WHEN (SELECT COUNT(*) FROM active) = 1
                     AND (SELECT COUNT(*) FROM pointer) = 1
                     AND (SELECT generation_id FROM pointer) =
                         (SELECT generation_id FROM active) THEN TRUE
                    ELSE FALSE
                END
                """,
            )[0]
        )

    return CatalogSnapshot(
        postgres_version_num=int(version_num),
        current_user=str(current_user),
        permissions={
            "database_connect": bool(database_connect),
            "database_temp": bool(database_temp),
            "schema_create": bool(schema_create),
            "schema_usage": bool(schema_usage),
        },
        tables=tables,
        columns=columns,
        constraints=constraints,
        indexes=indexes,
        triggers=triggers,
        function_definition=function_definition,
        lifecycle_statuses=lifecycle_statuses,
        duplicate_active_count=duplicate_active_count,
        duplicate_airflow_run_id_count=duplicate_airflow_run_id_count,
        pointer_row_count=pointer_row_count,
        pointer_status_consistent=pointer_status_consistent,
    )


def run_catalog_check(connection, *, require_complete: bool) -> dict[str, Any]:
    report = evaluate_catalog(
        collect_catalog_snapshot(connection),
        require_complete=require_complete,
    )
    if not report["valid"]:
        raise MigrationSafetyError(json.dumps(report, sort_keys=True))
    return report


def _tracking_record(connection, migration_id: str) -> tuple[str] | None:
    table_exists = bool(
        _query_one(
            connection,
            "SELECT pg_catalog.to_regclass('public.model_publication_schema_migrations') IS NOT NULL",
        )[0]
    )
    if not table_exists:
        return None
    rows = _query_all(
        connection,
        """
        SELECT checksum
        FROM public.model_publication_schema_migrations
        WHERE migration_id = %s
        """,
        (migration_id,),
    )
    if len(rows) > 1:
        raise MigrationSafetyError("Migration tracking contains duplicate identifiers")
    return (str(rows[0][0]),) if rows else None


def _set_local_timeouts(connection) -> None:
    lock_timeout = os.getenv("MODEL_PUBLICATION_LOCK_TIMEOUT_MS", "30000")
    statement_timeout = os.getenv("MODEL_PUBLICATION_STATEMENT_TIMEOUT_MS", "120000")
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT set_config('lock_timeout', %s, true),
                   set_config('statement_timeout', %s, true)
            """,
            (f"{int(lock_timeout)}ms", f"{int(statement_timeout)}ms"),
        )
    finally:
        cursor.close()


def apply_migration(connection, migration_path: Path) -> dict[str, Any]:
    checksum = migration_checksum(migration_path)
    preflight = run_catalog_check(connection, require_complete=False)
    connection.rollback()

    try:
        _set_local_timeouts(connection)
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext(%s))",
                (MIGRATION_ID,),
            )
        finally:
            cursor.close()

        existing = _tracking_record(connection, MIGRATION_ID)
        if existing is not None:
            if existing[0] != checksum:
                raise MigrationSafetyError(
                    "Migration checksum mismatch for an already applied identifier"
                )
            connection.rollback()
            connection.set_session(readonly=True, autocommit=False)
            verification = run_catalog_check(connection, require_complete=True)
            connection.rollback()
            return {
                "migration_id": MIGRATION_ID,
                "checksum": checksum,
                "status": "ALREADY_APPLIED",
                "preflight": preflight,
                "verification": verification,
            }

        migration_sql = migration_path.read_text(encoding="utf-8")
        cursor = connection.cursor()
        try:
            cursor.execute(migration_sql)
            cursor.execute(
                """
                INSERT INTO public.model_publication_schema_migrations
                    (migration_id, checksum)
                VALUES (%s, %s)
                """,
                (MIGRATION_ID, checksum),
            )
        finally:
            cursor.close()
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    connection.set_session(readonly=True, autocommit=False)
    verification = run_catalog_check(connection, require_complete=True)
    connection.rollback()
    return {
        "migration_id": MIGRATION_ID,
        "checksum": checksum,
        "status": "APPLIED",
        "preflight": preflight,
        "verification": verification,
    }


def _open_connection():
    dsn = os.getenv("MODEL_PUBLICATION_DATABASE_DSN")
    if not dsn:
        raise MigrationSafetyError("MODEL_PUBLICATION_DATABASE_DSN is required")
    import psycopg2

    return psycopg2.connect(dsn)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual preflight/apply/verify runner for atomic model publication."
    )
    parser.add_argument("mode", choices=("preflight", "apply", "verify"))
    parser.add_argument("--migration-file", type=Path, default=DEFAULT_MIGRATION_PATH)
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    identity_report = verify_environment_identities(os.environ)
    connection = _open_connection()
    try:
        if args.mode == "apply":
            result = apply_migration(connection, args.migration_file)
        else:
            connection.set_session(readonly=True, autocommit=False)
            result = run_catalog_check(
                connection,
                require_complete=args.mode == "verify",
            )
            connection.rollback()
    finally:
        connection.close()
    print(
        json.dumps(
            {"database_identity": identity_report, "mode": args.mode, "result": result},
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
