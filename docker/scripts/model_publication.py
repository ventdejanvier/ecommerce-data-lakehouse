from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


GENERATION_ID_MAX_LENGTH = 80
GENERATION_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")

BUILDING = "BUILDING"
READY = "READY"
ACTIVE = "ACTIVE"
SUPERSEDED = "SUPERSEDED"
FAILED = "FAILED"

NORMAL_STATUS_TRANSITIONS = {
    BUILDING: frozenset({READY, FAILED}),
    READY: frozenset({ACTIVE, FAILED}),
    ACTIVE: frozenset({SUPERSEDED}),
    SUPERSEDED: frozenset(),
    FAILED: frozenset(),
}
ROLLBACK_STATUS_TRANSITIONS = {
    ACTIVE: frozenset({SUPERSEDED}),
    SUPERSEDED: frozenset({ACTIVE}),
}

DEFAULT_POSTGRES_DSN = "dbname=data_lakehouse user=user password=password host=postgres port=5432"
DEFAULT_JDBC_URL = "jdbc:postgresql://postgres:5432/data_lakehouse"
DEFAULT_POSTGRES_USER = "user"
DEFAULT_POSTGRES_PASSWORD = "password"


@dataclass(frozen=True)
class ComponentSpec:
    name: str
    version_table: str
    required_columns: tuple[str, ...]
    logical_key_columns: tuple[str, ...]
    invalid_row_predicate: str


COMPONENT_SPECS = {
    "user_clusters": ComponentSpec(
        name="user_clusters",
        version_table="public.serving_user_clusters_versions",
        required_columns=("user_id", "cluster_id", "segment_name"),
        logical_key_columns=("user_id",),
        invalid_row_predicate=(
            "user_id IS NULL OR cluster_id IS NULL OR segment_name IS NULL "
            "OR BTRIM(user_id) = '' OR BTRIM(segment_name) = ''"
        ),
    ),
    "cluster_recommendations": ComponentSpec(
        name="cluster_recommendations",
        version_table="public.serving_recommendations_versions",
        required_columns=(
            "cluster_id",
            "product_id",
            "display_name",
            "cluster_total_score",
        ),
        logical_key_columns=("cluster_id", "product_id"),
        invalid_row_predicate=(
            "cluster_id IS NULL OR product_id IS NULL OR display_name IS NULL "
            "OR cluster_total_score IS NULL OR BTRIM(product_id) = '' "
            "OR BTRIM(display_name) = '' "
            "OR cluster_total_score = 'NaN'::double precision "
            "OR cluster_total_score = 'Infinity'::double precision "
            "OR cluster_total_score = '-Infinity'::double precision"
        ),
    ),
    "als": ComponentSpec(
        name="als",
        version_table="public.serving_als_versions",
        required_columns=("user_id", "product_id", "score"),
        logical_key_columns=("user_id", "product_id"),
        invalid_row_predicate=(
            "user_id IS NULL OR product_id IS NULL OR score IS NULL "
            "OR BTRIM(user_id) = '' OR BTRIM(product_id) = '' "
            "OR score = 'NaN'::double precision "
            "OR score = 'Infinity'::double precision "
            "OR score = '-Infinity'::double precision"
        ),
    ),
    "content_based": ComponentSpec(
        name="content_based",
        version_table="public.serving_content_based_versions",
        required_columns=(
            "user_id",
            "product_id",
            "source_product_id",
            "recommended_product_id",
            "score",
        ),
        logical_key_columns=(
            "user_id",
            "product_id",
            "source_product_id",
            "recommended_product_id",
        ),
        invalid_row_predicate=(
            "score IS NULL OR NOT ("
            "(user_id IS NOT NULL AND product_id IS NOT NULL "
            "AND source_product_id IS NULL AND recommended_product_id IS NULL) OR "
            "(source_product_id IS NOT NULL AND recommended_product_id IS NOT NULL "
            "AND user_id IS NULL AND product_id IS NULL)) OR "
            "COALESCE(BTRIM(user_id), BTRIM(source_product_id), '') = '' OR "
            "COALESCE(BTRIM(product_id), BTRIM(recommended_product_id), '') = '' OR "
            "score = 'NaN'::double precision "
            "OR score = 'Infinity'::double precision "
            "OR score = '-Infinity'::double precision"
        ),
    ),
    "item_based": ComponentSpec(
        name="item_based",
        version_table="public.serving_item_based_versions",
        required_columns=("source_product_id", "similar_product_id", "score"),
        logical_key_columns=("source_product_id", "similar_product_id"),
        invalid_row_predicate=(
            "source_product_id IS NULL OR similar_product_id IS NULL OR score IS NULL "
            "OR BTRIM(source_product_id) = '' OR BTRIM(similar_product_id) = '' "
            "OR score = 'NaN'::double precision "
            "OR score = 'Infinity'::double precision "
            "OR score = '-Infinity'::double precision"
        ),
    ),
}

REQUIRED_COMPONENTS = tuple(COMPONENT_SPECS)


@dataclass(frozen=True)
class ExportPlan:
    v2_enabled: bool
    generation_id: str | None
    target_table: str
    write_mode: str


class ModelPublicationError(RuntimeError):
    pass


class GenerationValidationError(ModelPublicationError):
    def __init__(self, report: dict[str, Any]):
        self.report = report
        super().__init__(json.dumps(report, sort_keys=True))


def validate_generation_id(generation_id: Any) -> str:
    if not isinstance(generation_id, str):
        raise ValueError("generation_id must be a string")
    normalized = generation_id.strip()
    if not normalized:
        raise ValueError("generation_id must not be empty")
    if len(normalized) > GENERATION_ID_MAX_LENGTH:
        raise ValueError(f"generation_id must be at most {GENERATION_ID_MAX_LENGTH} characters")
    if not GENERATION_ID_PATTERN.fullmatch(normalized):
        raise ValueError("generation_id contains unsupported characters")
    return normalized


def parse_boolean(value: Any, *, name: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off", ""}:
        return False
    raise ValueError(f"{name} must be true or false")


def publication_v2_enabled(environment: Mapping[str, str] | None = None) -> bool:
    source = os.environ if environment is None else environment
    return parse_boolean(
        source.get("MODEL_PUBLICATION_V2", "false"),
        name="MODEL_PUBLICATION_V2",
    )


def require_generation_id(environment: Mapping[str, str] | None = None) -> str:
    source = os.environ if environment is None else environment
    return validate_generation_id(source.get("MODEL_GENERATION_ID"))


def resolve_export_plan(
    component_name: str,
    legacy_table: str,
    environment: Mapping[str, str] | None = None,
) -> ExportPlan:
    spec = require_component(component_name)
    if not publication_v2_enabled(environment):
        return ExportPlan(False, None, legacy_table, "overwrite")
    generation_id = require_generation_id(environment)
    return ExportPlan(True, generation_id, spec.version_table, "append")


def require_component(component_name: str) -> ComponentSpec:
    try:
        return COMPONENT_SPECS[component_name]
    except KeyError as exc:
        raise ValueError(f"Unknown publication component: {component_name}") from exc


def missing_required_components(component_names: Sequence[str]) -> tuple[str, ...]:
    present = set(component_names)
    return tuple(component for component in REQUIRED_COMPONENTS if component not in present)


def validate_status_transition(current_status: str, next_status: str) -> None:
    if next_status not in NORMAL_STATUS_TRANSITIONS.get(current_status, frozenset()):
        raise ValueError(
            f"Invalid normal generation status transition: {current_status} -> {next_status}"
        )


def validate_rollback_status_transition(current_status: str, next_status: str) -> None:
    if next_status not in ROLLBACK_STATUS_TRANSITIONS.get(current_status, frozenset()):
        raise ValueError(
            f"Invalid rollback generation status transition: {current_status} -> {next_status}"
        )


def component_spec_contract_errors() -> dict[str, tuple[str, ...]]:
    errors: dict[str, tuple[str, ...]] = {}
    for component_name, spec in COMPONENT_SPECS.items():
        missing_columns = tuple(
            column
            for column in spec.required_columns
            if re.search(rf"\b{re.escape(column)}\b", spec.invalid_row_predicate) is None
        )
        if missing_columns:
            errors[component_name] = missing_columns
    return errors


_COMPONENT_SPEC_ERRORS = component_spec_contract_errors()
if _COMPONENT_SPEC_ERRORS:
    raise RuntimeError(
        f"Component validation predicates omit required columns: {_COMPONENT_SPEC_ERRORS}"
    )


def default_manifest() -> dict[str, Any]:
    return {
        "publication_schema_version": 2,
        "required_components": list(REQUIRED_COMPONENTS),
        "publication_boundary": "recommendation_models_only",
        "excluded_reference_outputs": ["global_popular", "dim_products"],
    }


def _cursor(connection):
    return connection.cursor()


def _fetchone(connection, sql: str, parameters: tuple[Any, ...]):
    cursor = _cursor(connection)
    try:
        cursor.execute(sql, parameters)
        return cursor.fetchone()
    finally:
        cursor.close()


def _fetchall(connection, sql: str, parameters: tuple[Any, ...]):
    cursor = _cursor(connection)
    try:
        cursor.execute(sql, parameters)
        return cursor.fetchall()
    finally:
        cursor.close()


def _execute(connection, sql: str, parameters: tuple[Any, ...] = ()) -> int:
    cursor = _cursor(connection)
    try:
        cursor.execute(sql, parameters)
        return int(cursor.rowcount)
    finally:
        cursor.close()


def create_generation(
    connection,
    *,
    airflow_run_id: str | None = None,
    generation_id: str | None = None,
) -> str:
    candidate = validate_generation_id(generation_id or str(uuid.uuid4()))
    manifest = default_manifest()
    try:
        _execute(
            connection,
            """
            INSERT INTO public.recommendation_generations
                (generation_id, airflow_run_id, status, manifest)
            VALUES (%s, %s, 'BUILDING', %s::jsonb)
            """,
            (candidate, airflow_run_id, json.dumps(manifest, sort_keys=True)),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return candidate


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _valid_validation_report(value: Any) -> Mapping[str, Any] | None:
    try:
        report = _decode_json(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(report, Mapping) or report.get("valid") is not True:
        return None
    return report


def _component_records(connection, generation_id: str) -> dict[str, dict[str, Any]]:
    rows = _fetchall(
        connection,
        """
        SELECT component_name, row_count, completion_status, source_info
        FROM public.recommendation_generation_components
        WHERE generation_id = %s
        """,
        (generation_id,),
    )
    return {
        str(name): {
            "row_count": int(row_count),
            "completion_status": str(status),
            "source_info": _decode_json(source_info),
        }
        for name, row_count, status, source_info in rows
    }


def validate_generation(connection, generation_id: str) -> dict[str, Any]:
    candidate = validate_generation_id(generation_id)
    try:
        generation = _fetchone(
            connection,
            """
            SELECT status, manifest, validated_at
            FROM public.recommendation_generations
            WHERE generation_id = %s
            FOR UPDATE
            """,
            (candidate,),
        )
        if generation is None:
            raise ModelPublicationError(f"Generation does not exist: {candidate}")

        status, raw_manifest, _validated_at = generation
        if status != BUILDING:
            raise ModelPublicationError(
                f"Generation {candidate} must be BUILDING for validation; found {status}"
            )

        manifest = _decode_json(raw_manifest) or {}
        errors: list[str] = []
        manifest_components = manifest.get("required_components", [])
        if missing_required_components(manifest_components):
            errors.append("generation source manifest is missing required components")

        records = _component_records(connection, candidate)
        missing = missing_required_components(tuple(records))
        if missing:
            errors.append(f"missing component completion records: {', '.join(missing)}")

        component_reports: dict[str, Any] = {}
        for component_name in REQUIRED_COMPONENTS:
            spec = COMPONENT_SPECS[component_name]
            record = records.get(component_name)
            actual_count = int(
                _fetchone(
                    connection,
                    f"SELECT COUNT(*) FROM {spec.version_table} WHERE generation_id = %s",
                    (candidate,),
                )[0]
            )
            invalid_count = int(
                _fetchone(
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
                _fetchone(
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

            recorded_count = record["row_count"] if record else None
            component_reports[component_name] = {
                "actual_row_count": actual_count,
                "recorded_row_count": recorded_count,
                "invalid_row_count": invalid_count,
                "duplicate_key_count": duplicate_count,
            }

            if actual_count == 0:
                errors.append(f"{component_name}: required component is empty")
            if record and record["completion_status"] != "COMPLETE":
                errors.append(f"{component_name}: completion status is not COMPLETE")
            if record and not record["source_info"]:
                errors.append(f"{component_name}: source manifest is missing")
            if record and recorded_count != actual_count:
                errors.append(
                    f"{component_name}: recorded row count {recorded_count} "
                    f"does not match actual row count {actual_count}"
                )
            if invalid_count:
                errors.append(f"{component_name}: contains {invalid_count} invalid row(s)")
            if duplicate_count:
                errors.append(f"{component_name}: contains {duplicate_count} duplicate key(s)")

        report = {
            "generation_id": candidate,
            "valid": not errors,
            "required_components": list(REQUIRED_COMPONENTS),
            "components": component_reports,
            "errors": errors,
        }

        if errors:
            validate_status_transition(BUILDING, FAILED)
            updated = _execute(
                connection,
                """
                UPDATE public.recommendation_generations
                SET status = 'FAILED', validation_report = %s::jsonb
                WHERE generation_id = %s AND status = 'BUILDING'
                """,
                (json.dumps(report, sort_keys=True), candidate),
            )
            if updated != 1:
                raise ModelPublicationError(
                    "Generation status changed while storing failed validation"
                )
            connection.commit()
            raise GenerationValidationError(report)

        validate_status_transition(BUILDING, READY)
        updated = _execute(
            connection,
            """
            UPDATE public.recommendation_generations
            SET status = 'READY', validated_at = NOW(), validation_report = %s::jsonb
            WHERE generation_id = %s AND status = 'BUILDING'
            """,
            (json.dumps(report, sort_keys=True), candidate),
        )
        if updated != 1:
            raise ModelPublicationError("Generation status changed during validation")
        connection.commit()
        return report
    except GenerationValidationError:
        raise
    except Exception:
        connection.rollback()
        raise


def _assert_components_complete(connection, generation_id: str) -> None:
    records = _component_records(connection, generation_id)
    missing = missing_required_components(tuple(records))
    incomplete = [
        name
        for name in REQUIRED_COMPONENTS
        if name in records
        and (
            records[name]["completion_status"] != "COMPLETE"
            or records[name]["row_count"] <= 0
        )
    ]
    if missing or incomplete:
        details = []
        if missing:
            details.append(f"missing={','.join(missing)}")
        if incomplete:
            details.append(f"incomplete={','.join(incomplete)}")
        raise ModelPublicationError("Required components are not complete: " + " ".join(details))


def publish_generation(connection, generation_id: str) -> dict[str, Any]:
    candidate = validate_generation_id(generation_id)
    try:
        # Consistent lock order: pointer, current generation, candidate generation.
        pointer = _fetchone(
            connection,
            """
            SELECT generation_id
            FROM public.active_recommendation_generation
            WHERE singleton_key = 1
            FOR UPDATE
            """,
            (),
        )
        if pointer is None:
            raise ModelPublicationError("Active-generation singleton row is missing")
        previous_generation_id = pointer[0]
        if previous_generation_id == candidate:
            raise ModelPublicationError("Candidate generation is already the active pointer")

        if previous_generation_id:
            current_row = _fetchone(
                connection,
                """
                SELECT status
                FROM public.recommendation_generations
                WHERE generation_id = %s
                FOR UPDATE
                """,
                (previous_generation_id,),
            )
            if current_row is None:
                raise ModelPublicationError(
                    "Active pointer references a missing generation"
                )
            if current_row[0] != ACTIVE:
                raise ModelPublicationError(
                    "Active pointer references a generation that is not ACTIVE"
                )

        generation = _fetchone(
            connection,
            """
            SELECT status, validated_at
            FROM public.recommendation_generations
            WHERE generation_id = %s
            FOR UPDATE
            """,
            (candidate,),
        )
        if generation is None or generation[0] != READY or generation[1] is None:
            raise ModelPublicationError("Only a validated READY generation can be published")

        _assert_components_complete(connection, candidate)
        validate_status_transition(READY, ACTIVE)
        if previous_generation_id:
            validate_status_transition(ACTIVE, SUPERSEDED)
            updated_previous = _execute(
                connection,
                """
                UPDATE public.recommendation_generations
                SET status = 'SUPERSEDED'
                WHERE generation_id = %s AND status = 'ACTIVE'
                """,
                (previous_generation_id,),
            )
            if updated_previous != 1:
                raise ModelPublicationError(
                    "Active pointer references a generation that is not ACTIVE"
                )

        updated_generation = _execute(
            connection,
            """
            UPDATE public.recommendation_generations
            SET status = 'ACTIVE', published_at = NOW(), previous_generation_id = %s
            WHERE generation_id = %s AND status = 'READY'
            """,
            (previous_generation_id, candidate),
        )
        if updated_generation != 1:
            raise ModelPublicationError(
                "Candidate publication update did not affect exactly one row"
            )

        updated_pointer = _execute(
            connection,
            """
            UPDATE public.active_recommendation_generation
            SET generation_id = %s, updated_at = NOW()
            WHERE singleton_key = 1
            """,
            (candidate,),
        )
        if updated_pointer != 1:
            raise ModelPublicationError(
                "Active pointer update did not affect exactly one row"
            )
        connection.commit()
        return {
            "generation_id": candidate,
            "previous_generation_id": previous_generation_id,
            "status": ACTIVE,
        }
    except Exception:
        connection.rollback()
        raise


def rollback_generation(
    connection,
    generation_id: str,
    *,
    reason: str | None = None,
) -> dict[str, Any]:
    target = validate_generation_id(generation_id)
    try:
        # Consistent lock order: pointer, current generation, rollback target.
        pointer = _fetchone(
            connection,
            """
            SELECT generation_id
            FROM public.active_recommendation_generation
            WHERE singleton_key = 1
            FOR UPDATE
            """,
            (),
        )
        if pointer is None:
            raise ModelPublicationError("Active-generation singleton row is missing")
        current = pointer[0]
        if not current:
            raise ModelPublicationError("No active generation exists to roll back")
        if current == target:
            raise ModelPublicationError("Target generation is already active")

        current_row = _fetchone(
            connection,
            """
            SELECT status
            FROM public.recommendation_generations
            WHERE generation_id = %s
            FOR UPDATE
            """,
            (current,),
        )
        if current_row is None:
            raise ModelPublicationError("Active pointer references a missing generation")
        if current_row[0] != ACTIVE:
            raise ModelPublicationError(
                "Active pointer references a generation that is not ACTIVE"
            )

        target_row = _fetchone(
            connection,
            """
            SELECT status, validated_at, validation_report,
                   published_at, previous_generation_id
            FROM public.recommendation_generations
            WHERE generation_id = %s
            FOR UPDATE
            """,
            (target,),
        )
        if (
            target_row is None
            or target_row[0] != SUPERSEDED
            or target_row[1] is None
            or _valid_validation_report(target_row[2]) is None
            or target_row[3] is None
        ):
            raise ModelPublicationError(
                "Rollback target must be a validated, previously published SUPERSEDED generation"
            )

        _assert_components_complete(connection, target)
        validate_rollback_status_transition(ACTIVE, SUPERSEDED)
        validate_rollback_status_transition(SUPERSEDED, ACTIVE)

        updated_current = _execute(
            connection,
            """
            UPDATE public.recommendation_generations
            SET status = 'SUPERSEDED'
            WHERE generation_id = %s AND status = 'ACTIVE'
            """,
            (current,),
        )
        if updated_current != 1:
            raise ModelPublicationError(
                "Current generation update did not affect exactly one row"
            )

        updated_target = _execute(
            connection,
            """
            UPDATE public.recommendation_generations
            SET status = 'ACTIVE'
            WHERE generation_id = %s AND status = 'SUPERSEDED'
            """,
            (target,),
        )
        if updated_target != 1:
            raise ModelPublicationError(
                "Rollback target update did not affect exactly one row"
            )

        updated_pointer = _execute(
            connection,
            """
            UPDATE public.active_recommendation_generation
            SET generation_id = %s, updated_at = NOW()
            WHERE singleton_key = 1
            """,
            (target,),
        )
        if updated_pointer != 1:
            raise ModelPublicationError(
                "Active pointer update did not affect exactly one row"
            )

        original_published_at = target_row[3]
        original_previous_generation_id = target_row[4]
        audit_updated = _execute(
            connection,
            """
            INSERT INTO public.recommendation_generation_rollbacks
                (from_generation_id, to_generation_id, reason, metadata)
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            (
                current,
                target,
                reason,
                json.dumps(
                    {
                        "operation": "rollback",
                        "source": "internal_cli",
                        "target_original_published_at": str(original_published_at),
                        "target_original_previous_generation_id": (
                            original_previous_generation_id
                        ),
                    },
                    sort_keys=True,
                ),
            ),
        )
        if audit_updated != 1:
            raise ModelPublicationError(
                "Rollback audit insert did not affect exactly one row"
            )
        connection.commit()
        return {
            "generation_id": target,
            "previous_active_generation_id": current,
            "status": ACTIVE,
        }
    except Exception:
        connection.rollback()
        raise


def open_postgres_connection(environment: Mapping[str, str] | None = None):
    source = os.environ if environment is None else environment
    dsn = source.get("MODEL_PUBLICATION_DATABASE_DSN", DEFAULT_POSTGRES_DSN)
    import psycopg2

    return psycopg2.connect(dsn)


def _set_prepared_statement_parameters(statement, parameters: Sequence[Any]) -> None:
    for index, value in enumerate(parameters, start=1):
        if value is None:
            statement.setObject(index, None)
        elif isinstance(value, bool):
            statement.setBoolean(index, value)
        elif isinstance(value, int):
            statement.setLong(index, value)
        elif isinstance(value, float):
            statement.setDouble(index, value)
        else:
            statement.setString(index, str(value))


def _jdbc_connection(spark, environment: Mapping[str, str] | None = None):
    source = os.environ if environment is None else environment
    jdbc_url = source.get("MODEL_PUBLICATION_JDBC_URL", DEFAULT_JDBC_URL)
    user = source.get("MODEL_PUBLICATION_DB_USER", DEFAULT_POSTGRES_USER)
    password = source.get("MODEL_PUBLICATION_DB_PASSWORD", DEFAULT_POSTGRES_PASSWORD)
    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    return jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)


def _close_jdbc_resource(resource) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception:
        pass


def _jdbc_building_generation_transaction(
    spark,
    generation_id: str,
    statements: Sequence[tuple[str, Sequence[Any]]],
) -> None:
    candidate = validate_generation_id(generation_id)
    connection = _jdbc_connection(spark)
    try:
        connection.setAutoCommit(False)
        lock_statement = connection.prepareStatement(
            """
            SELECT status
            FROM public.recommendation_generations
            WHERE generation_id = ?
            FOR UPDATE
            """
        )
        result_set = None
        try:
            _set_prepared_statement_parameters(lock_statement, (candidate,))
            result_set = lock_statement.executeQuery()
            status = str(result_set.getObject(1)) if result_set.next() else None
        finally:
            _close_jdbc_resource(result_set)
            _close_jdbc_resource(lock_statement)

        if status != BUILDING:
            raise ModelPublicationError(
                f"Generation {candidate} must be BUILDING for component mutation; "
                f"found {status}"
            )

        for sql, parameters in statements:
            statement = connection.prepareStatement(sql)
            try:
                _set_prepared_statement_parameters(statement, parameters)
                statement.executeUpdate()
            finally:
                _close_jdbc_resource(statement)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        _close_jdbc_resource(connection)


def retry_cleanup_statements(
    generation_id: str,
    component_names: Sequence[str],
) -> list[tuple[str, tuple[Any, ...]]]:
    candidate = validate_generation_id(generation_id)
    statements: list[tuple[str, tuple[Any, ...]]] = []
    for component_name in component_names:
        spec = require_component(component_name)
        statements.append(
            (f"DELETE FROM {spec.version_table} WHERE generation_id = ?", (candidate,))
        )
        statements.append(
            (
                "DELETE FROM public.recommendation_generation_components "
                "WHERE generation_id = ? AND component_name = ?",
                (candidate, component_name),
            )
        )
    return statements


def prepare_component_retry_spark(
    spark,
    generation_id: str,
    component_names: Sequence[str],
) -> None:
    candidate = validate_generation_id(generation_id)
    _jdbc_building_generation_transaction(
        spark,
        candidate,
        retry_cleanup_statements(candidate, component_names),
    )


def record_component_complete_spark(
    spark,
    generation_id: str,
    component_name: str,
    row_count: int,
    source_info: Mapping[str, Any],
    *,
    checksum: str | None = None,
) -> None:
    candidate = validate_generation_id(generation_id)
    require_component(component_name)
    if row_count <= 0:
        raise ModelPublicationError(
            f"Required component {component_name} must export at least one row"
        )
    _jdbc_building_generation_transaction(
        spark,
        candidate,
        [
            (
                """
                INSERT INTO public.recommendation_generation_components
                    (generation_id, component_name, row_count, completion_status,
                     checksum, completed_at, source_info)
                VALUES (?, ?, ?, 'COMPLETE', ?, NOW(), ?::jsonb)
                ON CONFLICT (generation_id, component_name) DO UPDATE SET
                    row_count = EXCLUDED.row_count,
                    completion_status = 'COMPLETE',
                    checksum = EXCLUDED.checksum,
                    completed_at = NOW(),
                    source_info = EXCLUDED.source_info
                """,
                (
                    candidate,
                    component_name,
                    row_count,
                    checksum,
                    json.dumps(dict(source_info), sort_keys=True),
                ),
            )
        ],
    )
