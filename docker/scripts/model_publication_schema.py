from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


MIGRATION_ID = "001_atomic_model_publication_v2"
TRACKING_TABLE = "model_publication_schema_migrations"
MIN_POSTGRES_VERSION_NUM = 130000
GUARD_FUNCTION = "public.guard_building_generation_mutation"
GUARD_TRIGGER = "trg_guard_building_generation"


def _columns(*definitions: tuple[str, str, bool]) -> dict[str, tuple[str, bool]]:
    return {name: (data_type, nullable) for name, data_type, nullable in definitions}


EXPECTED_COLUMNS = {
    "model_publication_schema_migrations": _columns(
        ("migration_id", "text", False),
        ("checksum", "text", False),
        ("applied_at", "timestamp with time zone", False),
        ("applied_by", "text", False),
    ),
    "recommendation_generations": _columns(
        ("generation_id", "character varying", False),
        ("airflow_run_id", "text", True),
        ("status", "character varying", False),
        ("created_at", "timestamp with time zone", False),
        ("validated_at", "timestamp with time zone", True),
        ("published_at", "timestamp with time zone", True),
        ("failed_at", "timestamp with time zone", True),
        ("previous_generation_id", "character varying", True),
        ("manifest", "jsonb", False),
        ("validation_report", "jsonb", True),
        ("failure_report", "jsonb", True),
    ),
    "active_recommendation_generation": _columns(
        ("singleton_key", "smallint", False),
        ("generation_id", "character varying", True),
        ("updated_at", "timestamp with time zone", False),
    ),
    "recommendation_generation_components": _columns(
        ("generation_id", "character varying", False),
        ("component_name", "character varying", False),
        ("row_count", "bigint", False),
        ("completion_status", "character varying", False),
        ("checksum", "text", True),
        ("completed_at", "timestamp with time zone", False),
        ("source_info", "jsonb", False),
    ),
    "serving_user_clusters_versions": _columns(
        ("generation_id", "character varying", False),
        ("user_id", "text", False),
        ("cluster_id", "integer", False),
        ("segment_name", "text", False),
    ),
    "serving_recommendations_versions": _columns(
        ("generation_id", "character varying", False),
        ("cluster_id", "integer", False),
        ("product_id", "text", False),
        ("display_name", "text", False),
        ("cluster_total_score", "double precision", False),
    ),
    "serving_als_versions": _columns(
        ("generation_id", "character varying", False),
        ("user_id", "text", False),
        ("product_id", "text", False),
        ("score", "double precision", False),
    ),
    "serving_content_based_versions": _columns(
        ("generation_id", "character varying", False),
        ("user_id", "text", True),
        ("product_id", "text", True),
        ("source_product_id", "text", True),
        ("recommended_product_id", "text", True),
        ("score", "double precision", False),
    ),
    "serving_item_based_versions": _columns(
        ("generation_id", "character varying", False),
        ("source_product_id", "text", False),
        ("similar_product_id", "text", False),
        ("score", "double precision", False),
    ),
    "recommendation_generation_rollbacks": _columns(
        ("rollback_id", "bigint", False),
        ("from_generation_id", "character varying", True),
        ("to_generation_id", "character varying", False),
        ("rolled_back_at", "timestamp with time zone", False),
        ("reason", "text", True),
        ("metadata", "jsonb", False),
    ),
}

VERSION_TABLES = (
    "serving_user_clusters_versions",
    "serving_recommendations_versions",
    "serving_als_versions",
    "serving_content_based_versions",
    "serving_item_based_versions",
)

EXPECTED_INDEX_FRAGMENTS = {
    "uq_recommendation_generations_airflow_run": (
        "recommendation_generations",
        "unique",
        "airflow_run_id",
        "where",
        "is not null",
    ),
    "uq_recommendation_generations_one_active": (
        "recommendation_generations",
        "unique",
        "status",
        "where",
        "active",
    ),
    "uq_serving_content_versions_user": (
        "serving_content_based_versions",
        "generation_id",
        "user_id",
        "product_id",
        "where",
    ),
    "uq_serving_content_versions_item": (
        "serving_content_based_versions",
        "generation_id",
        "source_product_id",
        "recommended_product_id",
        "where",
    ),
}

EXPECTED_CONSTRAINTS = {
    "model_publication_schema_migrations": {
        "p": (("migration_id",),),
    },
    "recommendation_generations": {
        "p": (("generation_id",),),
        "f": (("previous_generation_id", "recommendation_generations"),),
        "c": (("status", "building", "ready", "active", "superseded", "failed"),),
    },
    "active_recommendation_generation": {
        "p": (("singleton_key",),),
        "f": (("generation_id", "recommendation_generations"),),
        "c": (("singleton_key", "1"),),
    },
    "recommendation_generation_components": {
        "p": (("generation_id", "component_name"),),
        "f": (("generation_id", "recommendation_generations"),),
        "c": (
            ("component_name", "user_clusters", "cluster_recommendations", "als", "content_based", "item_based"),
            ("row_count", "0"),
            ("completion_status", "complete", "failed"),
        ),
    },
    "serving_user_clusters_versions": {
        "p": (("generation_id", "user_id"),),
        "f": (("generation_id", "recommendation_generations"),),
    },
    "serving_recommendations_versions": {
        "p": (("generation_id", "cluster_id", "product_id"),),
        "f": (("generation_id", "recommendation_generations"),),
    },
    "serving_als_versions": {
        "p": (("generation_id", "user_id", "product_id"),),
        "f": (("generation_id", "recommendation_generations"),),
    },
    "serving_content_based_versions": {
        "f": (("generation_id", "recommendation_generations"),),
        "c": (("user_id", "product_id", "source_product_id", "recommended_product_id"),),
    },
    "serving_item_based_versions": {
        "p": (("generation_id", "source_product_id", "similar_product_id"),),
        "f": (("generation_id", "recommendation_generations"),),
    },
    "recommendation_generation_rollbacks": {
        "p": (("rollback_id",),),
        "f": (
            ("from_generation_id", "recommendation_generations"),
            ("to_generation_id", "recommendation_generations"),
        ),
    },
}

ALLOWED_STATUSES = {"BUILDING", "READY", "ACTIVE", "SUPERSEDED", "FAILED"}


@dataclass
class CatalogSnapshot:
    postgres_version_num: int
    current_user: str
    permissions: Mapping[str, bool]
    tables: Mapping[str, str] = field(default_factory=dict)
    columns: Mapping[str, Mapping[str, tuple[str, bool]]] = field(default_factory=dict)
    constraints: Mapping[str, list[tuple[str, str]]] = field(default_factory=dict)
    indexes: Mapping[str, str] = field(default_factory=dict)
    triggers: Mapping[str, str] = field(default_factory=dict)
    function_definition: str | None = None
    lifecycle_statuses: set[str] = field(default_factory=set)
    duplicate_active_count: int = 0
    duplicate_airflow_run_id_count: int = 0
    pointer_row_count: int = 0
    pointer_status_consistent: bool = True


def _normalize(value: Any) -> str:
    return " ".join(str(value).lower().split())


def _has_fragments(value: str, fragments: tuple[str, ...]) -> bool:
    normalized = _normalize(value)
    return all(_normalize(fragment) in normalized for fragment in fragments)


def evaluate_catalog(
    snapshot: CatalogSnapshot,
    *,
    require_complete: bool,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    expected_tables = set(EXPECTED_COLUMNS)
    publication_tables = expected_tables - {TRACKING_TABLE}
    present_tables = expected_tables.intersection(snapshot.tables)

    if snapshot.postgres_version_num < MIN_POSTGRES_VERSION_NUM:
        errors.append(
            f"PostgreSQL {MIN_POSTGRES_VERSION_NUM} or newer is required"
        )
    for permission, granted in sorted(snapshot.permissions.items()):
        if not granted:
            errors.append(f"required permission is missing: {permission}")

    if not present_tables and not require_complete:
        return {
            "valid": not errors,
            "state": "FRESH",
            "errors": errors,
            "warnings": warnings,
        }

    missing_table_set = expected_tables - present_tables
    untracked_compatible = (
        not require_complete
        and publication_tables.issubset(present_tables)
        and missing_table_set == {TRACKING_TABLE}
    )
    if untracked_compatible:
        warnings.append(
            "Publication schema predates migration tracking; explicit apply will register it"
        )
        missing_table_set = set()
    missing_tables = sorted(missing_table_set)
    if missing_tables:
        qualifier = "partial migration" if present_tables else "migration not applied"
        errors.append(f"{qualifier}; missing tables: {', '.join(missing_tables)}")

    for table in sorted(present_tables):
        owner = snapshot.tables[table]
        if owner != snapshot.current_user:
            errors.append(
                f"public.{table} is owned by {owner}, not {snapshot.current_user}"
            )
        actual_columns = snapshot.columns.get(table, {})
        expected_columns = EXPECTED_COLUMNS[table]
        for column, expected_contract in expected_columns.items():
            actual_contract = actual_columns.get(column)
            if actual_contract is None:
                errors.append(f"public.{table} is missing column {column}")
            elif actual_contract != expected_contract:
                errors.append(
                    f"public.{table}.{column} has contract {actual_contract!r}; "
                    f"expected {expected_contract!r}"
                )

        actual_constraints = snapshot.constraints.get(table, [])
        for constraint_type, fragment_groups in EXPECTED_CONSTRAINTS[table].items():
            definitions = [
                definition
                for actual_type, definition in actual_constraints
                if actual_type == constraint_type
            ]
            for fragments in fragment_groups:
                if not any(_has_fragments(definition, fragments) for definition in definitions):
                    errors.append(
                        f"public.{table} lacks {constraint_type} constraint containing "
                        f"{', '.join(fragments)}"
                    )

    for index_name, fragments in EXPECTED_INDEX_FRAGMENTS.items():
        definition = snapshot.indexes.get(index_name)
        if definition is None or not _has_fragments(definition, fragments):
            errors.append(f"index definition is missing or incompatible: public.{index_name}")

    function_definition = snapshot.function_definition or ""
    for fragment in (
        "guard_building_generation_mutation",
        "tg_op = 'insert'",
        "tg_op = 'update'",
        "tg_op = 'delete'",
        "building",
    ):
        if _normalize(fragment) not in _normalize(function_definition):
            errors.append(f"guard function definition is missing fragment: {fragment}")

    for table in VERSION_TABLES:
        trigger_key = f"public.{table}.{GUARD_TRIGGER}"
        definition = snapshot.triggers.get(trigger_key)
        required = (
            "before insert or delete or update",
            f"on public.{table}",
            "guard_building_generation_mutation",
        )
        if definition is None or not _has_fragments(definition, required):
            errors.append(f"guard trigger definition is missing or incompatible: {trigger_key}")

    unexpected_statuses = sorted(snapshot.lifecycle_statuses - ALLOWED_STATUSES)
    if unexpected_statuses:
        errors.append(f"unexpected lifecycle statuses: {', '.join(unexpected_statuses)}")
    if snapshot.duplicate_active_count:
        errors.append("duplicate ACTIVE generations exist")
    if snapshot.duplicate_airflow_run_id_count:
        errors.append("duplicate non-null airflow_run_id values exist")
    if snapshot.pointer_row_count != 1:
        errors.append(
            f"active pointer row count is {snapshot.pointer_row_count}; expected 1"
        )
    if not snapshot.pointer_status_consistent:
        errors.append("active pointer does not reference the sole ACTIVE generation")

    return {
        "valid": not errors,
        "state": (
            "UNTRACKED_COMPATIBLE"
            if untracked_compatible
            else "COMPLETE" if not missing_tables else "INCOMPATIBLE"
        ),
        "errors": errors,
        "warnings": warnings,
    }
