from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT / "docker" / "scripts"))

import model_publication as publication  # noqa: E402


GENERATION_ID = "generation_20260628_a1b2c3"


class FakeConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def complete_records(row_count: int = 3) -> dict[str, dict[str, object]]:
    return {
        component: {
            "row_count": row_count,
            "completion_status": "COMPLETE",
            "source_info": {"source": f"gold.{component}"},
        }
        for component in publication.REQUIRED_COMPONENTS
    }


@pytest.mark.parametrize(
    "generation_id",
    [GENERATION_ID, "550e8400-e29b-41d4-a716-446655440000", "a.b-c_1"],
)
def test_generation_id_validation_accepts_safe_values(generation_id) -> None:
    assert publication.validate_generation_id(generation_id) == generation_id


@pytest.mark.parametrize(
    "generation_id",
    [None, "", "   ", "unsafe/id", "unsafe id", "x" * 81],
)
def test_generation_id_validation_rejects_unsafe_values(generation_id) -> None:
    with pytest.raises(ValueError):
        publication.validate_generation_id(generation_id)


def test_allowed_and_rejected_status_transitions() -> None:
    publication.validate_status_transition(publication.BUILDING, publication.READY)
    publication.validate_status_transition(publication.READY, publication.ACTIVE)
    publication.validate_status_transition(publication.ACTIVE, publication.SUPERSEDED)
    with pytest.raises(ValueError):
        publication.validate_status_transition(publication.BUILDING, publication.ACTIVE)
    with pytest.raises(ValueError):
        publication.validate_status_transition(publication.SUPERSEDED, publication.ACTIVE)


def test_required_component_manifest_completeness() -> None:
    assert publication.missing_required_components(publication.REQUIRED_COMPONENTS) == ()
    assert publication.missing_required_components(("als",)) == (
        "user_clusters",
        "cluster_recommendations",
        "content_based",
        "item_based",
    )


def test_export_v2_requires_generation_id() -> None:
    with pytest.raises(ValueError, match="generation_id"):
        publication.resolve_export_plan(
            "als",
            "public.serving_als",
            {"MODEL_PUBLICATION_V2": "true"},
        )


def test_legacy_export_plan_preserves_target_and_overwrite_mode() -> None:
    plan = publication.resolve_export_plan(
        "als",
        "public.serving_als",
        {"MODEL_PUBLICATION_V2": "false"},
    )
    assert plan.v2_enabled is False
    assert plan.generation_id is None
    assert plan.target_table == "public.serving_als"
    assert plan.write_mode == "overwrite"


@pytest.mark.parametrize(
    ("component", "legacy_table", "versioned_table"),
    [
        (
            "user_clusters",
            "serving_user_clusters",
            "public.serving_user_clusters_versions",
        ),
        (
            "cluster_recommendations",
            "serving_recommendations",
            "public.serving_recommendations_versions",
        ),
        ("als", "public.serving_als", "public.serving_als_versions"),
        (
            "content_based",
            "public.serving_content_based",
            "public.serving_content_based_versions",
        ),
        (
            "item_based",
            "public.serving_item_based",
            "public.serving_item_based_versions",
        ),
    ],
)
def test_v2_export_plan_targets_only_versioned_tables(
    component,
    legacy_table,
    versioned_table,
) -> None:
    plan = publication.resolve_export_plan(
        component,
        legacy_table,
        {
            "MODEL_PUBLICATION_V2": "true",
            "MODEL_GENERATION_ID": GENERATION_ID,
        },
    )
    assert plan.v2_enabled is True
    assert plan.generation_id == GENERATION_ID
    assert plan.target_table == versioned_table
    assert plan.write_mode == "append"


def test_retry_cleanup_is_restricted_to_one_generation() -> None:
    statements = publication.retry_cleanup_statements(
        GENERATION_ID,
        ("cluster_recommendations", "user_clusters"),
    )
    assert len(statements) == 4
    assert all(GENERATION_ID in parameters for _, parameters in statements)
    assert all("serving_recommendations " not in sql for sql, _ in statements)
    assert all("serving_user_clusters " not in sql for sql, _ in statements)
    assert any("serving_recommendations_versions" in sql for sql, _ in statements)
    assert any("serving_user_clusters_versions" in sql for sql, _ in statements)


def install_validation_fakes(
    monkeypatch,
    connection: FakeConnection,
    *,
    records: dict[str, dict[str, object]] | None = None,
    actual_counts: dict[str, int] | None = None,
    duplicates: dict[str, int] | None = None,
    invalid_rows: dict[str, int] | None = None,
):
    component_records = complete_records() if records is None else records
    counts = {name: 3 for name in publication.REQUIRED_COMPONENTS}
    counts.update(actual_counts or {})
    duplicate_counts = {name: 0 for name in publication.REQUIRED_COMPONENTS}
    duplicate_counts.update(duplicates or {})
    invalid_counts = {name: 0 for name in publication.REQUIRED_COMPONENTS}
    invalid_counts.update(invalid_rows or {})
    executed: list[tuple[str, tuple[object, ...]]] = []

    monkeypatch.setattr(
        publication,
        "_component_records",
        lambda _connection, _generation_id: component_records,
    )

    def fake_fetchone(_connection, sql, parameters):
        if "FROM public.recommendation_generations" in sql:
            return (publication.BUILDING, publication.default_manifest(), None)
        component = next(
            name
            for name, spec in publication.COMPONENT_SPECS.items()
            if spec.version_table in sql
        )
        if "duplicate_keys" in sql:
            return (duplicate_counts[component],)
        if " AND (" in sql:
            return (invalid_counts[component],)
        return (counts[component],)

    def fake_execute(_connection, sql, parameters=()):
        executed.append((sql, parameters))
        return 1

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)
    monkeypatch.setattr(publication, "_execute", fake_execute)
    return executed


def test_failed_middle_component_leaves_active_pointer_unchanged(monkeypatch) -> None:
    connection = FakeConnection()
    records = complete_records()
    records.pop("content_based")
    executed: list[tuple[str, tuple[object, ...]]] = []
    monkeypatch.setattr(
        publication,
        "_fetchone",
        lambda _connection, sql, _parameters: (
            ("previous_generation",)
            if "active_recommendation_generation" in sql
            else (publication.READY, object())
        ),
    )
    monkeypatch.setattr(publication, "_component_records", lambda *_args: records)
    monkeypatch.setattr(
        publication,
        "_execute",
        lambda _connection, sql, parameters=(): executed.append((sql, parameters)) or 1,
    )

    with pytest.raises(publication.ModelPublicationError, match="not complete"):
        publication.publish_generation(connection, GENERATION_ID)

    assert not any("UPDATE public.active_recommendation_generation" in sql for sql, _ in executed)
    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_validation_rejects_missing_components(monkeypatch) -> None:
    connection = FakeConnection()
    records = complete_records()
    records.pop("item_based")
    install_validation_fakes(monkeypatch, connection, records=records)

    with pytest.raises(publication.GenerationValidationError) as exc_info:
        publication.validate_generation(connection, GENERATION_ID)

    assert "missing component completion records: item_based" in exc_info.value.report["errors"]
    assert connection.commits == 1


def test_validation_rejects_row_count_mismatch(monkeypatch) -> None:
    connection = FakeConnection()
    install_validation_fakes(
        monkeypatch,
        connection,
        actual_counts={"als": 4},
    )

    with pytest.raises(publication.GenerationValidationError) as exc_info:
        publication.validate_generation(connection, GENERATION_ID)

    assert any("als: recorded row count 3" in error for error in exc_info.value.report["errors"])


def test_validation_rejects_duplicate_logical_keys(monkeypatch) -> None:
    connection = FakeConnection()
    install_validation_fakes(
        monkeypatch,
        connection,
        duplicates={"user_clusters": 1},
    )

    with pytest.raises(publication.GenerationValidationError) as exc_info:
        publication.validate_generation(connection, GENERATION_ID)

    assert "user_clusters: contains 1 duplicate key(s)" in exc_info.value.report["errors"]


def test_validation_rejects_invalid_required_columns(monkeypatch) -> None:
    connection = FakeConnection()
    install_validation_fakes(
        monkeypatch,
        connection,
        invalid_rows={"content_based": 2},
    )

    with pytest.raises(publication.GenerationValidationError) as exc_info:
        publication.validate_generation(connection, GENERATION_ID)

    assert "content_based: contains 2 invalid row(s)" in exc_info.value.report["errors"]


def test_validation_marks_complete_generation_ready(monkeypatch) -> None:
    connection = FakeConnection()
    executed = install_validation_fakes(monkeypatch, connection)

    report = publication.validate_generation(connection, GENERATION_ID)

    assert report["valid"] is True
    assert any("SET status = 'READY'" in sql for sql, _ in executed)
    assert connection.commits == 1
    assert connection.rollbacks == 0


def install_publication_fakes(monkeypatch, *, fail_on_pointer_update: bool = False):
    executed: list[tuple[str, tuple[object, ...]]] = []

    def fake_fetchone(_connection, sql, _parameters):
        if "active_recommendation_generation" in sql:
            return ("previous_generation",)
        return (publication.READY, object())

    def fake_execute(_connection, sql, parameters=()):
        executed.append((sql, parameters))
        if fail_on_pointer_update and "UPDATE public.active_recommendation_generation" in sql:
            raise RuntimeError("simulated pointer failure")
        return 1

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)
    monkeypatch.setattr(publication, "_component_records", lambda *_args: complete_records())
    monkeypatch.setattr(publication, "_execute", fake_execute)
    return executed


def test_publication_accepts_only_ready_generation(monkeypatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(
        publication,
        "_fetchone",
        lambda _connection, sql, _parameters: (
            ("previous_generation",)
            if "active_recommendation_generation" in sql
            else (publication.BUILDING, None)
        ),
    )

    with pytest.raises(publication.ModelPublicationError, match="READY"):
        publication.publish_generation(connection, GENERATION_ID)
    assert connection.rollbacks == 1


def test_publication_changes_pointer_and_statuses_in_one_commit(monkeypatch) -> None:
    connection = FakeConnection()
    executed = install_publication_fakes(monkeypatch)

    result = publication.publish_generation(connection, GENERATION_ID)

    assert result == {
        "generation_id": GENERATION_ID,
        "previous_generation_id": "previous_generation",
        "status": publication.ACTIVE,
    }
    assert any("status = 'SUPERSEDED'" in sql for sql, _ in executed)
    assert any("status = 'ACTIVE'" in sql for sql, _ in executed)
    assert any("active_recommendation_generation" in sql for sql, _ in executed)
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_publication_exception_rolls_back_pointer_and_statuses(monkeypatch) -> None:
    connection = FakeConnection()
    install_publication_fakes(monkeypatch, fail_on_pointer_update=True)

    with pytest.raises(RuntimeError, match="pointer failure"):
        publication.publish_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_rollback_switches_to_complete_validated_generation(monkeypatch) -> None:
    connection = FakeConnection()
    executed: list[tuple[str, tuple[object, ...]]] = []

    def fake_fetchone(_connection, sql, _parameters):
        if "active_recommendation_generation" in sql:
            return ("current_generation",)
        return (publication.SUPERSEDED, object(), {"valid": True})

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)
    monkeypatch.setattr(publication, "_component_records", lambda *_args: complete_records())
    monkeypatch.setattr(
        publication,
        "_execute",
        lambda _connection, sql, parameters=(): executed.append((sql, parameters)) or 1,
    )

    result = publication.rollback_generation(
        connection,
        GENERATION_ID,
        reason="acceptance rollback",
    )

    assert result["generation_id"] == GENERATION_ID
    assert result["previous_active_generation_id"] == "current_generation"
    assert any("recommendation_generation_rollbacks" in sql for sql, _ in executed)
    assert connection.commits == 1


def test_generation_ids_are_bound_not_interpolated_into_sql() -> None:
    sources = [
        (
            REPOSITORY_ROOT / "docker" / "scripts" / "model_publication.py"
        ).read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "backend" / "database.py").read_text(encoding="utf-8"),
    ]
    unsafe_sql = re.compile(
        r"(?:SELECT|INSERT|UPDATE|DELETE)[\s\S]{0,300}\{generation_id\}",
        re.IGNORECASE,
    )
    assert all(not unsafe_sql.search(source) for source in sources)
    assert "generation_id = %s" in sources[0]
    assert "generation_id = ?" in sources[0]
    assert "generation_id = :generation_id" in sources[1]


def test_migration_defines_registry_pointer_components_and_version_tables() -> None:
    migration = (
        REPOSITORY_ROOT
        / "docker"
        / "migrations"
        / "001_atomic_model_publication_v2.sql"
    ).read_text(encoding="utf-8")
    required_objects = {
        "recommendation_generations",
        "active_recommendation_generation",
        "recommendation_generation_components",
        "serving_user_clusters_versions",
        "serving_recommendations_versions",
        "serving_als_versions",
        "serving_content_based_versions",
        "serving_item_based_versions",
        "recommendation_generation_rollbacks",
    }

    assert all(f"public.{table_name}" in migration for table_name in required_objects)
    assert "generation_id VARCHAR(80)" in migration
    assert "PRIMARY KEY (generation_id, component_name)" in migration
