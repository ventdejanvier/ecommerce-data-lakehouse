from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT / "docker" / "scripts"))

import model_publication as publication  # noqa: E402
import publish_recommendation_generation as publish_cli  # noqa: E402
import rollback_recommendation_generation as rollback_cli  # noqa: E402
import validate_recommendation_generation as validate_cli  # noqa: E402


GENERATION_ID = "generation_20260628_a1b2c3"
DEFAULT_REPORT = object()


class FakeConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FakeJdbcResultSet:
    def __init__(self, connection, status: str | None) -> None:
        self.connection = connection
        self.status = status
        self.closed = False
        self._read = False

    def next(self) -> bool:
        self.connection.events.append(("result_next",))
        if self._read or self.status is None:
            return False
        self._read = True
        return True

    def getObject(self, index: int):
        self.connection.events.append(("result_get", index))
        return self.status

    def close(self) -> None:
        self.closed = True
        self.connection.events.append(("result_close",))


class FakeJdbcStatement:
    def __init__(self, connection, sql: str) -> None:
        self.connection = connection
        self.sql = " ".join(sql.split())
        self.parameters: dict[int, object] = {}
        self.closed = False

    def _set(self, index: int, value: object) -> None:
        self.parameters[index] = value
        self.connection.events.append(("bind", self.sql, index, value))

    setObject = _set
    setBoolean = _set
    setLong = _set
    setDouble = _set
    setString = _set

    def executeQuery(self):
        self.connection.events.append(("query", self.sql, dict(self.parameters)))
        return FakeJdbcResultSet(self.connection, self.connection.status)

    def executeUpdate(self) -> int:
        self.connection.events.append(("update", self.sql, dict(self.parameters)))
        if self.connection.fail_on_update:
            raise RuntimeError("simulated JDBC mutation failure")
        return 1

    def close(self) -> None:
        self.closed = True
        self.connection.events.append(("statement_close", self.sql))


class FakeJdbcConnection:
    def __init__(self, status: str | None, *, fail_on_update: bool = False) -> None:
        self.status = status
        self.fail_on_update = fail_on_update
        self.events: list[tuple[object, ...]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def setAutoCommit(self, enabled: bool) -> None:
        self.events.append(("autocommit", enabled))

    def prepareStatement(self, sql: str) -> FakeJdbcStatement:
        normalized = " ".join(sql.split())
        self.events.append(("prepare", normalized))
        return FakeJdbcStatement(self, sql)

    def commit(self) -> None:
        self.commits += 1
        self.events.append(("commit",))

    def rollback(self) -> None:
        self.rollbacks += 1
        self.events.append(("rollback",))

    def close(self) -> None:
        self.closed = True
        self.events.append(("connection_close",))


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
    publication.validate_rollback_status_transition(
        publication.SUPERSEDED,
        publication.ACTIVE,
    )
    publication.validate_rollback_status_transition(
        publication.ACTIVE,
        publication.SUPERSEDED,
    )
    with pytest.raises(ValueError):
        publication.validate_rollback_status_transition(
            publication.READY,
            publication.ACTIVE,
        )


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


def install_jdbc_connection(monkeypatch, connection: FakeJdbcConnection):
    opened_connections = []

    def fake_open(_spark):
        opened_connections.append(connection)
        return connection

    monkeypatch.setattr(publication, "_jdbc_connection", fake_open)
    return opened_connections


def mutation_sql_events(connection: FakeJdbcConnection) -> list[str]:
    return [event[1] for event in connection.events if event[0] == "update"]


def test_jdbc_cleanup_locks_and_mutates_on_one_transaction(monkeypatch) -> None:
    connection = FakeJdbcConnection(publication.BUILDING)
    opened = install_jdbc_connection(monkeypatch, connection)

    publication.prepare_component_retry_spark(None, GENERATION_ID, ("als",))

    assert opened == [connection]
    query_index = next(i for i, event in enumerate(connection.events) if event[0] == "query")
    update_index = next(i for i, event in enumerate(connection.events) if event[0] == "update")
    query_sql = connection.events[query_index][1]
    assert "FOR UPDATE" in query_sql
    assert query_index < update_index
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed is True
    assert all(GENERATION_ID in event[2].values() for event in connection.events if event[0] == "update")


@pytest.mark.parametrize(
    "status",
    [publication.READY, publication.ACTIVE, publication.SUPERSEDED, publication.FAILED, None],
)
def test_jdbc_cleanup_rejects_every_non_building_status(monkeypatch, status) -> None:
    connection = FakeJdbcConnection(status)
    install_jdbc_connection(monkeypatch, connection)

    with pytest.raises(publication.ModelPublicationError, match="must be BUILDING"):
        publication.prepare_component_retry_spark(None, GENERATION_ID, ("als",))

    assert mutation_sql_events(connection) == []
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closed is True


def test_jdbc_component_completion_uses_locked_single_transaction(monkeypatch) -> None:
    connection = FakeJdbcConnection(publication.BUILDING)
    opened = install_jdbc_connection(monkeypatch, connection)

    publication.record_component_complete_spark(
        None,
        GENERATION_ID,
        "als",
        12,
        {"source": "gold_db.recommendations_als"},
    )

    assert opened == [connection]
    query_index = next(i for i, event in enumerate(connection.events) if event[0] == "query")
    update_index = next(i for i, event in enumerate(connection.events) if event[0] == "update")
    assert "FOR UPDATE" in connection.events[query_index][1]
    assert query_index < update_index
    assert "recommendation_generation_components" in connection.events[update_index][1]
    assert connection.commits == 1
    assert connection.rollbacks == 0


@pytest.mark.parametrize(
    "status",
    [publication.READY, publication.ACTIVE, publication.SUPERSEDED, publication.FAILED, None],
)
def test_jdbc_component_completion_rejects_non_building_status(
    monkeypatch,
    status,
) -> None:
    connection = FakeJdbcConnection(status)
    install_jdbc_connection(monkeypatch, connection)

    with pytest.raises(publication.ModelPublicationError, match="must be BUILDING"):
        publication.record_component_complete_spark(
            None,
            GENERATION_ID,
            "als",
            12,
            {"source": "gold_db.recommendations_als"},
        )

    assert mutation_sql_events(connection) == []
    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_jdbc_mutation_exception_rolls_back_and_closes_resources(monkeypatch) -> None:
    connection = FakeJdbcConnection(publication.BUILDING, fail_on_update=True)
    install_jdbc_connection(monkeypatch, connection)

    with pytest.raises(RuntimeError, match="JDBC mutation failure"):
        publication.prepare_component_retry_spark(None, GENERATION_ID, ("als",))

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closed is True
    assert any(event[0] == "result_close" for event in connection.events)
    assert any(event[0] == "statement_close" for event in connection.events)


def install_validation_fakes(
    monkeypatch,
    connection: FakeConnection,
    *,
    records: dict[str, dict[str, object]] | None = None,
    actual_counts: dict[str, int] | None = None,
    duplicates: dict[str, int] | None = None,
    invalid_rows: dict[str, int] | None = None,
    query_log: list[tuple[str, tuple[object, ...]]] | None = None,
):
    component_records = complete_records() if records is None else records
    counts = {name: 3 for name in publication.REQUIRED_COMPONENTS}
    counts.update(actual_counts or {})
    duplicate_counts = {name: 0 for name in publication.REQUIRED_COMPONENTS}
    duplicate_counts.update(duplicates or {})
    invalid_counts = {name: 0 for name in publication.REQUIRED_COMPONENTS}
    invalid_counts.update(invalid_rows or {})
    executed: list[tuple[str, tuple[object, ...]]] = []
    queries = [] if query_log is None else query_log

    monkeypatch.setattr(
        publication,
        "_component_records",
        lambda _connection, _generation_id: component_records,
    )

    def fake_fetchone(_connection, sql, parameters):
        queries.append((sql, parameters))
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

    def fake_fetchone(_connection, sql, parameters):
        if "active_recommendation_generation" in sql:
            return ("previous_generation",)
        if parameters == ("previous_generation",):
            return (publication.ACTIVE,)
        return (publication.READY, object())

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)
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
    assert connection.rollbacks == 0


def test_validation_locks_generation_until_status_commit(monkeypatch) -> None:
    connection = FakeConnection()
    queries: list[tuple[str, tuple[object, ...]]] = []
    install_validation_fakes(monkeypatch, connection, query_log=queries)

    publication.validate_generation(connection, GENERATION_ID)

    generation_query, parameters = queries[0]
    assert "FROM public.recommendation_generations" in generation_query
    assert "FOR UPDATE" in generation_query
    assert parameters == (GENERATION_ID,)
    assert connection.commits == 1
    assert connection.rollbacks == 0


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


def test_failed_validation_report_storage_mismatch_rolls_back(monkeypatch) -> None:
    connection = FakeConnection()
    install_validation_fakes(
        monkeypatch,
        connection,
        invalid_rows={"als": 1},
    )
    monkeypatch.setattr(publication, "_execute", lambda *_args, **_kwargs: 0)

    with pytest.raises(publication.ModelPublicationError, match="failed validation"):
        publication.validate_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_component_predicates_cover_every_declared_required_column() -> None:
    assert publication.component_spec_contract_errors() == {}


@pytest.mark.parametrize(
    ("component", "score_column"),
    [
        ("cluster_recommendations", "cluster_total_score"),
        ("als", "score"),
        ("content_based", "score"),
        ("item_based", "score"),
    ],
)
def test_score_predicates_reject_all_non_finite_postgres_values(
    component,
    score_column,
) -> None:
    predicate = publication.COMPONENT_SPECS[component].invalid_row_predicate
    assert f"{score_column} = 'NaN'::double precision" in predicate
    assert f"{score_column} = 'Infinity'::double precision" in predicate
    assert f"{score_column} = '-Infinity'::double precision" in predicate
    assert f"{score_column} < 0" not in predicate


def test_content_component_rejects_mixed_and_partial_shapes_statically() -> None:
    predicate = publication.COMPONENT_SPECS["content_based"].invalid_row_predicate
    assert "user_id IS NOT NULL AND product_id IS NOT NULL" in predicate
    assert "source_product_id IS NULL AND recommended_product_id IS NULL" in predicate
    assert "source_product_id IS NOT NULL AND recommended_product_id IS NOT NULL" in predicate
    assert "user_id IS NULL AND product_id IS NULL" in predicate


def install_publication_fakes(
    monkeypatch,
    *,
    candidate_status: str = publication.READY,
    fail_on_pointer_update: bool = False,
    row_count_overrides: dict[str, int] | None = None,
):
    executed: list[tuple[str, tuple[object, ...]]] = []
    locks: list[tuple[str, tuple[object, ...]]] = []
    overrides = row_count_overrides or {}

    def fake_fetchone(_connection, sql, parameters):
        locks.append((sql, parameters))
        if "active_recommendation_generation" in sql:
            return ("previous_generation",)
        if parameters == ("previous_generation",):
            return (publication.ACTIVE,)
        return (candidate_status, object())

    def fake_execute(_connection, sql, parameters=()):
        executed.append((sql, parameters))
        if fail_on_pointer_update and "UPDATE public.active_recommendation_generation" in sql:
            raise RuntimeError("simulated pointer failure")
        if "SET status = 'SUPERSEDED'" in sql:
            return overrides.get("previous", 1)
        if "published_at = NOW()" in sql:
            return overrides.get("candidate", 1)
        if "UPDATE public.active_recommendation_generation" in sql:
            return overrides.get("pointer", 1)
        return 1

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)
    monkeypatch.setattr(publication, "_component_records", lambda *_args: complete_records())
    monkeypatch.setattr(publication, "_execute", fake_execute)
    return executed, locks


def test_publication_accepts_only_ready_generation(monkeypatch) -> None:
    connection = FakeConnection()
    install_publication_fakes(monkeypatch, candidate_status=publication.BUILDING)

    with pytest.raises(publication.ModelPublicationError, match="READY"):
        publication.publish_generation(connection, GENERATION_ID)
    assert connection.rollbacks == 1


def test_publication_changes_pointer_and_statuses_in_one_commit(monkeypatch) -> None:
    connection = FakeConnection()
    executed, locks = install_publication_fakes(monkeypatch)

    result = publication.publish_generation(connection, GENERATION_ID)

    assert result == {
        "generation_id": GENERATION_ID,
        "previous_generation_id": "previous_generation",
        "status": publication.ACTIVE,
    }
    assert any("status = 'SUPERSEDED'" in sql for sql, _ in executed)
    assert any("status = 'ACTIVE'" in sql for sql, _ in executed)
    assert any("active_recommendation_generation" in sql for sql, _ in executed)
    lock_parameters = [parameters for _sql, parameters in locks]
    assert lock_parameters == [(), ("previous_generation",), (GENERATION_ID,)]
    supersede_index = next(
        index for index, (sql, _parameters) in enumerate(executed)
        if "SET status = 'SUPERSEDED'" in sql
    )
    activate_index = next(
        index for index, (sql, _parameters) in enumerate(executed)
        if "published_at = NOW()" in sql
    )
    pointer_index = next(
        index for index, (sql, _parameters) in enumerate(executed)
        if "UPDATE public.active_recommendation_generation" in sql
    )
    assert supersede_index < activate_index < pointer_index
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_publication_exception_rolls_back_pointer_and_statuses(monkeypatch) -> None:
    connection = FakeConnection()
    install_publication_fakes(monkeypatch, fail_on_pointer_update=True)

    with pytest.raises(RuntimeError, match="pointer failure"):
        publication.publish_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize("row_name", ["previous", "candidate", "pointer"])
def test_publication_row_count_mismatch_causes_rollback(monkeypatch, row_name) -> None:
    connection = FakeConnection()
    install_publication_fakes(monkeypatch, row_count_overrides={row_name: 0})

    with pytest.raises(publication.ModelPublicationError):
        publication.publish_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_publication_rejects_candidate_already_at_active_pointer(monkeypatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(
        publication,
        "_fetchone",
        lambda _connection, _sql, _parameters: (GENERATION_ID,),
    )

    with pytest.raises(publication.ModelPublicationError, match="already"):
        publication.publish_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize("current_row", [None, (publication.READY,)])
def test_publication_rejects_inconsistent_active_pointer(monkeypatch, current_row) -> None:
    connection = FakeConnection()

    def fake_fetchone(_connection, sql, parameters):
        if "active_recommendation_generation" in sql:
            return ("previous_generation",)
        if parameters == ("previous_generation",):
            return current_row
        return (publication.READY, object())

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)

    with pytest.raises(publication.ModelPublicationError, match="Active pointer"):
        publication.publish_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def install_rollback_fakes(
    monkeypatch,
    *,
    target_status: str = publication.SUPERSEDED,
    validation_report: object = DEFAULT_REPORT,
    validated_at: object = "validated_at",
    published_at: object = "original_published_at",
    original_previous: str | None = "original_predecessor",
    fail_on: str | None = None,
    row_count_overrides: dict[str, int] | None = None,
):
    report = (
        {"valid": True}
        if validation_report is DEFAULT_REPORT
        else validation_report
    )
    executed: list[tuple[str, tuple[object, ...]]] = []
    locks: list[tuple[str, tuple[object, ...]]] = []
    overrides = row_count_overrides or {}

    def fake_fetchone(_connection, sql, parameters):
        locks.append((sql, parameters))
        if "active_recommendation_generation" in sql:
            return ("current_generation",)
        if parameters == ("current_generation",):
            return (publication.ACTIVE,)
        return (
            target_status,
            validated_at,
            report,
            published_at,
            original_previous,
        )

    def fake_execute(_connection, sql, parameters=()):
        executed.append((sql, parameters))
        if fail_on and fail_on in sql:
            raise RuntimeError("simulated rollback mutation failure")
        if "SET status = 'SUPERSEDED'" in sql:
            return overrides.get("current", 1)
        if "SET status = 'ACTIVE'" in sql:
            return overrides.get("target", 1)
        if "UPDATE public.active_recommendation_generation" in sql:
            return overrides.get("pointer", 1)
        if "recommendation_generation_rollbacks" in sql:
            return overrides.get("audit", 1)
        return 1

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)
    monkeypatch.setattr(publication, "_component_records", lambda *_args: complete_records())
    monkeypatch.setattr(publication, "_execute", fake_execute)
    return executed, locks


def test_rollback_switches_to_complete_validated_generation(monkeypatch) -> None:
    connection = FakeConnection()
    executed, locks = install_rollback_fakes(monkeypatch)

    result = publication.rollback_generation(
        connection,
        GENERATION_ID,
        reason="acceptance rollback",
    )

    assert result["generation_id"] == GENERATION_ID
    assert result["previous_active_generation_id"] == "current_generation"
    assert [parameters for _sql, parameters in locks] == [
        (),
        ("current_generation",),
        (GENERATION_ID,),
    ]
    current_index = next(
        index for index, (sql, _parameters) in enumerate(executed)
        if "SET status = 'SUPERSEDED'" in sql
    )
    target_index = next(
        index for index, (sql, _parameters) in enumerate(executed)
        if "SET status = 'ACTIVE'" in sql
    )
    pointer_index = next(
        index for index, (sql, _parameters) in enumerate(executed)
        if "UPDATE public.active_recommendation_generation" in sql
    )
    audit_index = next(
        index for index, (sql, _parameters) in enumerate(executed)
        if "recommendation_generation_rollbacks" in sql
    )
    assert current_index < target_index < pointer_index < audit_index
    target_sql = executed[target_index][0]
    assert "published_at" not in target_sql
    assert "previous_generation_id" not in target_sql
    audit_parameters = executed[audit_index][1]
    audit_metadata = json.loads(audit_parameters[3])
    assert audit_metadata["target_original_published_at"] == "original_published_at"
    assert audit_metadata["target_original_previous_generation_id"] == "original_predecessor"
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_rollback_accepts_valid_json_object_report(monkeypatch) -> None:
    connection = FakeConnection()
    install_rollback_fakes(monkeypatch, validation_report='{"valid": true}')

    publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 1
    assert connection.rollbacks == 0


@pytest.mark.parametrize(
    "current_row",
    [None, (publication.READY,), (publication.SUPERSEDED,), (publication.FAILED,)],
)
def test_rollback_rejects_missing_or_non_active_pointer_target(
    monkeypatch,
    current_row,
) -> None:
    connection = FakeConnection()

    def fake_fetchone(_connection, sql, parameters):
        if "active_recommendation_generation" in sql:
            return ("current_generation",)
        if parameters == ("current_generation",):
            return current_row
        pytest.fail("Rollback target must not be locked after pointer inconsistency")

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)

    with pytest.raises(publication.ModelPublicationError, match="Active pointer"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_rollback_rejects_target_without_original_publication_timestamp(
    monkeypatch,
) -> None:
    connection = FakeConnection()
    install_rollback_fakes(monkeypatch, published_at=None)

    with pytest.raises(publication.ModelPublicationError, match="previously published"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "target_status",
    [publication.READY, publication.FAILED, publication.BUILDING, publication.ACTIVE],
)
def test_rollback_rejects_every_non_superseded_target(monkeypatch, target_status) -> None:
    connection = FakeConnection()
    install_rollback_fakes(monkeypatch, target_status=target_status)

    with pytest.raises(publication.ModelPublicationError, match="SUPERSEDED"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_rollback_rejects_missing_target(monkeypatch) -> None:
    connection = FakeConnection()

    def fake_fetchone(_connection, sql, parameters):
        if "active_recommendation_generation" in sql:
            return ("current_generation",)
        if parameters == ("current_generation",):
            return (publication.ACTIVE,)
        return None

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)

    with pytest.raises(publication.ModelPublicationError, match="SUPERSEDED"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_rollback_rejects_target_already_active(monkeypatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(
        publication,
        "_fetchone",
        lambda _connection, sql, _parameters: (
            (GENERATION_ID,)
            if "active_recommendation_generation" in sql
            else (publication.ACTIVE,)
        ),
    )

    with pytest.raises(publication.ModelPublicationError, match="already active"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "validation_report",
    [
        {"valid": False, "errors": ["invalid"]},
        '{"valid": false}',
        "{malformed-json",
        [],
        "not-an-object",
        {},
        None,
    ],
)
def test_rollback_rejects_invalid_or_malformed_validation_report(
    monkeypatch,
    validation_report,
) -> None:
    connection = FakeConnection()
    install_rollback_fakes(monkeypatch, validation_report=validation_report)

    with pytest.raises(publication.ModelPublicationError, match="validated"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_rollback_rejects_incomplete_required_components(monkeypatch) -> None:
    connection = FakeConnection()
    install_rollback_fakes(monkeypatch)
    records = complete_records()
    records.pop("item_based")
    monkeypatch.setattr(publication, "_component_records", lambda *_args: records)

    with pytest.raises(publication.ModelPublicationError, match="not complete"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_rollback_exception_rolls_back_without_commit(monkeypatch) -> None:
    connection = FakeConnection()
    install_rollback_fakes(monkeypatch, fail_on="SET status = 'ACTIVE'")

    with pytest.raises(RuntimeError, match="rollback mutation failure"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize("row_name", ["current", "target", "pointer", "audit"])
def test_rollback_row_count_mismatch_causes_transaction_rollback(
    monkeypatch,
    row_name,
) -> None:
    connection = FakeConnection()
    install_rollback_fakes(monkeypatch, row_count_overrides={row_name: 0})

    with pytest.raises(publication.ModelPublicationError, match="exactly one row"):
        publication.rollback_generation(connection, GENERATION_ID)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_generation_ids_are_bound_not_interpolated_into_sql() -> None:
    source = (
        REPOSITORY_ROOT / "docker" / "scripts" / "model_publication.py"
    ).read_text(encoding="utf-8")
    unsafe_sql = re.compile(
        r"(?:SELECT|INSERT|UPDATE|DELETE)[\s\S]{0,300}\{generation_id\}",
        re.IGNORECASE,
    )
    assert unsafe_sql.search(source) is None
    assert "generation_id = %s" in source
    assert "generation_id = ?" in source


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
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_recommendation_generations_one_active" in migration
    assert "WHERE status = 'ACTIVE'" in migration


@pytest.mark.parametrize(
    ("cli_module", "arguments"),
    [
        (validate_cli, SimpleNamespace(generation_id=None)),
        (publish_cli, SimpleNamespace(generation_id=None)),
        (
            rollback_cli,
            SimpleNamespace(generation_id="unsafe/generation", reason=None),
        ),
    ],
)
def test_cli_rejects_invalid_generation_before_opening_database(
    monkeypatch,
    cli_module,
    arguments,
) -> None:
    monkeypatch.setattr(cli_module, "parse_args", lambda: arguments)
    monkeypatch.setattr(
        cli_module,
        "open_postgres_connection",
        lambda: pytest.fail("Database connection must not open for invalid input"),
    )

    with pytest.raises(ValueError):
        cli_module.main()
