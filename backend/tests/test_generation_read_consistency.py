from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import database  # noqa: E402


class ConnectionContext:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False


class FakeEngine:
    def __init__(self) -> None:
        self.connection = object()
        self.connect_calls = 0

    def connect(self) -> ConnectionContext:
        self.connect_calls += 1
        return ConnectionContext(self.connection)


class EmptyScalarResult:
    def scalar_one_or_none(self):
        return None


class EmptyPointerConnection:
    def execute(self, _query):
        return EmptyScalarResult()


def test_v2_reads_active_generation_once_for_als_and_content(monkeypatch) -> None:
    fake_engine = FakeEngine()
    active_reads = []
    model_reads = []
    monkeypatch.setattr(database, "engine", fake_engine)
    monkeypatch.setattr(database, "RECOMMENDATION_GENERATION_READ_V2", True)
    monkeypatch.setattr(
        database,
        "_get_active_generation_id",
        lambda connection: active_reads.append(connection) or "generation_v2",
    )
    monkeypatch.setattr(
        database,
        "_fetch_user_recommendations_from_als",
        lambda connection, user_id, limit, generation_id=None: (
            model_reads.append(("als", generation_id))
            or [{"product_id": "1", "cluster_total_score": 2.0}]
        ),
    )
    monkeypatch.setattr(
        database,
        "_fetch_user_recommendations_from_content_based",
        lambda connection, user_id, limit, generation_id=None: (
            model_reads.append(("content", generation_id))
            or [{"product_id": "2", "cluster_total_score": 1.0}]
        ),
    )

    rows = database.get_recommendations_with_fallback("user_1")

    assert [row["id"] for row in rows] == ["1", "2"]
    assert model_reads == [("als", "generation_v2"), ("content", "generation_v2")]
    assert len(active_reads) == 1
    assert fake_engine.connect_calls == 1


def test_v2_cluster_fallback_pins_cluster_and_recommendation_reads(monkeypatch) -> None:
    fake_engine = FakeEngine()
    observed = []
    monkeypatch.setattr(database, "engine", fake_engine)
    monkeypatch.setattr(database, "RECOMMENDATION_GENERATION_READ_V2", True)
    monkeypatch.setattr(database, "_get_active_generation_id", lambda _connection: "generation_v2")
    monkeypatch.setattr(
        database,
        "_fetch_user_level_recommendations",
        lambda connection, user_id, limit, generation_id=None: [],
    )
    monkeypatch.setattr(
        database,
        "_fetch_user_cluster_id",
        lambda connection, user_id, generation_id=None: (
            observed.append(("cluster", generation_id)) or 3
        ),
    )
    monkeypatch.setattr(
        database,
        "_fetch_cluster_recommendations",
        lambda connection, cluster_id, limit, generation_id=None: (
            observed.append(("recommendations", generation_id))
            or [{"id": "10", "cluster_total_score": 4.0}]
        ),
    )

    rows = database.get_recommendations_with_fallback("user_1")

    assert rows == [{"id": "10", "cluster_total_score": 4.0}]
    assert observed == [
        ("cluster", "generation_v2"),
        ("recommendations", "generation_v2"),
    ]


def test_feature_flag_false_preserves_legacy_generation_arguments(monkeypatch) -> None:
    fake_engine = FakeEngine()
    observed = []
    monkeypatch.setattr(database, "engine", fake_engine)
    monkeypatch.setattr(database, "RECOMMENDATION_GENERATION_READ_V2", False)
    monkeypatch.setattr(
        database,
        "_get_active_generation_id",
        lambda _connection: (_ for _ in ()).throw(AssertionError("pointer must not be read")),
    )
    monkeypatch.setattr(
        database,
        "_fetch_user_level_recommendations",
        lambda connection, user_id, limit, generation_id=None: (
            observed.append(generation_id) or [{"id": "legacy"}]
        ),
    )

    rows = database.get_recommendations_with_fallback("user_1")

    assert rows == [{"id": "legacy"}]
    assert observed == [None]


def test_v2_without_active_generation_fails_clearly() -> None:
    try:
        database._get_active_generation_id(EmptyPointerConnection())
    except RuntimeError as exc:
        assert str(exc) == "No active recommendation generation is published"
    else:
        raise AssertionError("Missing active generation must not silently use legacy tables")
