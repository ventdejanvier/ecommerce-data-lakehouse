from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPOSITORY_ROOT / "docker" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import fail_recommendation_generation as fail_cli  # noqa: E402
import model_publication as publication  # noqa: E402


GENERATION_ID = "generation_20260628_batch2"
EXISTING_GENERATION_ID = "generation_20260628_existing"
AIRFLOW_RUN_ID = "manual__2026-06-28T10:00:00+00:00"
MIGRATION_PATH = (
    REPOSITORY_ROOT
    / "docker"
    / "migrations"
    / "001_atomic_model_publication_v2.sql"
)
DAG_PATH = REPOSITORY_ROOT / "docker" / "dags" / "user_analytics_dag.py"
EXPORTER_PATHS = (
    SCRIPTS_DIR / "generate_cluster_recommendations.py",
    SCRIPTS_DIR / "export_als_recommendations.py",
    SCRIPTS_DIR / "export_content_based.py",
    SCRIPTS_DIR / "export_item_based.py",
)


class FakeConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_same_airflow_run_returns_existing_building_generation(monkeypatch) -> None:
    connection = FakeConnection()
    calls: list[tuple[str, tuple[object, ...]]] = []

    def fake_fetchone(_connection, sql, parameters):
        calls.append((sql, parameters))
        if "INSERT INTO" in sql:
            return None
        return (EXISTING_GENERATION_ID, publication.BUILDING)

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)

    result = publication.create_generation(
        connection,
        airflow_run_id=f"  {AIRFLOW_RUN_ID}  ",
        generation_id=GENERATION_ID,
    )

    assert result == EXISTING_GENERATION_ID
    assert len(calls) == 2
    assert "ON CONFLICT (airflow_run_id)" in calls[0][0]
    assert "FOR UPDATE" in calls[1][0]
    assert calls[0][1][1] == AIRFLOW_RUN_ID
    assert calls[1][1] == (AIRFLOW_RUN_ID,)
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_new_airflow_run_inserts_one_building_generation(monkeypatch) -> None:
    connection = FakeConnection()
    calls: list[tuple[str, tuple[object, ...]]] = []

    def fake_fetchone(_connection, sql, parameters):
        calls.append((sql, parameters))
        return (GENERATION_ID, publication.BUILDING)

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)

    result = publication.create_generation(
        connection,
        airflow_run_id=AIRFLOW_RUN_ID,
        generation_id=GENERATION_ID,
    )

    assert result == GENERATION_ID
    assert len(calls) == 1
    assert "RETURNING generation_id, status" in calls[0][0]
    assert connection.commits == 1


def test_generation_without_airflow_run_preserves_uuid_insert_behavior(monkeypatch) -> None:
    connection = FakeConnection()
    executed = []
    monkeypatch.setattr(
        publication,
        "_execute",
        lambda _connection, sql, parameters=(): executed.append((sql, parameters)) or 1,
    )

    result = publication.create_generation(
        connection,
        airflow_run_id="  ",
        generation_id=GENERATION_ID,
    )

    assert result == GENERATION_ID
    assert len(executed) == 1
    assert "VALUES (%s, NULL, 'BUILDING'" in executed[0][0]
    assert executed[0][1][0] == GENERATION_ID
    assert connection.commits == 1


@pytest.mark.parametrize(
    "status",
    [publication.READY, publication.ACTIVE, publication.SUPERSEDED, publication.FAILED],
)
def test_airflow_run_cannot_create_replacement_for_non_building_generation(
    monkeypatch,
    status,
) -> None:
    connection = FakeConnection()
    rows = iter((None, (EXISTING_GENERATION_ID, status)))
    monkeypatch.setattr(publication, "_fetchone", lambda *_args: next(rows))

    with pytest.raises(publication.ModelPublicationError, match=status):
        publication.create_generation(
            connection,
            airflow_run_id=AIRFLOW_RUN_ID,
            generation_id=GENERATION_ID,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_airflow_run_uniqueness_is_database_enforced_and_idempotent() -> None:
    migration = MIGRATION_PATH.read_text(encoding="utf-8")

    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in migration
    assert "uq_recommendation_generations_airflow_run" in migration
    assert "ON public.recommendation_generations (airflow_run_id)" in migration
    assert "WHERE airflow_run_id IS NOT NULL" in migration


class FakeWriter:
    def __init__(self) -> None:
        self.options: dict[str, str] = {}
        self.format_name = None
        self.mode_name = None
        self.saved = False

    def format(self, value):
        self.format_name = value
        return self

    def option(self, name, value):
        self.options[name] = value
        return self

    def mode(self, value):
        self.mode_name = value
        return self

    def save(self):
        self.saved = True


class FakeDataFrameForWriter:
    def __init__(self) -> None:
        self.write = FakeWriter()


def test_shared_jdbc_resolver_drives_writer_and_metadata_connection(monkeypatch) -> None:
    environment = {
        "MODEL_PUBLICATION_JDBC_URL": "jdbc:postgresql://db.example:5432/models",
        "MODEL_PUBLICATION_DB_USER": "publisher",
        "MODEL_PUBLICATION_DB_PASSWORD": "secret-value",
        "MODEL_PUBLICATION_JDBC_DRIVER": "org.postgresql.Driver",
    }
    dataframe = FakeDataFrameForWriter()
    publication.write_dataframe_to_postgres(
        dataframe,
        "public.target",
        "append",
        environment,
    )

    connection = object()
    driver_calls = []
    connection_calls = []

    class FakeClass:
        @staticmethod
        def forName(driver):
            driver_calls.append(driver)

    class FakeDriverManager:
        @staticmethod
        def getConnection(url, user, password):
            connection_calls.append((url, user, password))
            return connection

    fake_jvm = SimpleNamespace(
        java=SimpleNamespace(
            lang=SimpleNamespace(Class=FakeClass),
            sql=SimpleNamespace(DriverManager=FakeDriverManager),
        )
    )
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(
            _gateway=SimpleNamespace(jvm=fake_jvm),
        )
    )

    assert publication._jdbc_connection(spark, environment) is connection
    assert dataframe.write.options == {
        "url": environment["MODEL_PUBLICATION_JDBC_URL"],
        "dbtable": "public.target",
        "user": environment["MODEL_PUBLICATION_DB_USER"],
        "password": environment["MODEL_PUBLICATION_DB_PASSWORD"],
        "driver": environment["MODEL_PUBLICATION_JDBC_DRIVER"],
    }
    assert connection_calls == [
        (
            dataframe.write.options["url"],
            dataframe.write.options["user"],
            dataframe.write.options["password"],
        )
    ]
    assert driver_calls == [dataframe.write.options["driver"]]
    assert "secret-value" not in repr(publication.resolve_jdbc_config(environment))


def test_exporters_have_no_independent_jdbc_destination_or_password() -> None:
    for path in EXPORTER_PATHS:
        source = path.read_text(encoding="utf-8")
        assert "POSTGRES_URL" not in source
        assert "POSTGRES_PROPERTIES" not in source
        assert "jdbc:postgresql://" not in source
        assert '.option("password"' not in source
        assert "write_dataframe_to_postgres" in source


def test_database_guard_protects_all_version_tables_and_operations() -> None:
    migration = MIGRATION_PATH.read_text(encoding="utf-8")
    protected_tables = {
        "serving_user_clusters_versions",
        "serving_recommendations_versions",
        "serving_als_versions",
        "serving_content_based_versions",
        "serving_item_based_versions",
    }

    assert "guard_building_generation_mutation" in migration
    assert protected_tables.issubset(set(part.strip("'\n ,") for part in migration.split()))
    assert "BEFORE INSERT OR UPDATE OR DELETE" in migration
    assert "TG_OP = 'INSERT'" in migration
    assert "TG_OP = 'UPDATE'" in migration
    assert "TG_OP = 'DELETE'" in migration
    assert "IS DISTINCT FROM 'BUILDING'" in migration
    assert migration.count("FOR SHARE") >= 4
    assert "DROP TRIGGER" not in migration
    assert "DROP FUNCTION" not in migration


def install_failure_fakes(monkeypatch, status, *, stored_run_id=AIRFLOW_RUN_ID):
    connection = FakeConnection()
    executed: list[tuple[str, tuple[object, ...]]] = []
    monkeypatch.setattr(
        publication,
        "_fetchone",
        lambda *_args: (GENERATION_ID, status, stored_run_id),
    )
    monkeypatch.setattr(
        publication,
        "_execute",
        lambda _connection, sql, parameters=(): executed.append((sql, parameters)) or 1,
    )
    return connection, executed


def test_failure_finalization_atomically_marks_building_failed(monkeypatch) -> None:
    connection, executed = install_failure_fakes(monkeypatch, publication.BUILDING)

    result = publication.fail_generation(
        connection,
        GENERATION_ID,
        reason="export failed",
        metadata={"task_id": "export_als_recommendations"},
        airflow_run_id=AIRFLOW_RUN_ID,
    )

    assert result == {
        "generation_id": GENERATION_ID,
        "status": publication.FAILED,
        "already_failed": False,
    }
    assert len(executed) == 1
    sql, parameters = executed[0]
    assert "failed_at = NOW()" in sql
    assert "failure_report" in sql
    assert "active_recommendation_generation" not in sql
    report = json.loads(parameters[0])
    assert report["reason"] == "export failed"
    assert report["metadata"]["task_id"] == "export_als_recommendations"
    assert parameters[1] == GENERATION_ID
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_repeated_failure_finalization_is_idempotent(monkeypatch) -> None:
    connection, executed = install_failure_fakes(monkeypatch, publication.FAILED)

    result = publication.fail_generation(
        connection,
        GENERATION_ID,
        reason="retry",
        airflow_run_id=AIRFLOW_RUN_ID,
    )

    assert result["already_failed"] is True
    assert executed == []
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_failure_finalization_recovers_generation_by_airflow_run(monkeypatch) -> None:
    connection, executed = install_failure_fakes(monkeypatch, publication.BUILDING)
    queries = []

    def fake_fetchone(_connection, sql, parameters):
        queries.append((sql, parameters))
        return (GENERATION_ID, publication.BUILDING, AIRFLOW_RUN_ID)

    monkeypatch.setattr(publication, "_fetchone", fake_fetchone)

    result = publication.fail_generation(
        connection,
        None,
        reason="XCom output was unavailable",
        airflow_run_id=AIRFLOW_RUN_ID,
    )

    assert result["generation_id"] == GENERATION_ID
    assert "WHERE airflow_run_id = %s" in queries[0][0]
    assert "FOR UPDATE" in queries[0][0]
    assert queries[0][1] == (AIRFLOW_RUN_ID,)
    assert len(executed) == 1


def test_failure_finalization_rejects_generation_from_another_run(monkeypatch) -> None:
    connection, executed = install_failure_fakes(
        monkeypatch,
        publication.BUILDING,
        stored_run_id="manual__different-run",
    )

    with pytest.raises(publication.ModelPublicationError, match="does not belong"):
        publication.fail_generation(
            connection,
            GENERATION_ID,
            reason="wrong owner",
            airflow_run_id=AIRFLOW_RUN_ID,
        )

    assert executed == []
    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize(
    "status",
    [publication.READY, publication.ACTIVE, publication.SUPERSEDED],
)
def test_failure_finalization_rejects_non_building_lifecycle(monkeypatch, status) -> None:
    connection, executed = install_failure_fakes(monkeypatch, status)

    with pytest.raises(publication.ModelPublicationError, match="BUILDING"):
        publication.fail_generation(
            connection,
            GENERATION_ID,
            reason="must fail",
            airflow_run_id=AIRFLOW_RUN_ID,
        )

    assert executed == []
    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_failure_cli_validates_recovery_identity_before_database(monkeypatch) -> None:
    monkeypatch.setattr(
        fail_cli,
        "parse_args",
        lambda: SimpleNamespace(
            generation_id="",
            airflow_run_id=" ",
            reason="failure",
            metadata_json="{}",
        ),
    )
    monkeypatch.setattr(
        fail_cli,
        "open_postgres_connection",
        lambda: pytest.fail("database connection must not open"),
    )

    with pytest.raises(ValueError, match="required"):
        fail_cli.main()


@pytest.mark.parametrize(
    ("columns", "expected_form", "expected_mapping"),
    [
        (
            ["score", "extra", "product_id", "user_id"],
            "user_level",
            {"user_id": "user_id", "product_id": "product_id", "score": "score"},
        ),
        (
            ["recommended_product_id", "score", "source_product_id", "rank"],
            "item_to_item",
            {
                "source_product_id": "source_product_id",
                "recommended_product_id": "recommended_product_id",
                "score": "score",
            },
        ),
        (
            ["content_score", "rank", "product_id_2", "product_id_1"],
            "item_to_item_verified_aliases",
            {
                "source_product_id": "product_id_1",
                "recommended_product_id": "product_id_2",
                "score": "content_score",
            },
        ),
    ],
)
def test_content_v2_schema_resolution_is_explicit_and_order_independent(
    columns,
    expected_form,
    expected_mapping,
) -> None:
    form, mapping = publication.resolve_content_v2_schema(columns)
    assert form == expected_form
    assert mapping == expected_mapping


@pytest.mark.parametrize(
    "columns",
    [
        ["user_id", "product_id"],
        ["product_id_1", "product_id_2"],
        ["user_id", "product_id", "score", "source_product_id"],
        [
            "user_id",
            "product_id",
            "score",
            "source_product_id",
            "recommended_product_id",
        ],
    ],
)
def test_content_v2_rejects_missing_mixed_and_ambiguous_shapes(columns) -> None:
    with pytest.raises(ValueError):
        publication.resolve_content_v2_schema(columns)


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        (
            ["score", "similar_product_id", "extra", "source_product_id"],
            {
                "source_product_id": "source_product_id",
                "similar_product_id": "similar_product_id",
                "score": "score",
            },
        ),
        (
            ["similarity", "product_id_2", "rank", "product_id_1"],
            {
                "source_product_id": "product_id_1",
                "similar_product_id": "product_id_2",
                "score": "similarity",
            },
        ),
    ],
)
def test_item_v2_schema_resolution_is_explicit_and_order_independent(
    columns,
    expected,
) -> None:
    assert publication.resolve_item_v2_schema(columns) == expected


@pytest.mark.parametrize(
    "columns",
    [
        ["source_product_id", "similar_product_id"],
        ["product_id_1", "product_id_2"],
        ["source_product_id", "similar_product_id", "score", "product_id_1"],
        [
            "source_product_id",
            "similar_product_id",
            "score",
            "product_id_1",
            "product_id_2",
            "similarity",
        ],
    ],
)
def test_item_v2_rejects_missing_mixed_and_ambiguous_shapes(columns) -> None:
    with pytest.raises(ValueError):
        publication.resolve_item_v2_schema(columns)


def test_v2_exporter_paths_do_not_use_positional_fallback() -> None:
    content = (SCRIPTS_DIR / "export_content_based.py").read_text(encoding="utf-8")
    item = (SCRIPTS_DIR / "export_item_based.py").read_text(encoding="utf-8")

    content_v2 = content.split("else:\n        generation_id = plan.generation_id", 1)[1]
    item_v2 = item.split("else:\n        generation_id = plan.generation_id", 1)[1]
    assert "cols[" not in content_v2
    assert "cols[" not in item_v2
    assert "resolve_content_v2_schema(cols)" in content_v2
    assert "resolve_item_v2_schema(cols)" in item_v2


class FakePersistedDataFrame:
    def __init__(self, row_count: int, events: list[str]) -> None:
        self.row_count = row_count
        self.events = events
        self.unpersisted = False

    def persist(self):
        self.events.append("persist")
        return self

    def count(self):
        self.events.append("count")
        return self.row_count

    def unpersist(self):
        self.events.append("unpersist")
        self.unpersisted = True


def install_stable_export_fakes(
    monkeypatch,
    events,
    *,
    actual_count,
    write_error=None,
):
    monkeypatch.setattr(
        publication,
        "prepare_component_retry_spark",
        lambda *_args: events.append("prepare"),
    )

    def fake_write(*_args):
        events.append("write")
        if write_error:
            raise write_error

    monkeypatch.setattr(publication, "write_dataframe_to_postgres", fake_write)
    monkeypatch.setattr(
        publication,
        "count_component_rows_spark",
        lambda *_args: events.append("post_write_count") or actual_count,
    )
    recorded = []

    def fake_record(*args):
        events.append("record")
        recorded.append(args)

    monkeypatch.setattr(publication, "record_component_complete_spark", fake_record)
    return recorded


def test_versioned_export_uses_one_persisted_snapshot_and_actual_count(monkeypatch) -> None:
    events: list[str] = []
    dataframe = FakePersistedDataFrame(7, events)
    recorded = install_stable_export_fakes(monkeypatch, events, actual_count=7)

    result = publication.export_versioned_component_spark(
        object(),
        dataframe,
        GENERATION_ID,
        "als",
        {"source": "gold_db.recommendations_als"},
    )

    assert result == 7
    assert events == [
        "persist",
        "count",
        "prepare",
        "write",
        "post_write_count",
        "record",
        "unpersist",
    ]
    assert recorded[0][3] == 7
    assert dataframe.unpersisted is True


def test_versioned_export_does_not_complete_after_write_failure(monkeypatch) -> None:
    events: list[str] = []
    dataframe = FakePersistedDataFrame(7, events)
    recorded = install_stable_export_fakes(
        monkeypatch,
        events,
        actual_count=7,
        write_error=RuntimeError("write failed"),
    )

    with pytest.raises(RuntimeError, match="write failed"):
        publication.export_versioned_component_spark(
            object(),
            dataframe,
            GENERATION_ID,
            "als",
            {"source": "source"},
        )

    assert recorded == []
    assert "post_write_count" not in events
    assert events[-1] == "unpersist"


def test_versioned_export_does_not_complete_after_count_mismatch(monkeypatch) -> None:
    events: list[str] = []
    dataframe = FakePersistedDataFrame(7, events)
    recorded = install_stable_export_fakes(monkeypatch, events, actual_count=6)

    with pytest.raises(publication.ModelPublicationError, match="does not match"):
        publication.export_versioned_component_spark(
            object(),
            dataframe,
            GENERATION_ID,
            "als",
            {"source": "source"},
        )

    assert recorded == []
    assert events[-1] == "unpersist"


def test_versioned_export_rejects_empty_snapshot_before_cleanup_or_write(
    monkeypatch,
) -> None:
    events: list[str] = []
    dataframe = FakePersistedDataFrame(0, events)
    recorded = install_stable_export_fakes(monkeypatch, events, actual_count=0)

    with pytest.raises(publication.ModelPublicationError, match="cannot be empty"):
        publication.export_versioned_component_spark(
            object(),
            dataframe,
            GENERATION_ID,
            "als",
            {"source": "source"},
        )

    assert recorded == []
    assert events == ["persist", "count", "unpersist"]


def test_cluster_top_k_is_deterministic_and_cleans_both_components_first() -> None:
    source = (SCRIPTS_DIR / "generate_cluster_recommendations.py").read_text(
        encoding="utf-8"
    )

    assert 'F.desc("cluster_total_score")' in source
    assert 'F.col("product_id").cast("string").asc()' in source
    cleanup_index = source.index("prepare_component_retry_spark(")
    recommendation_write_index = source.index(
        'export_versioned_component_spark(\n            spark,\n            versioned_recommendations'
    )
    cluster_write_index = source.index(
        'export_versioned_component_spark(\n            spark,\n            versioned_clusters'
    )
    assert cleanup_index < recommendation_write_index < cluster_write_index
    assert '("cluster_recommendations", "user_clusters")' in source


def test_dag_has_one_generation_and_safe_success_failure_branches() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(DAG_PATH))

    assert source.count("task_id='create_generation'") == 1
    assert "schedule_interval=None" in source
    assert "max_active_runs=1" in source
    assert "catchup=False" in source
    assert "TriggerRule.ONE_FAILED" in source
    assert "validate_generation_task >> failure_finalizer_task" in source
    assert ">> validate_generation_task" in source
    assert ">> publish_generation_task" in source
    assert "trigger_rule=TriggerRule.ALL_SUCCESS" in source
    assert '--generation-id "$MODEL_GENERATION_ID"' in source
    assert '--airflow-run-id "$MODEL_AIRFLOW_RUN_ID"' in source
    assert "password" not in source.lower()
    assert "docker exec -d" not in source
    assert "nohup" not in source
    assert tree is not None


def test_all_v2_exporters_receive_the_same_xcom_and_all_spark_tasks_use_pool() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    task_calls = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and isinstance(node.value, ast.Call):
            task_calls[target.id] = node.value

    spark_tasks = {
        "run_kmeans_task",
        "generate_recommendations_task",
        "export_als_task",
        "export_content_based_task",
        "export_item_based_task",
    }
    v2_tasks = spark_tasks - {"run_kmeans_task"}
    for task_name in spark_tasks:
        keywords = {keyword.arg: keyword.value for keyword in task_calls[task_name].keywords}
        assert ast.literal_eval(keywords["pool"]) == "spark_heavy"
    for task_name in v2_tasks:
        keywords = {keyword.arg: keyword.value for keyword in task_calls[task_name].keywords}
        assert isinstance(keywords["env"], ast.Name)
        assert keywords["env"].id == "GENERATION_TASK_ENV"

    assert "ti.xcom_pull(task_ids='create_generation') or ''" in source
    assert source.count("V2_DOCKER_ENV") == 5


def test_create_generation_cli_keeps_generation_id_as_final_output() -> None:
    source = (SCRIPTS_DIR / "create_recommendation_generation.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    print_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "print"
    ]
    assert len(print_calls) == 1
    assert isinstance(print_calls[0].args[0], ast.Name)
    assert print_calls[0].args[0].id == "generation_id"


def test_legacy_targets_and_overwrite_defaults_remain_present() -> None:
    expected_targets = {
        "generate_cluster_recommendations.py": (
            "serving_recommendations",
            "serving_user_clusters",
        ),
        "export_als_recommendations.py": ("public.serving_als",),
        "export_content_based.py": ("public.serving_content_based",),
        "export_item_based.py": ("public.serving_item_based",),
    }
    for file_name, targets in expected_targets.items():
        source = (SCRIPTS_DIR / file_name).read_text(encoding="utf-8")
        assert all(target in source for target in targets)
        assert 'mode: str = "overwrite"' in source
