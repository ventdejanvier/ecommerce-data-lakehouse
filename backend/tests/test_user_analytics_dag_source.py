from __future__ import annotations

import ast
from pathlib import Path


DAG_PATH = (
    Path(__file__).resolve().parents[2]
    / "docker"
    / "dags"
    / "user_analytics_dag.py"
)


def test_user_analytics_dag_is_import_safe_at_ast_level_and_manual_only() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(DAG_PATH))

    assert tree is not None
    assert "schedule_interval=None" in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source


def test_all_spark_tasks_remain_attached_to_one_slot_pool() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    spark_task_variables = {
        "run_kmeans_task",
        "generate_recommendations_task",
        "export_als_task",
        "export_content_based_task",
        "export_item_based_task",
    }
    tree = ast.parse(source)
    task_calls = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and isinstance(node.value, ast.Call):
            task_calls[target.id] = node.value

    for task_name in spark_task_variables:
        call = task_calls[task_name]
        keyword_values = {keyword.arg: keyword.value for keyword in call.keywords}
        assert ast.literal_eval(keyword_values["pool"]) == "spark_heavy"
        assert "execution_timeout" in keyword_values


def test_generation_xcom_is_shared_and_publish_is_all_success() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")

    assert "ti.xcom_pull(task_ids='create_generation')" in source
    assert "-e MODEL_PUBLICATION_V2=true" in source
    assert "-e MODEL_GENERATION_ID=" in source
    assert source.count("V2_DOCKER_ENV") == 5
    assert "trigger_rule=TriggerRule.ALL_SUCCESS" in source
    assert "create_generation_task" in source
    assert ">> validate_generation_task" in source
    assert ">> publish_generation_task" in source
