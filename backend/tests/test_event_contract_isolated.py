from __future__ import annotations

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main  # noqa: E402
from recommendation_scoring import RecommendationScoringConfig  # noqa: E402


class RecordingBackgroundTasks:
    def __init__(self) -> None:
        self.tasks = []

    def add_task(self, function, *args, **kwargs) -> None:
        self.tasks.append((function, args, kwargs))

    def run(self) -> None:
        for function, args, kwargs in self.tasks:
            function(*args, **kwargs)


def capture_category_updates(monkeypatch, payload: dict[str, object]):
    captured = []
    monkeypatch.setattr(
        main,
        "increment_category_score",
        lambda user_id, category, score: captured.append((user_id, category, score)),
    )
    main.capture_recent_category(payload)
    return captured


def test_purchase_completed_increments_two_distinct_categories_once(monkeypatch) -> None:
    captured = capture_category_updates(
        monkeypatch,
        {
            "eventType": "PURCHASE_COMPLETED",
            "userId": "USER_001",
            "items": [
                {"category_main": "Electronics", "quantity": 2},
                {"productCategory": "Home Appliances", "quantity": 3},
            ],
        },
    )
    assert captured == [
        ("USER_001", "electronics", 10.0),
        ("USER_001", "home appliances", 10.0),
    ]


def test_purchase_completed_deduplicates_same_category(monkeypatch) -> None:
    captured = capture_category_updates(
        monkeypatch,
        {
            "eventType": "PURCHASE_COMPLETED",
            "userId": "USER_001",
            "items": [
                {"category": "Home Appliances"},
                {"product_category": "home_appliances"},
            ],
        },
    )
    assert captured == [("USER_001", "home appliances", 10.0)]


def test_purchase_completed_ignores_malformed_items(monkeypatch) -> None:
    captured = capture_category_updates(
        monkeypatch,
        {
            "eventType": "PURCHASE_COMPLETED",
            "userId": "USER_001",
            "items": [None, "bad", {}, {"category": 123}, {"categoryName": "Valid"}],
        },
    )
    assert captured == [("USER_001", "valid", 10.0)]


def test_purchase_completed_without_categories_does_not_increment(monkeypatch) -> None:
    captured = capture_category_updates(
        monkeypatch,
        {
            "eventType": "PURCHASE_COMPLETED",
            "userId": "USER_001",
            "items": [{"category": " "}, None],
        },
    )
    assert captured == []


def test_legacy_purchase_with_top_level_category_still_increments(monkeypatch) -> None:
    captured = capture_category_updates(
        monkeypatch,
        {
            "eventType": "purchase",
            "userId": "USER_001",
            "category": "Electronics",
        },
    )
    assert captured == [("USER_001", "electronics", 10.0)]


def test_cart_remove_prefers_category_main(monkeypatch) -> None:
    captured = capture_category_updates(
        monkeypatch,
        {
            "eventType": "CART_UPDATE",
            "action": "remove",
            "userId": "USER_001",
            "category_main": "Electronics",
            "category": "Fallback",
        },
    )
    assert captured == [("USER_001", "electronics", -5.0)]


def test_track_handler_updates_redis_synchronously_and_queues_kafka(monkeypatch) -> None:
    produced_events = []
    redis_updates = []
    monkeypatch.setattr(
        main,
        "produce_event",
        lambda topic, event: produced_events.append((topic, event)),
    )
    monkeypatch.setattr(
        main,
        "increment_category_score",
        lambda user_id, category, score: redis_updates.append((user_id, category, score)),
    )
    payload = {
        "eventId": "purchase_001",
        "eventType": "PURCHASE_COMPLETED",
        "userId": "USER_001",
        "items": [
            {"productCategory": "Electronics", "quantity": 3},
            {"category_main": "Books", "quantity": 1},
        ],
    }

    background_tasks = RecordingBackgroundTasks()
    response = main.track_event(payload, background_tasks)

    assert response == {"status": "ok", "eventId": "purchase_001"}
    assert redis_updates == [
        ("USER_001", "electronics", 10.0),
        ("USER_001", "books", 10.0),
    ]
    assert produced_events == []
    assert background_tasks.tasks == [
        (main.send_to_kafka_background, (payload,), {}),
    ]

    background_tasks.run()

    assert produced_events == [("ecommerce-raw-events", payload)]


def test_track_handler_reaches_kafka_when_redis_fails(monkeypatch) -> None:
    produced_events = []

    def fail_redis_increment(user_id, category, score) -> None:
        raise RuntimeError("simulated Redis failure")

    monkeypatch.setattr(main, "increment_category_score", fail_redis_increment)
    monkeypatch.setattr(
        main,
        "produce_event",
        lambda topic, event: produced_events.append((topic, event)),
    )
    payload = {
        "eventId": "purchase_redis_failure",
        "eventType": "PURCHASE_COMPLETED",
        "userId": "USER_001",
        "items": [{"productCategory": "Electronics"}],
    }

    background_tasks = RecordingBackgroundTasks()
    response = main.track_event(payload, background_tasks)

    assert response == {"status": "ok", "eventId": "purchase_redis_failure"}
    assert produced_events == []
    assert background_tasks.tasks == [
        (main.send_to_kafka_background, (payload,), {}),
    ]

    background_tasks.run()

    assert produced_events == [("ecommerce-raw-events", payload)]


def test_home_recent_rerank_uses_normalized_scores_when_v2_flag_is_disabled(
    monkeypatch,
) -> None:
    products = [
        {
            "id": "101",
            "name": "Demo Product",
            "category": "Laptops",
            "cluster_total_score": 12.5,
        }
    ]
    monkeypatch.setattr(main, "SCORING_CONFIG", RecommendationScoringConfig(enabled=False))
    monkeypatch.setattr(main, "get_global_recommendations", lambda: products)
    monkeypatch.setattr(main, "get_category_scores", lambda user_id: {"Laptops": 5.0})
    monkeypatch.setattr(main, "get_recent_category_candidates", lambda *args, **kwargs: [])

    response = main.get_home_recommendations("USER_001", is_ml_enabled=False)

    assert response == [
        {
            "id": "101",
            "name": "Demo Product",
            "category": "Laptops",
            "cluster_total_score": 12.5,
            "reranked_score": 1.0,
        }
    ]


def test_home_recent_reranking_prioritizes_recent_intent_over_base_scale(
    monkeypatch,
) -> None:
    products = [
        {"id": "high-base", "category": "Penalty", "cluster_total_score": 100.0},
        {"id": "low-base", "category": "Boost", "cluster_total_score": 0.0},
    ]
    monkeypatch.setattr(
        main,
        "SCORING_CONFIG",
        RecommendationScoringConfig(
            enabled=True,
            base_weight=0.8,
            recent_weight=0.2,
            recent_temperature=1.0,
        ),
    )
    monkeypatch.setattr(main, "get_global_recommendations", lambda: products)
    monkeypatch.setattr(
        main,
        "get_category_scores",
        lambda user_id: {"Penalty": -1e100, "Boost": 1e100},
    )
    monkeypatch.setattr(main, "get_recent_category_candidates", lambda *args, **kwargs: [])

    response = main.get_home_recommendations("USER_001", is_ml_enabled=False)

    assert [item["id"] for item in response] == ["low-base", "high-base"]
    assert response[0]["reranked_score"] == pytest.approx(0.35)
    assert response[1]["reranked_score"] == pytest.approx(0.3)
