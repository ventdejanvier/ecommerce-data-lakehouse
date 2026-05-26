from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main  # noqa: E402


client = TestClient(main.app)


def sample_recommendations() -> list[dict[str, object]]:
    return [
        {
            "cluster_id": 1,
            "product_id": 101,
            "display_name": "Demo Product",
            "cluster_total_score": 12.5,
        }
    ]


def test_health_check() -> None:
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_legacy_recommendations_endpoint_schema(monkeypatch) -> None:
    monkeypatch.setattr(
        main,
        "get_recommendations_from_db",
        lambda user_id: sample_recommendations(),
    )

    response = client.get("/api/recommendations/USER_001")

    assert response.status_code == 200
    assert response.json() == sample_recommendations()


def test_home_recommendations_endpoint_schema(monkeypatch) -> None:
    monkeypatch.setattr(
        main,
        "get_recommendations_from_db",
        lambda user_id: sample_recommendations(),
    )

    response = client.get("/api/recommend/home/USER_001")

    assert response.status_code == 200
    assert response.json() == sample_recommendations()


def test_track_endpoint_accepts_event_and_queues_kafka(monkeypatch) -> None:
    produced_events = []

    def fake_produce_event(topic: str, event: dict[str, object]) -> None:
        produced_events.append((topic, event))

    monkeypatch.setattr(main, "produce_event", fake_produce_event)

    payload = {
        "eventId": "evt_test_001",
        "timestamp": "2026-05-26T00:00:00Z",
        "sessionId": "session_test_001",
        "eventType": "page_view",
        "userId": "USER_001",
        "context": {
            "userAgent": "pytest",
            "url": "http://localhost:3000",
            "referrer": "",
        },
        "pageName": "homepage",
    }

    response = client.post("/api/track", json=payload)

    assert response.status_code == 202
    assert response.json() == {"status": "accepted", "eventId": "evt_test_001"}
    assert produced_events == [("ecommerce-raw-events", payload)]
