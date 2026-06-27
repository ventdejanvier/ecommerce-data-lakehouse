from __future__ import annotations

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import redis_client  # noqa: E402


class FakeRedis:
    def __init__(self) -> None:
        self.increments: list[tuple[str, float, str]] = []
        self.expirations: list[tuple[str, int]] = []

    def zincrby(self, key: str, score: float, member: str) -> None:
        self.increments.append((key, score, member))

    def expire(self, key: str, ttl_seconds: int) -> None:
        self.expirations.append((key, ttl_seconds))


def test_redis_writes_use_one_canonical_member_and_preserve_ttl(monkeypatch) -> None:
    fake_redis = FakeRedis()
    monkeypatch.setattr(redis_client, "r", fake_redis)

    redis_client.increment_category_score(" guest:session_1 ", "Home Appliances", 10)
    redis_client.increment_category_score("guest:session_1", "home_appliances", 5)
    redis_client.increment_category_score("guest:session_1", "home-appliances", -2)

    assert fake_redis.increments == [
        ("recent_categories:guest:session_1", 10.0, "home appliances"),
        ("recent_categories:guest:session_1", 5.0, "home appliances"),
        ("recent_categories:guest:session_1", -2.0, "home appliances"),
    ]
    assert fake_redis.expirations == [
        ("recent_categories:guest:session_1", 1800),
        ("recent_categories:guest:session_1", 1800),
        ("recent_categories:guest:session_1", 1800),
    ]


def test_redis_write_ignores_empty_categories_and_user_ids(monkeypatch) -> None:
    fake_redis = FakeRedis()
    monkeypatch.setattr(redis_client, "r", fake_redis)

    redis_client.increment_category_score("guest:session_1", "  ", 1)
    redis_client.increment_category_score("   ", "Electronics", 1)
    redis_client.increment_category_score("guest:session_1", 123, 1)
    redis_client.increment_category_score("guest:session_1", "Electronics", float("nan"))

    assert fake_redis.increments == []
    assert fake_redis.expirations == []


def test_recent_category_key_trims_and_preserves_valid_identifiers() -> None:
    assert (
        redis_client.get_recent_categories_key(" guest:session_1 ")
        == "recent_categories:guest:session_1"
    )
    assert (
        redis_client.get_recent_categories_key("1515915625355805313")
        == "recent_categories:1515915625355805313"
    )


@pytest.mark.parametrize("user_id", ["", "   ", None])
def test_recent_category_key_rejects_empty_identifiers(user_id) -> None:
    with pytest.raises(ValueError, match="user_id"):
        redis_client.get_recent_categories_key(user_id)
