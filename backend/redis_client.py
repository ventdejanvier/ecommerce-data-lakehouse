from __future__ import annotations

import logging
import os

try:
    import redis
except ImportError:  # pragma: no cover - dependency is installed in the backend runtime.
    redis = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)
SESSION_CATEGORY_TTL_SECONDS = 1800

r = (
    redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=6379,
        db=0,
        decode_responses=True,
    )
    if redis is not None
    else None
)


def add_recent_category(user_id: str, category: str) -> None:
    normalized_user_id = str(user_id).strip()
    normalized_category = str(category).strip()
    if not normalized_user_id or not normalized_category or r is None:
        return

    try:
        key = f"session:{normalized_user_id}:categories"
        r.sadd(key, normalized_category)
        r.expire(key, SESSION_CATEGORY_TTL_SECONDS)
    except redis.RedisError as exc:
        logger.warning("Redis unavailable while storing recent category: %s", exc)


def get_recent_categories(user_id: str) -> set[str]:
    normalized_user_id = str(user_id).strip()
    if not normalized_user_id or r is None:
        return set()

    try:
        return set(r.smembers(f"session:{normalized_user_id}:categories"))
    except redis.RedisError as exc:
        logger.warning("Redis unavailable while reading recent categories: %s", exc)
        return set()
