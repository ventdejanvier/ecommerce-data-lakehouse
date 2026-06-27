from __future__ import annotations

import logging
import os

from recommendation_scoring import finite_float, normalize_category

try:
    import redis
except ImportError:  # pragma: no cover - dependency is installed in the backend runtime.
    redis = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)
SESSION_CATEGORY_TTL_SECONDS = 1800


def resolve_redis_host() -> str:
    env_host = os.getenv("REDIS_HOST")
    if env_host:
        return env_host
    # Instant check for Docker container vs Local Windows
    if os.path.exists("/.dockerenv"):
        return "redis"
    return "127.0.0.1"


r = (
    redis.Redis(
        host=resolve_redis_host(),
        port=6379,
        db=0,
        decode_responses=True,
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
        health_check_interval=30,
    )
    if redis is not None
    else None
)


def get_recent_categories_key(user_id: str) -> str:
    normalized_user_id = str(user_id).strip() if user_id is not None else ""
    if not normalized_user_id:
        raise ValueError("user_id must contain a non-whitespace value")
    return f"recent_categories:{normalized_user_id}"


def increment_category_score(user_id: str, category: str, score: float) -> None:
    normalized_user_id = str(user_id).strip() if user_id is not None else ""
    canonical_category = normalize_category(category)
    finite_score = finite_float(score)
    if not normalized_user_id or not canonical_category or finite_score is None or r is None:
        return

    try:
        key = get_recent_categories_key(normalized_user_id)
        r.zincrby(key, finite_score, canonical_category)
        r.expire(key, SESSION_CATEGORY_TTL_SECONDS)
        logger.info(
            "Incremented category score in Redis: key=%s category=%s score=%s",
            key,
            canonical_category,
            finite_score,
        )
    except redis.RedisError as exc:
        logger.warning("Redis unavailable while incrementing category score: %s", exc)


def get_category_scores(user_id: str) -> dict[str, float]:
    normalized_user_id = str(user_id).strip() if user_id is not None else ""
    if not normalized_user_id or r is None:
        return {}

    try:
        raw_scores = r.zrevrange(get_recent_categories_key(normalized_user_id), 0, -1, withscores=True)
        return {category: float(score) for category, score in raw_scores}
    except redis.RedisError as exc:
        logger.warning("Redis unavailable while reading category scores: %s", exc)
        return {}
