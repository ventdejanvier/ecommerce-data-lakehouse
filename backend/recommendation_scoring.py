from __future__ import annotations

import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


DEFAULT_BASE_WEIGHT = 0.8
DEFAULT_RECENT_WEIGHT = 0.2
DEFAULT_RECENT_TEMPERATURE = 10.0
DEFAULT_ALS_WEIGHT = 0.5
DEFAULT_CONTENT_WEIGHT = 0.5
IDENTICAL_SCORE_NEUTRAL_VALUE = 0.5

RECENT_EVENT_WEIGHTS = {
    "view": 1.0,
    "product_view": 1.0,
    "product_click": 3.0,
    "add_to_cart": 5.0,
    "purchase": 10.0,
    "remove_from_cart": -5.0,
}
RECENT_CATEGORY_EVENT_TYPES = frozenset(
    {"product_click", "product_view", "add_to_cart", "purchase", "remove_from_cart"}
)
PURCHASE_ITEM_CATEGORY_FIELDS = (
    "category_main",
    "category",
    "productCategory",
    "product_category",
    "categoryName",
)
HOME_CANDIDATE_CATEGORY_FIELDS = (
    "category",
    "category_main",
    "category_sub",
    "category_detail",
    "category_name",
    "recent_match_category",
)


def finite_float(value: Any) -> float | None:
    """Return a finite float, or None for missing, invalid, NaN, or infinite input."""
    if value is None or isinstance(value, bool):
        return None

    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None

    return number if math.isfinite(number) else None


def min_max_normalize(
    values: Sequence[Any],
    *,
    neutral_value: float = IDENTICAL_SCORE_NEUTRAL_VALUE,
) -> list[float]:
    """Normalize valid values to [0, 1]; invalid values receive the safe floor 0.

    A single valid value, or a candidate set whose valid values are identical,
    receives ``neutral_value``. Empty input returns an empty list.
    """
    neutral = finite_float(neutral_value)
    if neutral is None or not 0.0 <= neutral <= 1.0:
        raise ValueError("neutral_value must be finite and within [0, 1]")

    converted = [finite_float(value) for value in values]
    valid_values = [value for value in converted if value is not None]
    if not valid_values:
        return [0.0 for _ in converted]

    minimum = min(valid_values)
    maximum = max(valid_values)
    if minimum == maximum:
        return [neutral if value is not None else 0.0 for value in converted]

    scale = maximum - minimum
    return [
        (value - minimum) / scale if value is not None else 0.0
        for value in converted
    ]


def normalize_weights(first_weight: Any, second_weight: Any) -> tuple[float, float]:
    """Validate two nonnegative weights and normalize them to sum to one."""
    first = finite_float(first_weight)
    second = finite_float(second_weight)
    if first is None or second is None:
        raise ValueError("weights must be finite numbers")
    if first < 0.0 or second < 0.0:
        raise ValueError("weights must be nonnegative")

    total = first + second
    if total <= 0.0:
        raise ValueError("at least one weight must be positive")
    return first / total, second / total


def bound_recent_signal(redis_score: Any, temperature: Any) -> float:
    """Bound a Redis score with tanh(score / temperature), without calibration claims."""
    parsed_temperature = finite_float(temperature)
    if parsed_temperature is None or parsed_temperature <= 0.0:
        raise ValueError("recent-signal temperature must be strictly positive")

    parsed_score = finite_float(redis_score)
    if parsed_score is None:
        return 0.0
    return math.tanh(parsed_score / parsed_temperature)


def fuse_scores(
    normalized_base_score: Any,
    recent_signal: Any,
    *,
    base_weight: Any,
    recent_weight: Any,
) -> float:
    """Fuse a [0, 1] base score and [-1, 1] recent signal.

    Weights are normalized to sum to one. The result is within
    ``[-normalized_recent_weight, 1]`` after defensive input clipping.
    """
    normalized_base = finite_float(normalized_base_score)
    bounded_recent = finite_float(recent_signal)
    normalized_base = min(max(normalized_base or 0.0, 0.0), 1.0)
    bounded_recent = min(max(bounded_recent or 0.0, -1.0), 1.0)
    normalized_base_weight, normalized_recent_weight = normalize_weights(
        base_weight,
        recent_weight,
    )
    return (
        normalized_base_weight * normalized_base
        + normalized_recent_weight * bounded_recent
    )


def stable_product_id(candidate: Mapping[str, Any]) -> str:
    """Return the stable product identifier used for merging and tie-breaking."""
    product_id = candidate.get("id")
    if product_id is None:
        product_id = candidate.get("product_id")
    if product_id is None:
        return ""
    return str(product_id).strip()


def _candidate_score(candidate: Mapping[str, Any]) -> Any:
    if "cluster_total_score" in candidate:
        return candidate.get("cluster_total_score")
    return candidate.get("score")


def _prepare_model_source(
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[dict[str, Any], float]]:
    usable = [candidate for candidate in candidates if stable_product_id(candidate)]
    normalized_scores = min_max_normalize(
        [_candidate_score(candidate) for candidate in usable]
    )

    prepared: dict[str, tuple[dict[str, Any], float]] = {}
    for candidate, normalized_score in zip(usable, normalized_scores, strict=True):
        product_id = stable_product_id(candidate)
        current = prepared.get(product_id)
        if current is None or normalized_score > current[1]:
            prepared[product_id] = (dict(candidate), normalized_score)
    return prepared


def blend_model_candidates(
    als_candidates: Sequence[Mapping[str, Any]],
    content_candidates: Sequence[Mapping[str, Any]],
    *,
    als_weight: Any,
    content_weight: Any,
    limit: int,
) -> list[dict[str, Any]]:
    """Blend independently normalized ALS and content candidate sets.

    Candidates missing from one source receive a zero contribution from that
    source. Results are ordered by blended score descending and product ID
    ascending. Candidates with missing/invalid scores remain safely at that
    source's zero floor.
    """
    if limit <= 0:
        return []

    normalized_als_weight, normalized_content_weight = normalize_weights(
        als_weight,
        content_weight,
    )
    als_by_id = _prepare_model_source(als_candidates)
    content_by_id = _prepare_model_source(content_candidates)

    blended: list[dict[str, Any]] = []
    for product_id in sorted(set(als_by_id) | set(content_by_id)):
        als_entry = als_by_id.get(product_id)
        content_entry = content_by_id.get(product_id)
        source_candidate = als_entry[0] if als_entry is not None else content_entry[0]
        result = dict(source_candidate)
        if content_entry is not None:
            for key, value in content_entry[0].items():
                result.setdefault(key, value)

        als_score = als_entry[1] if als_entry is not None else 0.0
        content_score = content_entry[1] if content_entry is not None else 0.0
        result["cluster_total_score"] = (
            normalized_als_weight * als_score
            + normalized_content_weight * content_score
        )
        blended.append(result)

    blended.sort(
        key=lambda candidate: (
            -float(candidate["cluster_total_score"]),
            stable_product_id(candidate),
        )
    )
    return blended[:limit]


def normalize_category(value: Any) -> str:
    """Return the canonical Redis category member for a string category.

    Non-string values are malformed under the event contract and normalize to
    an empty value so callers can safely ignore them.
    """
    if not isinstance(value, str):
        return ""
    normalized = value.strip().lower()
    normalized = normalized.replace("_", " ").replace("-", " ")
    return " ".join(normalized.split())


def aggregate_category_scores(
    category_scores: Mapping[Any, Any],
) -> dict[str, float]:
    """Aggregate finite alias scores into deterministic canonical totals."""
    scores_by_category: dict[str, list[float]] = {}
    for category, score in category_scores.items():
        canonical_category = normalize_category(category)
        finite_score = finite_float(score)
        if not canonical_category or finite_score is None:
            continue
        scores_by_category.setdefault(canonical_category, []).append(finite_score)

    return {
        category: math.fsum(sorted(scores))
        for category, scores in sorted(scores_by_category.items())
    }


def _matched_recent_score(
    item: Mapping[str, Any],
    aggregated_category_scores: Mapping[str, float],
) -> float:
    matched_scores: list[float] = []
    for field_name in ("category", "category_main"):
        category = normalize_category(item.get(field_name))
        if category and category in aggregated_category_scores:
            matched_scores.append(aggregated_category_scores[category])

    return max(matched_scores) if matched_scores else 0.0


def _legacy_rerank_candidates(
    items: list[dict[str, Any]],
    category_scores: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Preserve the pre-V2 production behavior exactly."""
    normalized_scores = aggregate_category_scores(category_scores)
    if not normalized_scores:
        for item in items:
            item["reranked_score"] = float(item.get("cluster_total_score") or 0.0)
        items.sort(key=lambda item: item.get("reranked_score", 0.0), reverse=True)
        return items

    for item in items:
        matched_score = _matched_recent_score(item, normalized_scores)
        base_score = float(item.get("cluster_total_score") or 0.0)
        item["reranked_score"] = base_score + matched_score

    items.sort(key=lambda item: item.get("reranked_score", 0.0), reverse=True)
    return items


def rerank_candidates(
    items: list[dict[str, Any]],
    category_scores: Mapping[str, Any],
    config: "RecommendationScoringConfig",
) -> list[dict[str, Any]]:
    """Apply legacy reranking or the feature-flagged V2 scoring contract."""
    if not config.enabled:
        return _legacy_rerank_candidates(items, category_scores)

    normalized_base_scores = min_max_normalize(
        [item.get("cluster_total_score") for item in items]
    )
    normalized_category_scores = aggregate_category_scores(category_scores)
    reranked: list[dict[str, Any]] = []
    for item, normalized_base_score in zip(items, normalized_base_scores, strict=True):
        result = dict(item)
        recent_score = _matched_recent_score(item, normalized_category_scores)
        recent_signal = bound_recent_signal(recent_score, config.recent_temperature)
        result["reranked_score"] = fuse_scores(
            normalized_base_score,
            recent_signal,
            base_weight=config.base_weight,
            recent_weight=config.recent_weight,
        )
        reranked.append(result)

    reranked.sort(
        key=lambda candidate: (
            -float(candidate["reranked_score"]),
            stable_product_id(candidate),
        )
    )
    return reranked


def rerank_home_candidates_with_recent_categories(
    items: list[dict[str, Any]],
    category_scores: Mapping[str, Any],
    *,
    base_weight: float = 0.65,
    recent_weight: float = 0.35,
    injected_candidate_boost: float = 0.03,
    fallback_config: "RecommendationScoringConfig" | None = None,
) -> list[dict[str, Any]]:
    """Rerank the expanded home pool while preserving raw model scores.

    The existing scorer remains authoritative when there is no usable Redis
    signal. With recent intent, model and Redis scores are normalized onto
    comparable scales so a model score in the thousands cannot hide a recent
    category match.
    """
    normalized_category_scores = aggregate_category_scores(category_scores)
    copied_items = [dict(item) for item in items]
    if not normalized_category_scores:
        return rerank_candidates(
            copied_items,
            category_scores,
            fallback_config or SCORING_CONFIG,
        )

    normalized_base_weight, normalized_recent_weight = normalize_weights(
        base_weight,
        recent_weight,
    )
    parsed_base_scores = [
        finite_float(item.get("cluster_total_score")) or 0.0
        for item in copied_items
    ]
    max_base_score = max([0.0, *parsed_base_scores])
    max_recent_score = max([0.0, *normalized_category_scores.values()])

    ranked_with_positions: list[tuple[dict[str, Any], int]] = []
    for original_position, (item, base_score) in enumerate(
        zip(copied_items, parsed_base_scores, strict=True)
    ):
        matched_scores = [
            normalized_category_scores[category]
            for field_name in HOME_CANDIDATE_CATEGORY_FIELDS
            if (category := normalize_category(item.get(field_name)))
            in normalized_category_scores
        ]
        matched_recent_score = max(matched_scores) if matched_scores else 0.0
        base_norm = base_score / max_base_score if max_base_score > 0.0 else 0.0
        recent_norm = (
            matched_recent_score / max_recent_score
            if max_recent_score > 0.0
            else 0.0
        )
        reranked_score = (
            normalized_base_weight * base_norm
            + normalized_recent_weight * recent_norm
        )
        if (
            matched_recent_score > 0.0
            and item.get("candidate_source") == "recent_category"
        ):
            reranked_score += injected_candidate_boost

        item["reranked_score"] = reranked_score
        ranked_with_positions.append((item, original_position))

    ranked_with_positions.sort(
        key=lambda entry: (
            -float(entry[0]["reranked_score"]),
            entry[1],
            stable_product_id(entry[0]),
        )
    )
    return [item for item, _ in ranked_with_positions]


def resolve_recent_event_weight(event_type: Any, action: Any = None) -> float | None:
    """Map supported events to category-score deltas.

    ``CART_UPDATE/remove`` maps to the legacy removal penalty. Other
    ``CART_UPDATE`` actions are ignored because the frontend already emits the
    dedicated ``add_to_cart`` event, avoiding a double positive increment.
    """
    normalized_event_type = str(event_type or "").strip().lower()
    normalized_action = str(action or "").strip().lower()
    if normalized_event_type == "cart_update":
        return RECENT_EVENT_WEIGHTS["remove_from_cart"] if normalized_action == "remove" else None
    if normalized_event_type not in RECENT_CATEGORY_EVENT_TYPES:
        return None
    return RECENT_EVENT_WEIGHTS[normalized_event_type]


def purchase_completed_category_updates(
    event: Mapping[str, Any],
) -> list[tuple[str, float]]:
    """Return one purchase increment per distinct valid item category.

    Categories are returned as canonical Redis members. Quantity is
    intentionally ignored.
    """
    event_type = str(event.get("eventType") or "").strip().lower()
    if event_type != "purchase_completed":
        return []

    items = event.get("items")
    if not isinstance(items, list):
        return []

    canonical_categories: dict[str, None] = {}
    for item in items:
        if not isinstance(item, Mapping):
            continue

        category = None
        for field_name in PURCHASE_ITEM_CATEGORY_FIELDS:
            value = item.get(field_name)
            if not isinstance(value, str):
                continue
            stripped_value = value.strip()
            if stripped_value:
                category = stripped_value
                break

        normalized_category = normalize_category(category)
        if normalized_category:
            canonical_categories.setdefault(normalized_category, None)

    purchase_weight = RECENT_EVENT_WEIGHTS["purchase"]
    return [
        (category, purchase_weight)
        for category in canonical_categories
    ]


def _parse_bool(name: str, value: Any) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of true/false, 1/0, yes/no, or on/off")


def _parse_float(name: str, value: Any) -> float:
    parsed = finite_float(value)
    if parsed is None:
        raise ValueError(f"{name} must be a finite number")
    return parsed


@dataclass(frozen=True)
class RecommendationScoringConfig:
    """Feature flag and provisional scoring parameters loaded at process start."""

    enabled: bool = False
    base_weight: float = DEFAULT_BASE_WEIGHT
    recent_weight: float = DEFAULT_RECENT_WEIGHT
    recent_temperature: float = DEFAULT_RECENT_TEMPERATURE
    als_weight: float = DEFAULT_ALS_WEIGHT
    content_weight: float = DEFAULT_CONTENT_WEIGHT

    def __post_init__(self) -> None:
        if finite_float(self.recent_temperature) is None or self.recent_temperature <= 0.0:
            raise ValueError("recent_temperature must be strictly positive")
        normalize_weights(self.base_weight, self.recent_weight)
        normalize_weights(self.als_weight, self.content_weight)

    @classmethod
    def from_env(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "RecommendationScoringConfig":
        source = os.environ if environment is None else environment
        return cls(
            enabled=_parse_bool(
                "RECOMMENDATION_SCORING_V2",
                source.get("RECOMMENDATION_SCORING_V2", "false"),
            ),
            base_weight=_parse_float(
                "RECOMMENDATION_BASE_WEIGHT",
                source.get("RECOMMENDATION_BASE_WEIGHT", str(DEFAULT_BASE_WEIGHT)),
            ),
            recent_weight=_parse_float(
                "RECOMMENDATION_RECENT_WEIGHT",
                source.get("RECOMMENDATION_RECENT_WEIGHT", str(DEFAULT_RECENT_WEIGHT)),
            ),
            recent_temperature=_parse_float(
                "RECOMMENDATION_RECENT_TEMPERATURE",
                source.get(
                    "RECOMMENDATION_RECENT_TEMPERATURE",
                    str(DEFAULT_RECENT_TEMPERATURE),
                ),
            ),
            als_weight=_parse_float(
                "RECOMMENDATION_ALS_WEIGHT",
                source.get("RECOMMENDATION_ALS_WEIGHT", str(DEFAULT_ALS_WEIGHT)),
            ),
            content_weight=_parse_float(
                "RECOMMENDATION_CONTENT_WEIGHT",
                source.get("RECOMMENDATION_CONTENT_WEIGHT", str(DEFAULT_CONTENT_WEIGHT)),
            ),
        )


SCORING_CONFIG = RecommendationScoringConfig.from_env()
