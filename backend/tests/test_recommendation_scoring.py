from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recommendation_scoring import (  # noqa: E402
    SCORING_CONFIG,
    RecommendationScoringConfig,
    aggregate_category_scores,
    blend_model_candidates,
    bound_recent_signal,
    finite_float,
    fuse_scores,
    min_max_normalize,
    normalize_category,
    normalize_weights,
    purchase_completed_category_updates,
    rerank_candidates,
    rerank_home_candidates_with_recent_categories,
    resolve_recent_event_weight,
)


def enabled_config(**overrides) -> RecommendationScoringConfig:
    values = {
        "enabled": True,
        "base_weight": 0.8,
        "recent_weight": 0.2,
        "recent_temperature": 1.0,
        "als_weight": 0.5,
        "content_weight": 0.5,
    }
    values.update(overrides)
    return RecommendationScoringConfig(**values)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1.25", 1.25),
        (-3, -3.0),
        (None, None),
        (float("nan"), None),
        (float("inf"), None),
        (float("-inf"), None),
        ("not-a-number", None),
        (True, None),
    ],
)
def test_finite_float(value, expected) -> None:
    assert finite_float(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Home Appliances", "home appliances"),
        (" HOME   APPLIANCES ", "home appliances"),
        ("home_appliances", "home appliances"),
        ("home-appliances", "home appliances"),
        ("", ""),
        ("   ", ""),
        (None, ""),
        (123, ""),
        (True, ""),
    ],
)
def test_category_normalization_contract(value, expected) -> None:
    assert normalize_category(value) == expected


def test_alias_aggregation_sums_finite_scores_deterministically() -> None:
    aliases = {
        "Home Appliances": 10,
        "home_appliances": 5,
        "home-appliances": -2,
    }

    expected = {"home appliances": 13.0}
    assert aggregate_category_scores(aliases) == expected
    assert aggregate_category_scores(dict(reversed(list(aliases.items())))) == expected


def test_alias_aggregation_ignores_malformed_values_and_preserves_negative_totals() -> None:
    scores = {
        "Books": -10,
        "books": 2,
        "": 99,
        None: 99,
        "phones": "invalid",
        "laptops": float("nan"),
        "televisions": float("inf"),
    }

    assert aggregate_category_scores(scores) == {"books": -8.0}


@pytest.mark.parametrize(
    ("scores", "expected"),
    [
        ([10.0, 20.0, 30.0], [0.0, 0.5, 1.0]),
        ([-10.0, 0.0, 10.0], [0.0, 0.5, 1.0]),
        ([7.0, 7.0], [0.5, 0.5]),
        ([7.0], [0.5]),
        ([], []),
    ],
)
def test_min_max_normalization(scores, expected) -> None:
    assert min_max_normalize(scores) == pytest.approx(expected)


def test_min_max_normalization_safely_floors_invalid_values() -> None:
    scores = [1.0, None, float("nan"), float("inf"), "bad", 3.0]
    assert min_max_normalize(scores) == pytest.approx([0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
    assert min_max_normalize([None, "bad"]) == [0.0, 0.0]


def test_tanh_recent_signal_is_signed_and_bounded() -> None:
    assert bound_recent_signal(0.0, 2.0) == 0.0
    assert 0.0 < bound_recent_signal(2.0, 2.0) < 1.0
    assert -1.0 < bound_recent_signal(-2.0, 2.0) < 0.0
    assert bound_recent_signal(1e100, 1.0) == pytest.approx(1.0)
    assert bound_recent_signal(-1e100, 1.0) == pytest.approx(-1.0)
    assert bound_recent_signal("bad", 1.0) == 0.0


@pytest.mark.parametrize("temperature", [0.0, -1.0, float("nan"), "bad"])
def test_tanh_recent_signal_rejects_invalid_temperature(temperature) -> None:
    with pytest.raises(ValueError):
        bound_recent_signal(1.0, temperature)


def test_weighted_fusion_normalizes_weights_and_is_deterministic() -> None:
    expected = 0.5  # (3/4 * 1.0) + (1/4 * -1.0)
    first = fuse_scores(1.0, -1.0, base_weight=3.0, recent_weight=1.0)
    second = fuse_scores(1.0, -1.0, base_weight=3.0, recent_weight=1.0)
    assert first == pytest.approx(expected)
    assert second == first
    assert normalize_weights(3.0, 1.0) == pytest.approx((0.75, 0.25))


@pytest.mark.parametrize(
    ("base_weight", "recent_weight"),
    [(-1.0, 1.0), (1.0, -1.0), (0.0, 0.0), (float("nan"), 1.0)],
)
def test_weighted_fusion_rejects_invalid_weights(base_weight, recent_weight) -> None:
    with pytest.raises(ValueError):
        fuse_scores(
            0.5,
            0.5,
            base_weight=base_weight,
            recent_weight=recent_weight,
        )


def test_als_content_blend_normalizes_incompatible_scales() -> None:
    als = [
        {"product_id": "1", "cluster_total_score": 100.0},
        {"product_id": "2", "cluster_total_score": 200.0},
        {"product_id": "3", "cluster_total_score": 150.0},
    ]
    content = [
        {"product_id": "1", "cluster_total_score": 1.0},
        {"product_id": "3", "cluster_total_score": 3.0},
        {"product_id": "4", "cluster_total_score": 2.0},
    ]

    blended = blend_model_candidates(
        als,
        content,
        als_weight=0.5,
        content_weight=0.5,
        limit=10,
    )

    assert [row["product_id"] for row in blended] == ["3", "2", "4", "1"]
    assert [row["cluster_total_score"] for row in blended] == pytest.approx(
        [0.75, 0.5, 0.25, 0.0]
    )


def test_als_content_blend_has_stable_tie_breaking_and_limit() -> None:
    blended = blend_model_candidates(
        [{"product_id": "2", "cluster_total_score": 50.0}],
        [{"product_id": "10", "cluster_total_score": 5.0}],
        als_weight=0.5,
        content_weight=0.5,
        limit=1,
    )
    assert [row["product_id"] for row in blended] == ["10"]


def test_als_content_blend_safely_handles_missing_scores() -> None:
    blended = blend_model_candidates(
        [
            {"product_id": "missing", "cluster_total_score": None},
            {"product_id": "valid", "cluster_total_score": 5.0},
        ],
        [],
        als_weight=1.0,
        content_weight=0.0,
        limit=10,
    )
    assert [row["product_id"] for row in blended] == ["valid", "missing"]
    assert [row["cluster_total_score"] for row in blended] == pytest.approx([0.5, 0.0])


def test_positive_recent_behavior_increases_rank() -> None:
    items = [
        {"id": "1", "category": "Laptops", "cluster_total_score": 10.0},
        {"id": "2", "category": "Phones", "cluster_total_score": 10.0},
    ]
    reranked = rerank_candidates(items, {"Laptops": 1.0}, enabled_config())
    assert [item["id"] for item in reranked] == ["1", "2"]
    assert reranked[0]["reranked_score"] > reranked[1]["reranked_score"]


def test_negative_recent_behavior_decreases_rank() -> None:
    items = [
        {"id": "1", "category": "Laptops", "cluster_total_score": 10.0},
        {"id": "2", "category": "Phones", "cluster_total_score": 10.0},
    ]
    reranked = rerank_candidates(items, {"Laptops": -1.0}, enabled_config())
    assert [item["id"] for item in reranked] == ["2", "1"]


def test_extreme_recent_scores_are_bounded_and_base_remains_relevant() -> None:
    items = [
        {"id": "high-base", "category": "Penalty", "cluster_total_score": 100.0},
        {"id": "low-base", "category": "Boost", "cluster_total_score": 0.0},
    ]
    reranked = rerank_candidates(
        items,
        {"Penalty": -1e100, "Boost": 1e100},
        enabled_config(),
    )
    assert [item["id"] for item in reranked] == ["high-base", "low-base"]
    assert reranked[0]["reranked_score"] == pytest.approx(0.6)
    assert reranked[1]["reranked_score"] == pytest.approx(0.2)
    assert bound_recent_signal(1e6, 1.0) == bound_recent_signal(1e100, 1.0)


def test_feature_flag_disabled_preserves_legacy_addition() -> None:
    config = RecommendationScoringConfig(enabled=False)
    items = [
        {"id": "101", "category": "Laptops", "cluster_total_score": 12.5},
    ]
    reranked = rerank_candidates(items, {"Laptops": 5.0}, config)
    assert reranked[0]["reranked_score"] == 17.5


def test_v1_preserves_single_negative_category_match() -> None:
    items = [{"id": "101", "category": "Laptops", "cluster_total_score": 10.0}]

    reranked = rerank_candidates(
        items,
        {"Laptops": -5.0},
        RecommendationScoringConfig(enabled=False),
    )

    assert reranked[0]["reranked_score"] == 5.0


def test_v1_unmatched_category_main_does_not_neutralize_negative_category() -> None:
    items = [
        {
            "id": "101",
            "category": "Laptops",
            "category_main": "Untracked Parent",
            "cluster_total_score": 10.0,
        }
    ]

    reranked = rerank_candidates(
        items,
        {"Laptops": -5.0},
        RecommendationScoringConfig(enabled=False),
    )

    assert reranked[0]["reranked_score"] == 5.0


def test_v1_no_matching_category_has_zero_recent_influence() -> None:
    items = [{"id": "101", "category": "Phones", "cluster_total_score": 10.0}]

    reranked = rerank_candidates(
        items,
        {"Laptops": -5.0},
        RecommendationScoringConfig(enabled=False),
    )

    assert reranked[0]["reranked_score"] == 10.0


def test_v1_both_category_fields_select_only_the_greater_actual_match() -> None:
    items = [
        {
            "id": "101",
            "category": "Laptops",
            "category_main": "Electronics",
            "cluster_total_score": 10.0,
        }
    ]
    category_scores = {
        "Laptops": -5.0,
        "Electronics": -2.0,
        "Unrelated Positive Category": 100.0,
    }

    reranked = rerank_candidates(
        items,
        category_scores,
        RecommendationScoringConfig(enabled=False),
    )

    assert reranked[0]["reranked_score"] == 8.0


def test_v1_uses_aggregated_aliases_with_legacy_addition() -> None:
    items = [
        {"id": "101", "category": " HOME-APPLIANCES ", "cluster_total_score": 12.0},
    ]
    category_scores = {
        "Home Appliances": 10,
        "home_appliances": 5,
        "home-appliances": -2,
    }

    reranked = rerank_candidates(
        items,
        category_scores,
        RecommendationScoringConfig(enabled=False),
    )

    assert reranked[0]["reranked_score"] == 25.0


def test_v2_uses_aggregated_alias_score_before_tanh() -> None:
    items = [
        {"id": "101", "category_main": "home appliances", "cluster_total_score": 12.0},
    ]
    category_scores = {
        "Home Appliances": 10,
        "home_appliances": 5,
        "home-appliances": -2,
    }

    reranked = rerank_candidates(items, category_scores, enabled_config())

    expected = 0.8 * 0.5 + 0.2 * math.tanh(13.0)
    assert reranked[0]["reranked_score"] == pytest.approx(expected)


def test_v2_uses_negative_matched_score_before_tanh() -> None:
    items = [{"id": "101", "category": "Laptops", "cluster_total_score": 10.0}]

    reranked = rerank_candidates(
        items,
        {"Laptops": -5.0},
        enabled_config(),
    )

    expected = 0.8 * 0.5 + 0.2 * math.tanh(-5.0)
    assert reranked[0]["reranked_score"] == pytest.approx(expected)


def test_feature_flag_enabled_uses_v2_without_exposing_debug_fields() -> None:
    items = [
        {"id": "101", "category": "Laptops", "cluster_total_score": 12.5},
    ]
    reranked = rerank_candidates(items, {"Laptops": 5.0}, enabled_config())
    assert -0.2 <= reranked[0]["reranked_score"] <= 1.0
    assert set(reranked[0]) == {
        "id",
        "category",
        "cluster_total_score",
        "reranked_score",
    }


def test_home_rerank_keeps_top_model_and_promotes_recent_over_low_base() -> None:
    items = [
        {
            "id": "top-model",
            "category": "Computers",
            "cluster_total_score": 5000.0,
        },
        {
            "id": "low-model",
            "category": "Computers",
            "cluster_total_score": 100.0,
        },
        {
            "id": "recent",
            "category_main": "Accessories",
            "cluster_total_score": 0.0,
            "candidate_source": "recent_category",
            "recent_match_category": "accessories",
        },
    ]

    reranked = rerank_home_candidates_with_recent_categories(
        items,
        {"Accessories": 5.0},
    )

    assert [item["id"] for item in reranked] == [
        "top-model",
        "recent",
        "low-model",
    ]
    assert reranked[0]["reranked_score"] == pytest.approx(0.65)
    assert reranked[1]["reranked_score"] == pytest.approx(0.38)
    assert reranked[1]["cluster_total_score"] == 0.0
    assert reranked[0]["cluster_total_score"] == 5000.0


def test_home_rerank_matches_all_category_fields_and_normalized_aliases() -> None:
    items = [
        {
            "id": "detail-match",
            "category": "Electronics",
            "category_detail": "Country-Yard",
            "cluster_total_score": 1.0,
        },
        {
            "id": "unmatched",
            "category": "Electronics",
            "cluster_total_score": 1.0,
        },
    ]

    reranked = rerank_home_candidates_with_recent_categories(
        items,
        {"country_yard": 3.0},
    )

    assert [item["id"] for item in reranked] == ["detail-match", "unmatched"]
    assert reranked[0]["reranked_score"] > reranked[1]["reranked_score"]


def test_home_rerank_without_redis_scores_preserves_existing_scoring() -> None:
    items = [
        {"id": "1", "category": "Laptops", "cluster_total_score": 2.0},
        {"id": "2", "category": "Phones", "cluster_total_score": 1.0},
    ]

    expected = rerank_candidates(
        [dict(item) for item in items],
        {},
        SCORING_CONFIG,
    )
    actual = rerank_home_candidates_with_recent_categories(items, {})

    assert actual == expected


def test_event_semantics_cover_cart_remove_and_legacy_events() -> None:
    assert resolve_recent_event_weight("CART_UPDATE", "remove") == -5.0
    assert resolve_recent_event_weight("add_to_cart", "add") == 5.0
    assert resolve_recent_event_weight("remove_from_cart") == -5.0
    assert resolve_recent_event_weight("CART_UPDATE", "add") is None
    assert resolve_recent_event_weight("page_view") is None


def test_purchase_completed_updates_each_distinct_category_once() -> None:
    updates = purchase_completed_category_updates(
        {
            "eventType": "PURCHASE_COMPLETED",
            "items": [
                {"category_main": "Electronics", "quantity": 2},
                {"productCategory": "Home Appliances", "quantity": 5},
            ],
        }
    )
    assert updates == [("electronics", 10.0), ("home appliances", 10.0)]


def test_purchase_completed_deduplicates_normalized_categories() -> None:
    updates = purchase_completed_category_updates(
        {
            "eventType": "PURCHASE_COMPLETED",
            "items": [
                {"category": "Home Appliances", "quantity": 1},
                {"product_category": "home_appliances", "quantity": 99},
            ],
        }
    )
    assert updates == [("home appliances", 10.0)]


def test_purchase_completed_ignores_malformed_items_and_empty_categories() -> None:
    updates = purchase_completed_category_updates(
        {
            "eventType": "PURCHASE_COMPLETED",
            "items": [
                None,
                "not-an-item",
                {},
                {"category_main": "  "},
                {"category": 123},
                {"categoryName": "Valid Category"},
            ],
        }
    )
    assert updates == [("valid category", 10.0)]


@pytest.mark.parametrize(
    "event",
    [
        {"eventType": "PURCHASE_COMPLETED"},
        {"eventType": "PURCHASE_COMPLETED", "items": None},
        {"eventType": "PURCHASE_COMPLETED", "items": []},
        {"eventType": "PURCHASE_COMPLETED", "items": [{"category": " "}]},
    ],
)
def test_purchase_completed_without_categories_has_no_updates(event) -> None:
    assert purchase_completed_category_updates(event) == []


def test_configuration_defaults_to_disabled_and_parses_overrides() -> None:
    defaults = RecommendationScoringConfig.from_env({})
    assert defaults.enabled is False
    assert defaults.base_weight == 0.8
    assert defaults.recent_weight == 0.2
    assert defaults.recent_temperature == 10.0
    assert defaults.als_weight == 0.5
    assert defaults.content_weight == 0.5

    enabled = RecommendationScoringConfig.from_env(
        {
            "RECOMMENDATION_SCORING_V2": "true",
            "RECOMMENDATION_BASE_WEIGHT": "3",
            "RECOMMENDATION_RECENT_WEIGHT": "1",
            "RECOMMENDATION_RECENT_TEMPERATURE": "4",
            "RECOMMENDATION_ALS_WEIGHT": "9",
            "RECOMMENDATION_CONTENT_WEIGHT": "1",
        }
    )
    assert enabled.enabled is True
    assert enabled.recent_temperature == 4.0
    assert normalize_weights(enabled.base_weight, enabled.recent_weight) == pytest.approx((0.75, 0.25))
    assert normalize_weights(enabled.als_weight, enabled.content_weight) == pytest.approx((0.9, 0.1))


@pytest.mark.parametrize(
    "environment",
    [
        {"RECOMMENDATION_SCORING_V2": "maybe"},
        {"RECOMMENDATION_BASE_WEIGHT": "nan"},
        {"RECOMMENDATION_RECENT_TEMPERATURE": "0"},
        {"RECOMMENDATION_ALS_WEIGHT": "0", "RECOMMENDATION_CONTENT_WEIGHT": "0"},
    ],
)
def test_configuration_rejects_invalid_values(environment) -> None:
    with pytest.raises(ValueError):
        RecommendationScoringConfig.from_env(environment)


def test_tanh_implementation_matches_math_contract() -> None:
    assert bound_recent_signal(2.0, 4.0) == pytest.approx(math.tanh(0.5))
