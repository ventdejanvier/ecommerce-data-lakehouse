from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import database  # noqa: E402


class FakeRows:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class FakeConnection:
    def __init__(self) -> None:
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params):
        self.calls.append((str(query), params))
        rows_by_category = {
            "home appliances": [
                {
                    "product_id": 20,
                    "display_name": "Coffee Maker",
                    "price": Decimal("49.99"),
                    "category_main": "Home-Appliances",
                    "category_sub": "Kitchen",
                    "category_detail": None,
                    "category_name": None,
                    "category": None,
                }
            ],
            "accessories": [
                {
                    "product_id": 10,
                    "display_name": "Auto Accessories GPS",
                    "price": Decimal("9.99"),
                    "category_main": "Auto",
                    "category_sub": "Accessories",
                    "category_detail": "Gps",
                    "category_name": None,
                    "category": None,
                }
            ],
        }
        return FakeRows(rows_by_category.get(params["recent_category"], []))


class FakeEngine:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def connect(self):
        return self.connection


def test_recent_category_candidates_are_schema_aware_and_stably_ordered(monkeypatch) -> None:
    connection = FakeConnection()
    columns = {
        "product_id",
        "display_name",
        "avg_price",
        "category_main",
        "category_sub",
        "category_detail",
    }
    monkeypatch.setattr(database, "engine", FakeEngine(connection))
    monkeypatch.setattr(
        database,
        "_get_public_table_columns",
        lambda unused_connection, table_name: columns,
    )

    candidates = database.get_recent_category_candidates(
        {
            "Accessories": 5.0,
            "home_appliances": 6.0,
            "Computers": 1.0,
        },
        max_categories=2,
        limit_per_category=12,
    )

    assert [call[1]["recent_category"] for call in connection.calls] == [
        "home appliances",
        "accessories",
    ]
    assert all(call[1]["limit"] == 12 for call in connection.calls)
    assert 'FROM public."dim_products" p' in connection.calls[0][0]
    assert 'ORDER BY p."product_id" ASC' in connection.calls[0][0]
    assert "REGEXP_REPLACE" in connection.calls[0][0]
    assert [candidate["id"] for candidate in candidates] == ["20", "10"]
    assert candidates[0]["price"] == 49.99
    assert candidates[0]["recent_match_category"] == "home appliances"
    assert candidates[1]["category"] == "Accessories"
    assert candidates[1]["category_main"] == "Auto"
    assert candidates[1]["category_sub"] == "Accessories"
    assert candidates[1]["category_detail"] == "Gps"
    assert all(
        candidate["candidate_source"] == "recent_category"
        and candidate["cluster_total_score"] == 0.0
        for candidate in candidates
    )
