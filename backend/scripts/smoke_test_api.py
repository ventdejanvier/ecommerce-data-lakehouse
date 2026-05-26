from __future__ import annotations

import argparse
from datetime import UTC, datetime
from uuid import uuid4

import httpx


def assert_recommendation_schema(payload: object) -> None:
    if not isinstance(payload, list):
        raise AssertionError(f"Expected a JSON list, got {type(payload).__name__}")

    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise AssertionError(f"Recommendation {index} is not an object")

        required_keys = {
            "cluster_id": int,
            "product_id": int,
            "display_name": str,
            "cluster_total_score": (int, float),
        }
        for key, expected_type in required_keys.items():
            if key not in item:
                raise AssertionError(f"Recommendation {index} is missing '{key}'")
            if not isinstance(item[key], expected_type):
                raise AssertionError(
                    f"Recommendation {index}.{key} has invalid type "
                    f"{type(item[key]).__name__}"
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test the live FastAPI backend.")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="FastAPI base URL.",
    )
    parser.add_argument(
        "--user-id",
        default="USER_TEST",
        help="User id to request recommendations for.",
    )
    parser.add_argument(
        "--expect-recommendations",
        action="store_true",
        help="Fail if the recommendation response is an empty list.",
    )
    return parser.parse_args()


def request_or_none(
    client: httpx.Client,
    method: str,
    path: str,
    failures: list[str],
    **kwargs: object,
) -> httpx.Response | None:
    try:
        return client.request(method, path, **kwargs)
    except httpx.HTTPError as exc:
        failures.append(f"{method} {path} request failed: {exc}")
        print(f"[FAIL] {method} {path} -> request failed: {exc}")
        return None


def main() -> int:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    failures = []

    with httpx.Client(base_url=base_url, timeout=10.0) as client:
        health = request_or_none(client, "GET", "/api/health", failures)
        if health is None:
            return 1

        if health.status_code == 200:
            print(f"[OK] GET /api/health -> {health.json()}")
        else:
            failures.append(f"GET /api/health returned {health.status_code}: {health.text}")
            print(f"[FAIL] GET /api/health -> {health.status_code}")

        for path in (
            f"/api/recommend/home/{args.user_id}",
            f"/api/recommendations/{args.user_id}",
        ):
            response = request_or_none(client, "GET", path, failures)
            if response is None:
                continue

            if response.status_code != 200:
                failures.append(f"GET {path} returned {response.status_code}: {response.text}")
                print(f"[FAIL] GET {path} -> {response.status_code}")
                continue

            payload = response.json()
            try:
                assert_recommendation_schema(payload)
            except AssertionError as exc:
                failures.append(f"GET {path} schema failure: {exc}")
                print(f"[FAIL] GET {path} schema -> {exc}")
                continue

            if args.expect_recommendations and not payload:
                failures.append(f"GET {path} returned an empty recommendation list")
                print(f"[FAIL] GET {path} -> empty recommendation list")
                continue

            print(f"[OK] GET {path} -> {len(payload)} recommendation(s)")
            if payload:
                print(f"     first: {payload[0]}")

        event_id = f"evt_smoke_{uuid4().hex}"
        tracking_payload = {
            "eventId": event_id,
            "timestamp": datetime.now(UTC).isoformat(),
            "sessionId": f"session_smoke_{uuid4().hex}",
            "eventType": "page_view",
            "userId": args.user_id,
            "pageName": "api_smoke_test",
            "context": {
                "userAgent": "backend-smoke-test",
                "url": base_url,
                "referrer": "",
            },
        }
        track = request_or_none(client, "POST", "/api/track", failures, json=tracking_payload)
        if track is None:
            pass
        elif track.status_code != 202:
            failures.append(f"POST /api/track returned {track.status_code}: {track.text}")
            print(f"[FAIL] POST /api/track -> {track.status_code}")
        else:
            body = track.json()
            if body.get("status") != "accepted" or body.get("eventId") != event_id:
                failures.append(f"Unexpected tracking response: {body}")
                print(f"[FAIL] POST /api/track response schema -> {body}")
            else:
                print(f"[OK] POST /api/track -> {body}")

    if failures:
        print("\nAPI smoke test failed:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
