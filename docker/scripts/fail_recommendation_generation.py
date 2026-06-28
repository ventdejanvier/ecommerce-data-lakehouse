from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping

from model_publication import (
    fail_generation,
    normalize_airflow_run_id,
    open_postgres_connection,
    validate_generation_id,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mark one BUILDING recommendation generation as FAILED."
    )
    parser.add_argument(
        "--generation-id",
        default=os.getenv("MODEL_GENERATION_ID"),
    )
    parser.add_argument(
        "--airflow-run-id",
        default=os.getenv("MODEL_AIRFLOW_RUN_ID"),
    )
    parser.add_argument("--reason", required=True)
    parser.add_argument("--metadata-json", default="{}")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generation_id = (
        validate_generation_id(args.generation_id)
        if args.generation_id and args.generation_id.strip()
        else None
    )
    airflow_run_id = normalize_airflow_run_id(args.airflow_run_id)
    if generation_id is None and airflow_run_id is None:
        raise ValueError("generation_id or airflow_run_id is required")
    if not isinstance(args.reason, str) or not args.reason.strip():
        raise ValueError("failure reason must not be empty")
    metadata = json.loads(args.metadata_json)
    if not isinstance(metadata, Mapping):
        raise ValueError("metadata-json must decode to an object")

    connection = open_postgres_connection()
    try:
        result = fail_generation(
            connection,
            generation_id,
            reason=args.reason,
            metadata=metadata,
            airflow_run_id=airflow_run_id,
        )
    finally:
        connection.close()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
