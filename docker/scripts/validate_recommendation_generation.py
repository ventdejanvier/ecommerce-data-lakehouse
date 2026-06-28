from __future__ import annotations

import argparse
import json
import os

from model_publication import (
    GenerationValidationError,
    open_postgres_connection,
    validate_generation,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a staged recommendation generation.")
    parser.add_argument(
        "generation_id",
        nargs="?",
        default=os.getenv("MODEL_GENERATION_ID"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    connection = open_postgres_connection()
    try:
        try:
            report = validate_generation(connection, args.generation_id)
        except GenerationValidationError as exc:
            print(json.dumps(exc.report, sort_keys=True))
            return 1
    finally:
        connection.close()
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
