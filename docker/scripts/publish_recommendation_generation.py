from __future__ import annotations

import argparse
import json
import os

from model_publication import open_postgres_connection, publish_generation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Atomically publish a READY generation.")
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
        result = publish_generation(connection, args.generation_id)
    finally:
        connection.close()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
