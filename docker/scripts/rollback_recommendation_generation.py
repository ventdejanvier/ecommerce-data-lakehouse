from __future__ import annotations

import argparse
import json

from model_publication import (
    open_postgres_connection,
    rollback_generation,
    validate_generation_id,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Atomically roll back the active generation.")
    parser.add_argument("generation_id")
    parser.add_argument("--reason")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    generation_id = validate_generation_id(args.generation_id)
    connection = open_postgres_connection()
    try:
        result = rollback_generation(
            connection,
            generation_id,
            reason=args.reason,
        )
    finally:
        connection.close()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
