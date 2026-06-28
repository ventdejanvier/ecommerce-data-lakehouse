from __future__ import annotations

import argparse
import os

from model_publication import create_generation, open_postgres_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register one BUILDING recommendation generation.")
    parser.add_argument(
        "--airflow-run-id",
        default=os.getenv("AIRFLOW_RUN_ID"),
        help="Optional Airflow DAG run identifier stored for auditability.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    connection = open_postgres_connection()
    try:
        generation_id = create_generation(connection, airflow_run_id=args.airflow_run_id)
    finally:
        connection.close()
    print(generation_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
