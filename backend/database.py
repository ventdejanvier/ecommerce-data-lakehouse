from typing import Any

import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5434/data_lakehouse",
)

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=1800,
)


def get_recommendations_from_db(user_id: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        result = connection.execute(
            text(
                """
                SELECT
                    r.cluster_id,
                    r.product_id,
                    r.display_name,
                    r.cluster_total_score
                FROM serving_recommendations AS r
                INNER JOIN serving_user_clusters AS u
                    ON r.cluster_id = u.cluster_id
                WHERE u.user_id = :user_id
                ORDER BY r.cluster_total_score DESC
                LIMIT 10
                """
            ),
            {"user_id": user_id},
        )

        return [dict(row) for row in result.mappings().all()]
