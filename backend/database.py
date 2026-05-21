from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

DATABASE_URL = "postgresql://user:password@postgres:5432/data_lakehouse"

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=1800,
)


def _serving_user_clusters_exists(connection: Connection) -> bool:
    query = text("SELECT to_regclass('public.serving_user_clusters') IS NOT NULL")
    return bool(connection.execute(query).scalar())


def _parse_cluster_id(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def get_recommendations_from_db(user_id: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        if _serving_user_clusters_exists(connection):
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
                        ON u.cluster_id = r.cluster_id
                    WHERE u.user_id = :user_id
                    ORDER BY r.cluster_total_score DESC
                    LIMIT 10
                    """
                ),
                {"user_id": user_id},
            )
        else:
            # Phase 1 exports cluster-level recommendations only. Until a
            # serving-side user-to-cluster table exists, allow direct cluster
            # lookups through the same path parameter for MVP testing.
            cluster_id = _parse_cluster_id(user_id)
            if cluster_id is None:
                return []

            result = connection.execute(
                text(
                    """
                    SELECT
                        cluster_id,
                        product_id,
                        display_name,
                        cluster_total_score
                    FROM serving_recommendations
                    WHERE cluster_id = :cluster_id
                    ORDER BY cluster_total_score DESC
                    LIMIT 10
                    """
                ),
                {"cluster_id": cluster_id},
            )

        return [dict(row) for row in result.mappings().all()]
