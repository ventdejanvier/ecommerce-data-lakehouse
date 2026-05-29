import os
import re
from decimal import Decimal
from typing import Any

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Connection

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


MAX_RECOMMENDATIONS = 10
PRODUCT_ID_COLUMNS = ("product_id",)
PRODUCT_NAME_COLUMNS = ("display_name", "product_name", "name")
PRODUCT_PRICE_COLUMNS = ("avg_price", "price")
PRODUCT_CATEGORY_COLUMNS = ("category_main", "category_fixed", "category")
GLOBAL_TOP_TABLE_CANDIDATES = tuple(
    table.strip()
    for table in os.getenv(
        "GLOBAL_TOP_PRODUCTS_TABLES",
        (
            "serving_top_10_popular_products,"
            "serving_top_popular_products,"
            "serving_popular_products,"
            "serving_top_trending,"
            "top_trending"
        ),
    ).split(",")
    if table.strip()
)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def table_ref(table_name: str) -> str:
    return f'public.{quote_identifier(table_name)}'


def _get_public_table_columns(connection: Connection, table_name: str) -> set[str]:
    result = connection.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :table_name
            """
        ),
        {"table_name": table_name},
    ).scalars()
    return set(result)


def _resolve_column(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    return next((column for column in candidates if column in columns), None)


def _qualified_column(alias: str, column: str) -> str:
    return f"{alias}.{quote_identifier(column)}"


def get_dim_product_price_expr(
    connection: Connection,
    alias: str | None = None,
    coalesce: bool = False,
) -> str:
    columns = _get_public_table_columns(connection, "dim_products")
    price_col = _resolve_column(columns, PRODUCT_PRICE_COLUMNS)
    if not price_col:
        return "0.0"

    price_expr = _qualified_column(alias, price_col) if alias else quote_identifier(price_col)
    return f"COALESCE({price_expr}, 0.0)" if coalesce else price_expr


def _get_dim_product_lookup_sql(
    connection: Connection,
    product_expr: str,
    alias: str = "p",
) -> tuple[str, str, str, str]:
    columns = _get_public_table_columns(connection, "dim_products")
    product_id_col = _resolve_column(columns, PRODUCT_ID_COLUMNS)
    if not product_id_col:
        return "", "0.0", "NULL", "'Recommended'"

    join_clause = (
        f"LEFT JOIN {table_ref('dim_products')} {alias} "
        f"ON {product_expr}::text = {_qualified_column(alias, product_id_col)}::text"
    )
    price_expr = get_dim_product_price_expr(connection, alias=alias, coalesce=True)

    name_col = _resolve_column(columns, PRODUCT_NAME_COLUMNS)
    name_expr = _qualified_column(alias, name_col) if name_col else "NULL"

    category_col = _resolve_column(columns, PRODUCT_CATEGORY_COLUMNS)
    category_expr = _qualified_column(alias, category_col) if category_col else "'Recommended'"

    return join_clause, price_expr, name_expr, category_expr


def _normalize_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    score = row.get("cluster_total_score", 0.0)
    if isinstance(score, Decimal):
        score = float(score)

    price = row.get("price", 0.0)
    if isinstance(price, Decimal):
        price = float(price)

    product_id = row.get("product_id")

    return {
        "id": str(product_id), 
        "name": str(row.get("display_name") or f"Product {product_id}"),
        "price": float(price or 0.0),
        "category": str(row.get("category_name") or "Recommended"),
        "cluster_total_score": float(score or 0.0),
    }


def _normalize_recommendation_response(row: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_recommendation(row)
    display_name = str(row.get("display_name") or normalized["name"])
    product_id = row.get("product_id")
    cluster_id = row.get("cluster_id", -1)
    try:
        response_product_id = int(product_id) if product_id is not None else 0
    except (TypeError, ValueError):
        response_product_id = product_id

    return {
        "cluster_id": int(cluster_id) if cluster_id is not None else -1,
        "product_id": response_product_id,
        "display_name": display_name,
        "cluster_total_score": normalized["cluster_total_score"],
        **normalized,
    }


def _merge_recommendations(
    existing: dict[int, dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> None:
    for row in incoming:
        normalized = _normalize_recommendation(row) 
        product_id = int(normalized["id"]) 
        current = existing.get(product_id)
        if current is None or normalized["cluster_total_score"] > current["cluster_total_score"]:
            existing[product_id] = normalized


def _fetch_user_recommendations_from_als(
    connection: Connection,
    user_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    columns = _get_public_table_columns(connection, "serving_als")
    if not columns:
        return []

    user_col = _resolve_column(columns, ("user_id",))
    product_col = _resolve_column(columns, ("product_id",))
    score_col = _resolve_column(columns, ("score", "prediction", "rating_prediction"))
    display_col = _resolve_column(columns, ("display_name", "product_name"))
    rank_col = _resolve_column(columns, ("rank",))

    if not user_col or not product_col or not score_col:
        return []

    product_expr = _qualified_column("r", product_col)
    score_expr = _qualified_column("r", score_col)
    product_join, price_expr, product_name_expr, category_expr = _get_dim_product_lookup_sql(
        connection,
        product_expr,
    )
    source_display_expr = _qualified_column("r", display_col) if display_col else "NULL"
    display_expr = f"COALESCE({source_display_expr}, {product_name_expr}, 'Product ' || {product_expr}::text)"
    order_clause = (
        f"{score_expr} DESC, {_qualified_column('r', rank_col)} ASC"
        if rank_col
        else f"{score_expr} DESC"
    )

    query = text(
        f"""
        SELECT
            -1 AS cluster_id,
            {product_expr} AS product_id,
            {display_expr} AS display_name,
            {score_expr} AS cluster_total_score,
            {price_expr} AS price,
            {category_expr} AS category_name
        FROM {table_ref("serving_als")} r
        {product_join}
        WHERE {_qualified_column("r", user_col)} = :user_id
          AND {product_expr} IS NOT NULL
          AND {score_expr} IS NOT NULL
        ORDER BY {order_clause}
        LIMIT :limit
        """
    )
    result = connection.execute(query, {"user_id": user_id, "limit": limit}).mappings().all()
    return [dict(row) for row in result]


def _fetch_user_recommendations_from_content_based(
    connection: Connection,
    user_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    columns = _get_public_table_columns(connection, "serving_content_based")
    if not columns:
        return []

    # Some deployments include user_id on serving_content_based, some do not.
    user_col = _resolve_column(columns, ("user_id",))
    product_col = _resolve_column(
        columns,
        ("product_id", "recommended_product_id", "similar_product_id", "target_product_id"),
    )
    score_col = _resolve_column(columns, ("score", "similarity_score", "cosine_similarity"))
    display_col = _resolve_column(columns, ("display_name", "product_name", "recommended_display_name"))
    rank_col = _resolve_column(columns, ("rank",))

    if not user_col or not product_col or not score_col:
        return []

    product_expr = _qualified_column("r", product_col)
    score_expr = _qualified_column("r", score_col)
    product_join, price_expr, product_name_expr, category_expr = _get_dim_product_lookup_sql(
        connection,
        product_expr,
    )
    source_display_expr = _qualified_column("r", display_col) if display_col else "NULL"
    display_expr = f"COALESCE({source_display_expr}, {product_name_expr}, 'Product ' || {product_expr}::text)"
    order_clause = (
        f"{score_expr} DESC, {_qualified_column('r', rank_col)} ASC"
        if rank_col
        else f"{score_expr} DESC"
    )

    query = text(
        f"""
        SELECT
            -1 AS cluster_id,
            {product_expr} AS product_id,
            {display_expr} AS display_name,
            {score_expr} AS cluster_total_score,
            {price_expr} AS price,
            {category_expr} AS category_name
        FROM {table_ref("serving_content_based")} r
        {product_join}
        WHERE {_qualified_column("r", user_col)} = :user_id
          AND {product_expr} IS NOT NULL
          AND {score_expr} IS NOT NULL
        ORDER BY {order_clause}
        LIMIT :limit
        """
    )
    result = connection.execute(query, {"user_id": user_id, "limit": limit}).mappings().all()
    return [dict(row) for row in result]


def get_content_based_recommendations(
    product_id: str,
    limit: int = 4,
) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        columns = _get_public_table_columns(connection, "serving_content_based")
        if not columns:
            return []

        source_col = _resolve_column(columns, ("source_product_id", "product_id"))
        product_col = _resolve_column(
            columns,
            ("recommended_product_id", "similar_product_id", "target_product_id"),
        )
        score_col = _resolve_column(columns, ("score", "similarity_score", "cosine_similarity"))
        display_col = _resolve_column(columns, ("display_name", "product_name", "recommended_display_name"))
        rank_col = _resolve_column(columns, ("rank", "recommendation_rank"))

        if not source_col or not product_col or not score_col:
            return []

        source_expr = _qualified_column("r", source_col)
        product_expr = _qualified_column("r", product_col)
        score_expr = _qualified_column("r", score_col)
        product_join, price_expr, product_name_expr, category_expr = _get_dim_product_lookup_sql(
            connection,
            product_expr,
        )
        source_display_expr = _qualified_column("r", display_col) if display_col else "NULL"
        display_expr = f"COALESCE({source_display_expr}, {product_name_expr}, 'Product ' || {product_expr}::text)"
        order_clause = (
            f"{score_expr} DESC, {_qualified_column('r', rank_col)} ASC"
            if rank_col
            else f"{score_expr} DESC"
        )

        query = text(
            f"""
            SELECT
                -1 AS cluster_id,
                {product_expr} AS product_id,
                {display_expr} AS display_name,
                {score_expr} AS cluster_total_score,
                {price_expr} AS price,
                {category_expr} AS category_name
            FROM {table_ref("serving_content_based")} r
            {product_join}
            WHERE {source_expr}::text = :product_id
              AND {product_expr} IS NOT NULL
              AND {score_expr} IS NOT NULL
            ORDER BY {order_clause}
            LIMIT :limit
            """
        )
        rows = connection.execute(
            query,
            {"product_id": product_id, "limit": limit},
        ).mappings().all()
        return [_normalize_recommendation_response(dict(row)) for row in rows]


def get_item_based_recommendations(
    product_ids: list[str],
    limit: int = 4,
) -> list[dict[str, Any]]:
    cart_product_ids = [
        str(product_id).strip()
        for product_id in product_ids
        if product_id and str(product_id).strip()
    ]
    if not cart_product_ids:
        return []

    with engine.connect() as connection:
        columns = _get_public_table_columns(connection, "serving_item_based")
        if not columns:
            return []

        source_col = _resolve_column(columns, ("source_product_id", "product_id"))
        product_col = _resolve_column(
            columns,
            ("similar_product_id", "recommended_product_id", "target_product_id"),
        )
        score_col = _resolve_column(columns, ("score", "similarity_score", "cosine_similarity"))
        display_col = _resolve_column(
            columns,
            ("similar_display_name", "recommended_display_name", "display_name", "product_name"),
        )
        frequency_col = _resolve_column(columns, ("co_interaction_count", "frequency", "count"))
        rank_col = _resolve_column(columns, ("rank", "recommendation_rank"))

        if not source_col or not product_col or not score_col:
            return []

        source_expr = _qualified_column("r", source_col)
        product_expr = _qualified_column("r", product_col)
        score_expr = _qualified_column("r", score_col)
        product_join, price_expr, product_name_expr, category_expr = _get_dim_product_lookup_sql(
            connection,
            product_expr,
        )
        source_display_expr = f"{_qualified_column('r', display_col)}::text" if display_col else "NULL::text"
        display_expr = f"COALESCE(MAX({source_display_expr}), {product_name_expr}, 'Product ' || {product_expr}::text)"

        order_terms = [f"MAX({score_expr}) DESC"]
        if frequency_col:
            order_terms.append(f"SUM({_qualified_column('r', frequency_col)}) DESC")
        if rank_col:
            order_terms.append(f"MIN({_qualified_column('r', rank_col)}) ASC")
        order_terms.append(f"{product_expr} ASC")
        order_clause = ", ".join(order_terms)

        query = text(
            f"""
            SELECT
                -1 AS cluster_id,
                {product_expr} AS product_id,
                {display_expr} AS display_name,
                MAX({score_expr}) AS cluster_total_score,
                {price_expr} AS price,
                {category_expr} AS category_name
            FROM {table_ref("serving_item_based")} r
            {product_join}
            WHERE {source_expr}::text IN :source_product_ids
              AND {product_expr} IS NOT NULL
              AND {product_expr}::text NOT IN :excluded_product_ids
              AND {score_expr} IS NOT NULL
            GROUP BY {product_expr}, {product_name_expr}, {price_expr}, {category_expr}
            ORDER BY {order_clause}
            LIMIT :limit
            """
        ).bindparams(
            bindparam("source_product_ids", expanding=True),
            bindparam("excluded_product_ids", expanding=True),
        )
        rows = connection.execute(
            query,
            {
                "source_product_ids": cart_product_ids,
                "excluded_product_ids": cart_product_ids,
                "limit": max(1, limit),
            },
        ).mappings().all()
        return [_normalize_recommendation_response(dict(row)) for row in rows]


def _fetch_user_level_recommendations(
    connection: Connection,
    user_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    merged: dict[int, dict[str, Any]] = {}
    _merge_recommendations(merged, _fetch_user_recommendations_from_als(connection, user_id, limit))
    _merge_recommendations(
        merged,
        _fetch_user_recommendations_from_content_based(connection, user_id, limit),
    )
    recommendations = sorted(
        merged.values(),
        key=lambda row: row["cluster_total_score"],
        reverse=True,
    )
    return recommendations[:limit]


def _fetch_user_cluster_id(connection: Connection, user_id: str) -> int | None:
    columns = _get_public_table_columns(connection, "serving_user_clusters")
    if not columns:
        return None

    user_col = _resolve_column(columns, ("user_id",))
    cluster_col = _resolve_column(columns, ("cluster_id",))
    if not user_col or not cluster_col:
        return None

    cluster_id = connection.execute(
        text(
            f"""
            SELECT {quote_identifier(cluster_col)}
            FROM {table_ref("serving_user_clusters")}
            WHERE {quote_identifier(user_col)} = :user_id
            LIMIT 1
            """
        ),
        {"user_id": user_id},
    ).scalar_one_or_none()
    return int(cluster_id) if cluster_id is not None else None


def _fetch_cluster_recommendations(
    connection: Connection,
    cluster_id: int,
    limit: int,
) -> list[dict[str, Any]]:
    columns = _get_public_table_columns(connection, "serving_recommendations")
    if not columns:
        return []

    cluster_col = _resolve_column(columns, ("cluster_id",))
    product_col = _resolve_column(columns, ("product_id",))
    display_col = _resolve_column(columns, ("display_name", "product_name"))
    score_col = _resolve_column(columns, ("cluster_total_score", "score"))

    if not cluster_col or not product_col or not display_col or not score_col:
        return []

    product_expr = _qualified_column("r", product_col)
    score_expr = _qualified_column("r", score_col)
    product_join, price_expr, product_name_expr, category_expr = _get_dim_product_lookup_sql(
        connection,
        product_expr,
    )
    display_expr = (
        f"COALESCE({_qualified_column('r', display_col)}, "
        f"{product_name_expr}, 'Product ' || {product_expr}::text)"
    )

    query = text(
        f"""
        SELECT
            {_qualified_column("r", cluster_col)} AS cluster_id,
            {product_expr} AS product_id,
            {display_expr} AS display_name,
            {score_expr} AS cluster_total_score,
            {price_expr} AS price,
            {category_expr} AS category_name
        FROM {table_ref("serving_recommendations")} r
        {product_join}
        WHERE {_qualified_column("r", cluster_col)} = :cluster_id
          AND {product_expr} IS NOT NULL
          AND {score_expr} IS NOT NULL
        ORDER BY {score_expr} DESC
        LIMIT :limit
        """
    )
    result = connection.execute(query, {"cluster_id": cluster_id, "limit": limit}).mappings().all()
    return [_normalize_recommendation(dict(row)) for row in result]


def _fetch_global_top_recommendations(connection: Connection, limit: int) -> list[dict[str, Any]]:
    for table_name in GLOBAL_TOP_TABLE_CANDIDATES:
        columns = _get_public_table_columns(connection, table_name)
        if not columns:
            continue

        product_col = _resolve_column(columns, ("product_id", "recommended_product_id"))
        display_col = _resolve_column(columns, ("display_name", "product_name", "recommended_display_name"))
        score_col = _resolve_column(
            columns,
            ("cluster_total_score", "score", "popularity_score", "interaction_count", "event_count"),
        )
        rank_col = _resolve_column(columns, ("rank",))

        if not product_col:
            continue

        product_expr = _qualified_column("r", product_col)
        product_join, price_expr, product_name_expr, category_expr = _get_dim_product_lookup_sql(
            connection,
            product_expr,
        )
        source_display_expr = _qualified_column("r", display_col) if display_col else "NULL"
        display_expr = f"COALESCE({source_display_expr}, {product_name_expr}, 'Product ' || {product_expr}::text)"
        score_expr = _qualified_column("r", score_col) if score_col else "0.0"

        if score_col:
            order_clause = f"{_qualified_column('r', score_col)} DESC"
        elif rank_col:
            order_clause = f"{_qualified_column('r', rank_col)} ASC"
        else:
            order_clause = f"{product_expr} ASC"

        query = text(
            f"""
            SELECT
                -1 AS cluster_id,
                {product_expr} AS product_id,
                {display_expr} AS display_name,
                {score_expr} AS cluster_total_score,
                {price_expr} AS price,
                {category_expr} AS category_name
            FROM {table_ref(table_name)} r
            {product_join}
            WHERE {product_expr} IS NOT NULL
            ORDER BY {order_clause}
            LIMIT :limit
            """
        )
        rows = connection.execute(query, {"limit": limit}).mappings().all()
        if rows:
            return [_normalize_recommendation(dict(row)) for row in rows]

    return []


def _fetch_emergency_recommendations(connection: Connection, limit: int) -> list[dict[str, Any]]:
    columns = _get_public_table_columns(connection, "serving_recommendations")
    if not columns:
        return []

    cluster_col = _resolve_column(columns, ("cluster_id",))
    product_col = _resolve_column(columns, ("product_id",))
    display_col = _resolve_column(columns, ("display_name", "product_name"))
    score_col = _resolve_column(columns, ("cluster_total_score", "score"))

    if not cluster_col or not product_col or not display_col or not score_col:
        return []

    product_expr = _qualified_column("r", product_col)
    score_expr = _qualified_column("r", score_col)
    product_join, price_expr, product_name_expr, category_expr = _get_dim_product_lookup_sql(
        connection,
        product_expr,
    )
    display_expr = (
        f"COALESCE({_qualified_column('r', display_col)}, "
        f"{product_name_expr}, 'Product ' || {product_expr}::text)"
    )

    query = text(
        f"""
        SELECT
            {_qualified_column("r", cluster_col)} AS cluster_id,
            {product_expr} AS product_id,
            {display_expr} AS display_name,
            {score_expr} AS cluster_total_score,
            {price_expr} AS price,
            {category_expr} AS category_name
        FROM {table_ref("serving_recommendations")} r
        {product_join}
        WHERE {product_expr} IS NOT NULL
          AND {score_expr} IS NOT NULL
        ORDER BY {score_expr} DESC
        LIMIT :limit
        """
    )
    rows = connection.execute(query, {"limit": limit}).mappings().all()
    return [_normalize_recommendation(dict(row)) for row in rows]


def get_recommendations_with_fallback(user_id: str, limit: int = MAX_RECOMMENDATIONS) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        user_level = _fetch_user_level_recommendations(connection, user_id, limit)
        if user_level:
            return user_level

        cluster_id = _fetch_user_cluster_id(connection, user_id)
        if cluster_id is not None:
            cluster_level = _fetch_cluster_recommendations(connection, cluster_id, limit)
            if cluster_level:
                return cluster_level

        global_top = _fetch_global_top_recommendations(connection, limit)
        if global_top:
            return global_top

        return _fetch_emergency_recommendations(connection, limit)


def get_recommendations_from_db(user_id: str) -> list[dict[str, Any]]:
    # Backward-compatible alias for existing imports/tests.
    return get_recommendations_with_fallback(user_id)


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return int(value)
    return int(value)


def _to_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "uncategorized"


def _escape_ilike_term(value: str) -> str:
    return (
        value.replace("!", "!!")
        .replace("%", "!%")
        .replace("_", "!_")
    )


def _normalize_text(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    normalized = str(value).strip()
    return normalized or fallback


def get_categories_from_db() -> list[dict[str, str]]:
    with engine.connect() as connection:
        columns = _get_public_table_columns(connection, "dim_products")
        if not columns:
            return [{"id": "all", "name": "All"}]

        category_col = _resolve_column(columns, ("category_main", "category_fixed", "category"))
        if not category_col:
            return [{"id": "all", "name": "All"}]

        category_expr = quote_identifier(category_col)
        rows = connection.execute(
            text(
                f"""
                SELECT DISTINCT {category_expr} AS category_name
                FROM {table_ref("dim_products")}
                WHERE {category_expr} IS NOT NULL
                  AND TRIM({category_expr}::text) <> ''
                ORDER BY category_name
                """
            )
        ).scalars().all()

        categories = [{"id": "all", "name": "All"}]
        categories.extend(
            {
                "id": _slugify(str(category_name)),
                "name": str(category_name),
            }
            for category_name in rows
        )
        return categories


def get_categories_and_brands_from_db() -> list[dict[str, Any]]:
    with engine.connect() as connection:
        columns = _get_public_table_columns(connection, "dim_products")
        if not columns:
            return []

        category_col = _resolve_column(columns, ("category_main", "category_fixed", "category"))
        category_sub_col = _resolve_column(columns, ("category_sub", "subcategory", "category_level_2"))
        category_detail_col = _resolve_column(
            columns,
            ("category_detail", "category_level_3", "category_leaf"),
        )
        brand_col = _resolve_column(columns, ("brand", "brand_fixed"))
        if not category_col or not category_sub_col or not category_detail_col or not brand_col:
            return []

        category_expr = quote_identifier(category_col)
        category_sub_expr = quote_identifier(category_sub_col)
        category_detail_expr = quote_identifier(category_detail_col)
        brand_expr = quote_identifier(brand_col)

        rows = connection.execute(
            text(
                f"""
                SELECT
                    {category_expr} AS category_main,
                    {category_sub_expr} AS category_sub,
                    {category_detail_expr} AS category_detail,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT {brand_expr}), NULL) AS brands
                FROM {table_ref("dim_products")}
                WHERE {category_expr} IS NOT NULL
                GROUP BY {category_expr}, {category_sub_expr}, {category_detail_expr}
                ORDER BY {category_expr}, {category_sub_expr}, {category_detail_expr}
                """
            )
        ).mappings().all()

        hierarchy: dict[str, dict[str, Any]] = {}
        for row in rows:
            main_name = _normalize_text(row.get("category_main"), "Uncategorized")
            sub_name = _normalize_text(row.get("category_sub"), "General")
            detail_name = _normalize_text(row.get("category_detail"), "General")
            row_brands = sorted(
                {
                    _normalize_text(brand, "")
                    for brand in (row.get("brands") or [])
                    if _normalize_text(brand, "")
                }
            )

            main_entry = hierarchy.setdefault(
                main_name,
                {
                    "category_main": main_name,
                    "brands": set(),
                    "subcategories": {},
                },
            )
            main_entry["brands"].update(row_brands)

            subcategories = main_entry["subcategories"]
            sub_entry = subcategories.setdefault(
                sub_name,
                {
                    "category_sub": sub_name,
                    "brands": set(),
                    "details": {},
                },
            )
            sub_entry["brands"].update(row_brands)

            details = sub_entry["details"]
            detail_entry = details.setdefault(
                detail_name,
                {
                    "category_detail": detail_name,
                    "brands": set(),
                },
            )
            detail_entry["brands"].update(row_brands)

        result: list[dict[str, Any]] = []
        for main_name in sorted(hierarchy):
            main_entry = hierarchy[main_name]
            sub_list: list[dict[str, Any]] = []
            for sub_name in sorted(main_entry["subcategories"]):
                sub_entry = main_entry["subcategories"][sub_name]
                detail_list = []
                for detail_name in sorted(sub_entry["details"]):
                    detail_entry = sub_entry["details"][detail_name]
                    detail_list.append(
                        {
                            "category_detail": detail_entry["category_detail"],
                            "brands": sorted(detail_entry["brands"]),
                        }
                    )
                sub_list.append(
                    {
                        "category_sub": sub_entry["category_sub"],
                        "brands": sorted(sub_entry["brands"]),
                        "details": detail_list,
                    }
                )

            result.append(
                {
                    "category_main": main_entry["category_main"],
                    "brands": sorted(main_entry["brands"]),
                    "subcategories": sub_list,
                }
            )

        return result


def get_products_from_db(
    selected_category: str = "all",
    selected_category_main: str | None = None,
    selected_category_sub: str | None = None,
    selected_category_detail: str | None = None,
    selected_brands: list[str] | None = None,
    search_query: str | None = None,
    limit: int = 120,
) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        columns = _get_public_table_columns(connection, "dim_products")
        if not columns:
            return []

        product_id_col = _resolve_column(columns, PRODUCT_ID_COLUMNS)
        name_col = _resolve_column(columns, PRODUCT_NAME_COLUMNS)
        category_col = _resolve_column(columns, PRODUCT_CATEGORY_COLUMNS)
        category_sub_col = _resolve_column(columns, ("category_sub", "subcategory", "category_level_2"))
        category_detail_col = _resolve_column(
            columns,
            ("category_detail", "category_level_3", "category_leaf"),
        )
        brand_col = _resolve_column(columns, ("brand", "brand_fixed"))
        original_price_col = _resolve_column(columns, ("original_price", "msrp", "list_price"))
        rating_col = _resolve_column(columns, ("rating", "avg_rating"))
        review_count_col = _resolve_column(columns, ("review_count", "rating_count"))
        in_stock_col = _resolve_column(columns, ("in_stock", "is_available"))

        if not product_id_col or not name_col or not category_col:
            return []

        product_id_expr = quote_identifier(product_id_col)
        name_expr = quote_identifier(name_col)
        category_expr = quote_identifier(category_col)
        category_sub_expr = quote_identifier(category_sub_col) if category_sub_col else "NULL"
        category_detail_expr = quote_identifier(category_detail_col) if category_detail_col else "NULL"
        brand_expr = quote_identifier(brand_col) if brand_col else "NULL"
        price_expr = get_dim_product_price_expr(connection)
        original_price_expr = quote_identifier(original_price_col) if original_price_col else "NULL"
        rating_expr = quote_identifier(rating_col) if rating_col else "4.5"
        review_expr = quote_identifier(review_count_col) if review_count_col else "100"
        stock_expr = quote_identifier(in_stock_col) if in_stock_col else "TRUE"

        where_clauses: list[str] = []
        params: dict[str, Any] = {"limit": limit}
        if selected_category_detail and selected_category_detail.lower() != "all" and category_detail_col:
            where_clauses.append(f"LOWER({category_detail_expr}) = LOWER(:selected_category_detail)")
            params["selected_category_detail"] = selected_category_detail
        elif selected_category_sub and selected_category_sub.lower() != "all" and category_sub_col:
            where_clauses.append(f"LOWER({category_sub_expr}) = LOWER(:selected_category_sub)")
            params["selected_category_sub"] = selected_category_sub
        elif selected_category_main and selected_category_main.lower() != "all":
            where_clauses.append(f"LOWER({category_expr}) = LOWER(:selected_category_main)")
            params["selected_category_main"] = selected_category_main
        elif selected_category.lower() != "all":
            where_clauses.append(f"LOWER({category_expr}) = LOWER(:selected_category)")
            params["selected_category"] = selected_category

        normalized_brands = [
            brand.strip()
            for brand in (selected_brands or [])
            if brand and brand.strip() and brand.strip().lower() != "all"
        ]
        if normalized_brands and brand_col:
            brand_placeholders = []
            for index, brand in enumerate(normalized_brands):
                param_name = f"selected_brand_{index}"
                params[param_name] = brand.lower()
                brand_placeholders.append(f":{param_name}")
            where_clauses.append(
                f"LOWER({brand_expr}) IN ({', '.join(brand_placeholders)})"
            )

        search_terms: list[str] = []
        if search_query:
            search_terms = [
                term.strip()
                for term in search_query.strip().split()
                if term.strip()
            ]
        for index, term in enumerate(search_terms):
            param_name = f"search_term_{index}"
            params[param_name] = f"%{_escape_ilike_term(term)}%"
            where_clauses.append(f"{name_expr} ILIKE :{param_name} ESCAPE '!'")

        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        rows = connection.execute(
            text(
                f"""
                SELECT
                    {product_id_expr} AS product_id,
                    {name_expr} AS display_name,
                    {price_expr} AS price,
                    {original_price_expr} AS original_price,
                    {rating_expr} AS rating,
                    {review_expr} AS review_count,
                    {category_expr} AS category_name,
                    {category_sub_expr} AS category_sub_name,
                    {category_detail_expr} AS category_detail_name,
                    {brand_expr} AS brand_name,
                    {stock_expr} AS in_stock
                FROM {table_ref("dim_products")}
                {where_clause}
                ORDER BY {product_id_expr} DESC
                LIMIT :limit
                """
            ),
            params,
        ).mappings().all()

        products: list[dict[str, Any]] = []
        for row in rows:
            product_id = _to_int(row.get("product_id"))
            price = _to_float(row.get("price"), default=0.0)
            original_price_raw = row.get("original_price")
            original_price = _to_float(original_price_raw, default=0.0) if original_price_raw is not None else None
            rating = _to_float(row.get("rating"), default=4.5)
            review_count = max(0, _to_int(row.get("review_count"), default=100))

            products.append(
                {
                    "id": str(product_id),
                    "name": str(row.get("display_name") or f"Product {product_id}"),
                    "price": price,
                    "originalPrice": original_price if (original_price and original_price > price) else None,
                    "rating": max(0.0, min(5.0, rating)),
                    "reviewCount": review_count,
                    "category": str(row.get("category_name") or "Uncategorized"),
                    "categorySub": str(row.get("category_sub_name") or ""),
                    "categoryDetail": str(row.get("category_detail_name") or ""),
                    "brand": str(row.get("brand_name") or "Unknown"),
                    "inStock": _to_bool(row.get("in_stock"), default=True),
                }
            )

        return products
