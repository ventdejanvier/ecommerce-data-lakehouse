import logging
import os
from pathlib import Path
import threading
from contextlib import asynccontextmanager
from typing import Any
from dotenv import load_dotenv

# MUST BE CALLED BEFORE ANY LOCAL IMPORTS
# Force absolute path to the backend directory's .env file
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

from sqlalchemy import text
from database import engine
from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from database import (
    get_categories_and_brands_from_db,
    get_categories_from_db,
    get_content_based_recommendations,
    get_dim_product_price_expr,
    get_item_based_recommendations,
    get_products_from_db,
    get_recent_category_candidates,
    get_recommendations_by_strategy,
    get_recommendations_with_fallback,
)
from kafka_producer import produce_event
from recommendation_scoring import (
    SCORING_CONFIG,
    normalize_category,
    purchase_completed_category_updates,
    rerank_home_candidates_with_recent_categories,
    resolve_recent_event_weight,
)
from redis_client import increment_category_score, get_category_scores
from schemas import (
    CartRecommendRequest,
    CategoryResponse,
    ProductResponse,
    RecommendationResponse,
)

logger = logging.getLogger(__name__)

HOME_RECOMMENDATION_RETURN_LIMIT = 10
HOME_RECOMMENDATION_CANDIDATE_LIMIT = 50
HOME_RECOMMENDATION_MAX_RECENT_CATEGORY_ITEMS = 4
HOME_RECOMMENDATION_MAX_ITEMS_PER_CATEGORY = 6
RECENT_CATEGORY_MAX_CATEGORIES = 2
RECENT_CATEGORY_CANDIDATES_PER_CATEGORY = 6
RECENT_CATEGORY_TOTAL_CANDIDATE_LIMIT = 12


def warmup_database_caches():
    logger.info("Starting database cache warmup...")
    try:
        get_categories_from_db()
        get_global_recommendations()
        demo_personas = [
            "1515915625355805313",  # Champions
            "1515915625353561691",  # Loyal
            "1515915625353226922",  # At Risk
            "1515915625353236157",  # Browsers
        ]
        for pid in demo_personas:
            get_recommendations_by_strategy(pid, "als")
            get_recommendations_by_strategy(pid, "cluster")
        get_products_from_db(limit=1)
        logger.info("Database cache warmup completed.")
    except Exception as e:
        logger.warning("Database cache warmup failed: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the warmup in a separate daemon thread so it doesn't block startup
    threading.Thread(target=warmup_database_caches, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)

# Backward-compatible name for tests/legacy monkeypatching.
get_recommendations_from_db = get_recommendations_with_fallback

SESSION_CATEGORY_FIELDS = (
    "category_main",
    "category",
    "productCategory",
    "product_category",
    "categoryName",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/recommend/global")
def get_global_recommendations():
    from database import engine 
    from sqlalchemy import text
    try:
        with engine.connect() as connection:
            price_expr = get_dim_product_price_expr(connection, alias="p", coalesce=True)
            query = f"""
            SELECT 
                g.product_id::text AS id,
                p.display_name AS name,
                {price_expr} AS price,
                p.category_main AS category,
                10.0 AS cluster_total_score
            FROM public.global_popular g
            LEFT JOIN public.dim_products p ON g.product_id = p.product_id
            LIMIT 10
            """
            result = connection.execute(text(query)).mappings().all()
            return [dict(row) for row in result]
    except Exception as e:
        # Fallback: Nếu bảng global_popular lỗi, tự động lấy 10 sản phẩm ngẫu nhiên
        with engine.connect() as connection:
            price_expr = get_dim_product_price_expr(connection, coalesce=True)
            query = f"""
            SELECT 
                product_id::text AS id,
                display_name AS name,
                {price_expr} AS price,
                category_main AS category,
                10.0 AS cluster_total_score
            FROM public.dim_products
            LIMIT 10
            """
            result = connection.execute(text(query)).mappings().all()
            return [dict(row) for row in result]

@app.get("/api/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}

@app.post("/api/admin/showtime")
def set_showtime_mode(is_active: bool = Query(...)):
    import urllib.request
    import json
    import base64
    
    url = "http://airflow-webserver:8080/api/v1/dags/transform_gold_dag"
    payload = json.dumps({"is_paused": not is_active}).encode("utf-8")
    
    req = urllib.request.Request(url, data=payload, method="PATCH")
    req.add_header("Content-Type", "application/json")
    
    # Basic Auth: admin:admin
    auth_str = "admin:admin"
    auth_b64 = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
    req.add_header("Authorization", f"Basic {auth_b64}")
    
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status in (200, 201, 202, 204):
                return {"status": "success", "is_active": is_active}
            else:
                return {"status": "error", "message": f"Airflow returned status {response.status}"}
    except Exception as e:
        logger.warning("Failed to contact Airflow API: %s", e)
        return {"status": "error", "message": f"Could not connect to Airflow: {str(e)}"}


def send_to_kafka_background(data: dict[str, Any]) -> None:
    try:
        # Giữ nguyên logic producer hiện tại và chạy ở background.
        produce_event("ecommerce-raw-events", data)
    except Exception as e:
        print(f"Kafka tạm thời không kết nối được (không sao cả): {e}")


def extract_session_category(data: dict[str, Any]) -> str | None:
    for field in SESSION_CATEGORY_FIELDS:
        category = data.get(field)
        canonical_category = normalize_category(category)
        if canonical_category:
            return canonical_category

    return None


def capture_recent_category(data: dict[str, Any]) -> None:
    try:
        event_type = str(data.get("eventType", "")).strip()
        purchase_updates = purchase_completed_category_updates(data)
        if purchase_updates:
            category_updates = purchase_updates
        else:
            score = resolve_recent_event_weight(event_type, data.get("action"))
            category = extract_session_category(data)
            category_updates = [(category, score)] if category and score is not None else []

        if not category_updates:
            return

        user_id = data.get("userId")
        normalized_user_id = str(user_id).strip() if user_id is not None else ""
        if not normalized_user_id:
            logger.info(
                "Skipping recent category capture: missing userId eventId=%s",
                data.get("eventId"),
            )
            return

        for category, score in category_updates:
            canonical_category = normalize_category(category)
            if not canonical_category:
                continue
            try:
                increment_category_score(normalized_user_id, canonical_category, score)
            except Exception as exc:
                logger.warning(
                    "Failed to increment recent category: userId=%s category=%s error=%s",
                    normalized_user_id,
                    canonical_category,
                    exc,
                )
    except Exception as exc:
        logger.warning("Failed to capture recent category: %s", exc)


def normalize_category_for_reranking(value: Any) -> str:
    return normalize_category(value)


def apply_category_reranking(
    items: list[dict[str, Any]],
    category_scores: dict[str, float],
) -> list[dict[str, Any]]:
    return rerank_home_candidates_with_recent_categories(
        items,
        category_scores,
        fallback_config=SCORING_CONFIG,
    )


def rerank_by_recent_categories(
    products: list[dict[str, Any]],
    category_scores: dict[str, float],
) -> list[dict[str, Any]]:
    return apply_category_reranking(products, category_scores)


def select_home_recommendation_mix(
    reranked_products: list[dict[str, Any]],
    return_limit: int = HOME_RECOMMENDATION_RETURN_LIMIT,
    max_recent_category_items: int = HOME_RECOMMENDATION_MAX_RECENT_CATEGORY_ITEMS,
    max_items_per_category: int = HOME_RECOMMENDATION_MAX_ITEMS_PER_CATEGORY,
) -> list[dict[str, Any]]:
    """Apply recent-candidate and display-category caps with stable backfill."""
    if return_limit <= 0:
        return []

    recent_limit = max(0, max_recent_category_items)
    category_limit = max_items_per_category if max_items_per_category > 0 else None
    unique_products: list[dict[str, Any]] = []
    seen_product_ids: set[str] = set()

    for product in reranked_products:
        product_id = product.get("id") or product.get("product_id")
        if product_id is None:
            continue
        product_key = str(product_id)
        if product_key in seen_product_ids:
            continue
        seen_product_ids.add(product_key)
        unique_products.append(product)

    primary_candidates: list[dict[str, Any]] = []
    recent_backfill: list[dict[str, Any]] = []
    recent_count = 0
    for product in unique_products:
        if (
            product.get("candidate_source") == "recent_category"
            and recent_count >= recent_limit
        ):
            recent_backfill.append(product)
            continue
        primary_candidates.append(product)
        if product.get("candidate_source") == "recent_category":
            recent_count += 1

    selected: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}

    def select_with_category_cap(
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        skipped: list[dict[str, Any]] = []
        for product in candidates:
            if len(selected) >= return_limit:
                break
            display_category = normalize_category(
                product.get("category")
                or product.get("category_main")
                or product.get("recent_match_category")
            )
            if (
                category_limit is not None
                and display_category
                and category_counts.get(display_category, 0) >= category_limit
            ):
                skipped.append(product)
                continue
            selected.append(product)
            if display_category:
                category_counts[display_category] = (
                    category_counts.get(display_category, 0) + 1
                )
        return skipped

    skipped_primary = select_with_category_cap(primary_candidates)
    if len(selected) >= return_limit:
        return selected
    if len(primary_candidates) >= return_limit:
        for product in skipped_primary:
            selected.append(product)
            if len(selected) >= return_limit:
                return selected

    skipped_recent = select_with_category_cap(recent_backfill)
    if len(selected) >= return_limit:
        return selected
    for product in [*skipped_primary, *skipped_recent]:
        selected.append(product)
        if len(selected) >= return_limit:
            break

    return selected


@app.post("/api/track")
def track_event(data: dict[str, Any], background_tasks: BackgroundTasks) -> dict[str, str]:
    # Đẩy tác vụ gửi vào Kafka ra background để API phản hồi ngay.
    event_data = dict(data)
    background_tasks.add_task(capture_recent_category, event_data)
    background_tasks.add_task(send_to_kafka_background, event_data)
    return {"status": "ok", "eventId": str(event_data.get("eventId", ""))}

@app.get("/api/recommend/home/{user_id}")
def get_home_recommendations(
    user_id: str,
    is_ml_enabled: bool = False,
    strategy: str = Query(default="als"),
):
    if not is_ml_enabled:
        # Gọi tên hàm lấy dữ liệu global
        products = list(get_global_recommendations())
    else:
        # Gọi tên hàm lấy dữ liệu cá nhân hóa
        products = list(
            get_recommendations_by_strategy(
                user_id,
                strategy,
                limit=HOME_RECOMMENDATION_CANDIDATE_LIMIT,
            )
        )

    model_candidate_count = len(products)
    category_scores = get_category_scores(user_id)
    recent_category_candidates: list[dict[str, Any]] = []
    if category_scores:
        try:
            recent_category_candidates = get_recent_category_candidates(
                category_scores,
                max_categories=RECENT_CATEGORY_MAX_CATEGORIES,
                limit_per_category=RECENT_CATEGORY_CANDIDATES_PER_CATEGORY,
            )[:RECENT_CATEGORY_TOTAL_CANDIDATE_LIMIT]
        except Exception as exc:
            logger.warning(
                "Recent-category candidate expansion failed: user_id=%s error=%s",
                user_id,
                exc,
            )

    unique_products_by_id: dict[str, dict[str, Any]] = {}
    for product in [*products, *recent_category_candidates]:
        product_id = product.get("id") or product.get("product_id")
        if product_id is None:
            continue
        product_key = str(product_id)
        existing = unique_products_by_id.get(product_key)
        if existing is None:
            unique_products_by_id[product_key] = dict(product)
            continue
        for field_name in (
            "category",
            "category_main",
            "category_sub",
            "category_detail",
            "category_name",
            "recent_match_category",
        ):
            if not existing.get(field_name) and product.get(field_name):
                existing[field_name] = product[field_name]

    unique_products = list(unique_products_by_id.values())
    products = apply_category_reranking(unique_products, category_scores)
    returned_products = select_home_recommendation_mix(
        products,
        return_limit=HOME_RECOMMENDATION_RETURN_LIMIT,
        max_recent_category_items=HOME_RECOMMENDATION_MAX_RECENT_CATEGORY_ITEMS,
    )
    recent_category_items_returned = sum(
        product.get("candidate_source") == "recent_category"
        for product in returned_products
    )
    model_items_returned = len(returned_products) - recent_category_items_returned
    top_returned_categories = [
        product.get("category")
        or product.get("category_main")
        or product.get("recent_match_category")
        for product in returned_products
    ]
    top_returned_ids = [
        product.get("id") or product.get("product_id")
        for product in returned_products
    ]
    logger.info(
        "Home recommendation rerank user_id=%s is_ml_enabled=%s strategy=%s "
        "model_candidate_count=%s redis_category_scores=%s "
        "recent_category_candidate_count=%s merged_candidate_count=%s "
        "returned_count=%s recent_category_items_returned=%s "
        "model_items_returned=%s top_returned_categories=%s top_returned_ids=%s",
        user_id,
        is_ml_enabled,
        strategy,
        model_candidate_count,
        category_scores,
        len(recent_category_candidates),
        len(unique_products),
        len(returned_products),
        recent_category_items_returned,
        model_items_returned,
        top_returned_categories,
        top_returned_ids,
    )
    return returned_products


@app.get("/api/recommend/product/{product_id}")
def get_product_recommendations(
    product_id: str,
    limit: int = Query(default=4, ge=1, le=20),
) -> list[dict[str, Any]]:
    return get_content_based_recommendations(product_id, limit=limit)


@app.post("/api/recommend/cart")
def get_cart_recommendations(
    payload: CartRecommendRequest,
    limit: int = Query(default=4, ge=1, le=20),
) -> list[dict[str, Any]]:
    return get_item_based_recommendations(payload.product_ids, limit=limit)


@app.get("/api/categories", response_model=list[CategoryResponse])
def get_categories() -> list[dict[str, str]]:
    return get_categories_from_db()


@app.get("/api/categories-and-brands")
def get_categories_and_brands() -> list[dict[str, Any]]:
    return get_categories_and_brands_from_db()


@app.get("/api/category-tree")
def get_category_tree() -> list[dict[str, Any]]:
    # Backward-compatible alias for hierarchical category data.
    return get_categories_and_brands_from_db()


@app.get("/api/products", response_model=list[ProductResponse])
def get_products(
    category: str = Query(default="all"),
    query: str | None = Query(default=None),
    category_main: str | None = Query(default=None),
    category_sub: str | None = Query(default=None),
    category_detail: str | None = Query(default=None),
    brand: list[str] | None = Query(default=None),
) -> list[dict[str, Any]]:
    return get_products_from_db(
        selected_category=category,
        selected_category_main=category_main,
        selected_category_sub=category_sub,
        selected_category_detail=category_detail,
        selected_brands=brand,
        search_query=query,
    )


@app.get("/api/products/{selected_category}", response_model=list[ProductResponse])
def get_products_by_category(selected_category: str) -> list[dict[str, Any]]:
    categories = get_categories_from_db()
    category_by_slug = {category["id"]: category["name"] for category in categories}
    category_name = category_by_slug.get(selected_category, selected_category)
    return get_products_from_db(category_name)
 
@app.get("/api/recommendations/{user_id}", response_model=list[RecommendationResponse])
def get_recommendations(user_id: str) -> list[dict[str, Any]]:
    return get_recommendations_from_db(user_id)
