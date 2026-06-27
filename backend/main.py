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
    get_recommendations_by_strategy,
    get_recommendations_with_fallback,
)
from kafka_producer import produce_event
from recommendation_scoring import (
    SCORING_CONFIG,
    normalize_category,
    purchase_completed_category_updates,
    rerank_candidates,
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
        if category is None:
            continue

        normalized_category = str(category).strip()
        if normalized_category:
            return normalized_category

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
            try:
                increment_category_score(normalized_user_id, category, score)
            except Exception as exc:
                logger.warning(
                    "Failed to increment recent category: userId=%s category=%s error=%s",
                    normalized_user_id,
                    category,
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
    return rerank_candidates(items, category_scores, SCORING_CONFIG)


def rerank_by_recent_categories(
    products: list[dict[str, Any]],
    category_scores: dict[str, float],
) -> list[dict[str, Any]]:
    return apply_category_reranking(products, category_scores)


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
        products = list(get_recommendations_by_strategy(user_id, strategy))

    category_scores = get_category_scores(user_id)

    unique_products_by_id: dict[str, dict[str, Any]] = {}
    for product in products:
        product_id = product.get("id") or product.get("product_id")
        if product_id is None:
            continue
        unique_products_by_id.setdefault(str(product_id), product)

    unique_products = list(unique_products_by_id.values())
    products = apply_category_reranking(unique_products, category_scores)
    return products


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
