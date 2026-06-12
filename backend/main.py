import logging
from typing import Any 
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
from redis_client import add_recent_category, get_recent_categories
from schemas import (
    CartRecommendRequest,
    CategoryResponse,
    ProductResponse,
    RecommendationResponse,
)

app = FastAPI()
logger = logging.getLogger(__name__)

# Backward-compatible name for tests/legacy monkeypatching.
get_recommendations_from_db = get_recommendations_with_fallback

SESSION_CATEGORY_EVENT_TYPES = {"product_click", "product_view"}
SESSION_CATEGORY_FIELDS = (
    "category",
    "category_main",
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
        user_id = data.get("userId")
        normalized_user_id = str(user_id).strip() if user_id is not None else ""
        if event_type not in SESSION_CATEGORY_EVENT_TYPES:
            return
        if not normalized_user_id:
            logger.info(
                "Skipping recent category capture: missing userId eventId=%s",
                data.get("eventId"),
            )
            return

        category = extract_session_category(data)
        if not category:
            logger.info(
                "Skipping recent category capture: missing category eventId=%s userId=%s",
                data.get("eventId"),
                normalized_user_id,
            )
            return

        add_recent_category(normalized_user_id, category)
    except Exception as exc:
        logger.warning("Failed to capture recent category: %s", exc)


def normalize_category_for_reranking(value: Any) -> str:
    if value is None:
        return ""

    normalized = str(value).strip().lower()
    normalized = normalized.replace("_", " ").replace("-", " ")
    return " ".join(normalized.split())


def apply_category_reranking(
    items: list[dict[str, Any]],
    recent_categories: set[str],
) -> list[dict[str, Any]]:
    if not recent_categories:
        return items

    normalized_recent_categories = {
        normalized_category
        for category in recent_categories
        if (normalized_category := normalize_category_for_reranking(category))
    }
    if not normalized_recent_categories:
        return items

    promoted_items: list[dict[str, Any]] = []
    remaining_items: list[dict[str, Any]] = []

    for item in items:
        item_categories = {
            normalize_category_for_reranking(item.get("category")),
            normalize_category_for_reranking(item.get("category_main")),
        }
        if item_categories.intersection(normalized_recent_categories):
            promoted_items.append(item)
        else:
            remaining_items.append(item)

    return promoted_items + remaining_items


def rerank_by_recent_categories(
    products: list[dict[str, Any]],
    recent_categories: set[str],
) -> list[dict[str, Any]]:
    return apply_category_reranking(products, recent_categories)


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

    recent_categories = get_recent_categories(user_id)
    products = apply_category_reranking(products, recent_categories)
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
 
def get_recommendations(user_id: str) -> list[dict[str, Any]]:
    return get_recommendations_from_db(user_id)
