from typing import Any 
from sqlalchemy import text
from database import engine
from fastapi import BackgroundTasks, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from database import (
    get_categories_and_brands_from_db,
    get_categories_from_db,
    get_products_from_db,
    get_recommendations_with_fallback,
)
from kafka_producer import produce_event
from schemas import (
    CategoryResponse,
    ProductResponse,
    RecommendationResponse,
)

app = FastAPI()

# Backward-compatible name for tests/legacy monkeypatching.
get_recommendations_from_db = get_recommendations_with_fallback

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
            query = """
            SELECT 
                g.product_id::text AS id,
                p.display_name AS name,
                COALESCE(p.price, 25.50) AS price,
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
            query = """
            SELECT 
                product_id::text AS id,
                display_name AS name,
                COALESCE(price, 25.50) AS price,
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


@app.post("/api/track")
def track_event(data: dict[str, Any], background_tasks: BackgroundTasks) -> dict[str, str]:
    # Đẩy tác vụ gửi vào Kafka ra background để API phản hồi ngay.
    background_tasks.add_task(send_to_kafka_background, data)
    return {"status": "ok"}

@app.get("/api/recommend/home/{user_id}")
def get_home_recommendations(user_id: str, is_ml_enabled: bool = False):
    if not is_ml_enabled:
        # Gọi tên hàm lấy dữ liệu global
        return get_global_recommendations()
    else:
        # Gọi tên hàm lấy dữ liệu cá nhân hóa
        return get_recommendations_from_db(user_id)


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
    )


@app.get("/api/products/{selected_category}", response_model=list[ProductResponse])
def get_products_by_category(selected_category: str) -> list[dict[str, Any]]:
    categories = get_categories_from_db()
    category_by_slug = {category["id"]: category["name"] for category in categories}
    category_name = category_by_slug.get(selected_category, selected_category)
    return get_products_from_db(category_name)
 
def get_recommendations(user_id: str) -> list[dict[str, Any]]:
    return get_recommendations_from_db(user_id)
