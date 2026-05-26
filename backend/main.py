from typing import Any

from fastapi import BackgroundTasks, FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from database import get_recommendations_from_db
from kafka_producer import produce_event
from schemas import RecommendationResponse, TelemetryEvent

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/track", status_code=status.HTTP_202_ACCEPTED)
async def track_event(
    event: TelemetryEvent, background_tasks: BackgroundTasks
) -> dict[str, str]:
    payload = event.model_dump(mode="json")
    background_tasks.add_task(produce_event, "ecommerce-raw-events", payload)

    return {
        "status": "accepted",
        "eventId": event.eventId,
    }


@app.get(
    "/api/recommend/home/{user_id}",
    response_model=list[RecommendationResponse],
)
def get_home_recommendations(user_id: str) -> list[dict[str, Any]]:
    return get_recommendations_from_db(user_id)


@app.get(
    "/api/recommendations/{user_id}",
    response_model=list[RecommendationResponse],
)
def get_recommendations(user_id: str) -> list[dict[str, Any]]:
    return get_recommendations_from_db(user_id)
