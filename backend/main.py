from fastapi import BackgroundTasks, FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from kafka_producer import produce_event
from schemas import TelemetryEvent

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)


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
