from typing import Literal

from pydantic import BaseModel, ConfigDict

EventType = Literal[
    "page_view",
    "search",
    "category_filter",
    "product_click",
    "product_view",
    "add_to_cart",
    "login_click",
    "guest_click",
    "navbar_click",
    "back_click",
    "LOGIN_SUCCESS",
    "LOGOUT",
    "CART_UPDATE",
    "CART_OPEN",
    "CHECKOUT_INITIATE",
    "PURCHASE_COMPLETED",
    "ML_TOGGLE",
    "SIGNUP_SUCCESS",
    "REGISTER_SUCCESS",
    "NAVIGATE_HOME",
]


class EventContext(BaseModel):
    userAgent: str
    url: str
    referrer: str


class TelemetryEvent(BaseModel):
    eventId: str
    timestamp: str
    sessionId: str
    eventType: EventType
    context: EventContext
    userId: str | None = None

    # Accept arbitrary event-specific fields (action, pageName, productId, etc.)
    model_config = ConfigDict(extra="allow")
