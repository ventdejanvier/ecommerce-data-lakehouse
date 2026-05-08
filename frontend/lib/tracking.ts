// Telemetry utility for E-commerce Data Lakehouse
// All events are logged with timestamps and sent via fire-and-forget HTTP POST

export type EventType =
  | "page_view"
  | "search"
  | "category_filter"
  | "product_click"
  | "product_view"
  | "add_to_cart"
  | "login_click"
  | "guest_click"
  | "navbar_click"
  | "back_click"
  | "LOGIN_SUCCESS"
  | "LOGOUT"
  | "CART_UPDATE"
  | "CART_OPEN"
  | "CHECKOUT_INITIATE"
  | "PURCHASE_COMPLETED"
  | "ML_TOGGLE"
  | "SIGNUP_SUCCESS"
  | "REGISTER_SUCCESS"
  | "NAVIGATE_HOME";

// Global event listeners for telemetry widget
type TelemetryListener = (event: {
  type: EventType;
  payload: EventPayload;
}) => void;
const telemetryListeners: TelemetryListener[] = [];

export function subscribeTelemetry(listener: TelemetryListener): () => void {
  telemetryListeners.push(listener);
  return () => {
    const index = telemetryListeners.indexOf(listener);
    if (index > -1) telemetryListeners.splice(index, 1);
  };
}

export interface EventPayload {
  eventId?: string;
  timestamp?: string;
  sessionId?: string;
  userId?: string;
  [key: string]: unknown;
}

// Generate unique event ID
const generateEventId = (): string => {
  return `evt_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
};

// Get or create session ID
const getSessionId = (): string => {
  if (typeof window === "undefined") return "server_session";

  let sessionId = sessionStorage.getItem("lakehouse_session_id");
  if (!sessionId) {
    sessionId = `session_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
    sessionStorage.setItem("lakehouse_session_id", sessionId);
  }
  return sessionId;
};

/**
 * Log an event to the Data Lakehouse
 * Uses fire-and-forget async HTTP POST - does NOT block the UI
 */
export function logEvent(type: EventType, payload: EventPayload = {}): void {
  const enrichedPayload = {
    eventId: generateEventId(),
    timestamp: new Date().toISOString(),
    sessionId: getSessionId(),
    eventType: type,
    ...payload,
    // Add browser context
    context: {
      userAgent:
        typeof window !== "undefined" ? window.navigator.userAgent : "server",
      url: typeof window !== "undefined" ? window.location.href : "",
      referrer: typeof window !== "undefined" ? document.referrer : "",
    },
  };

  // Log to console for local debugging
  console.log(`[DataLakehouse] Event: ${type}`, enrichedPayload);

  // Notify all telemetry listeners
  telemetryListeners.forEach((listener) => {
    listener({ type, payload: enrichedPayload });
  });

  // Fire-and-forget async HTTP POST - does NOT await to avoid blocking UI
  fetch("http://localhost:8000/api/track", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(enrichedPayload),
  }).catch(console.error); // Silently handle network failures
}

/**
 * Track page views automatically
 */
export function logPageView(
  pageName: string,
  metadata?: Record<string, unknown>,
): void {
  logEvent("page_view", {
    pageName,
    ...metadata,
  });
}
