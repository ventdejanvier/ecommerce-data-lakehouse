// Telemetry utility for E-commerce Data Lakehouse
// All events are logged with timestamps and sent via fire-and-forget HTTP POST

import { useAuthStore } from "./auth-store";
import { useMLStore } from "./ml-store";
import {
  normalizeTrackingUserId,
  resolveTrackingUserId,
} from "./tracking-identity";

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
  | "ML_STRATEGY_CHANGE"
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

const getActiveTrackingUserId = (
  payloadUserId: unknown,
  sessionId: string,
): string => {
  let selectedPersonaUserId: string | null = null;
  try {
    const mlState = useMLStore.getState() as {
      isAiEnabled?: boolean;
      isMLEnabled?: boolean;
      userId?: unknown;
      mlUserId?: unknown;
      activeUserId?: unknown;
      selectedUserId?: unknown;
      selectedPersonaId?: unknown;
      persona?: { id?: unknown; userId?: unknown } | null;
      user?: { id?: unknown; userId?: unknown } | null;
    };

    if (mlState.isAiEnabled || mlState.isMLEnabled) {
      selectedPersonaUserId =
        normalizeTrackingUserId(mlState.userId) ??
        normalizeTrackingUserId(mlState.mlUserId) ??
        normalizeTrackingUserId(mlState.activeUserId) ??
        normalizeTrackingUserId(mlState.selectedUserId) ??
        normalizeTrackingUserId(mlState.persona?.userId) ??
        normalizeTrackingUserId(mlState.persona?.id) ??
        normalizeTrackingUserId(mlState.user?.userId) ??
        normalizeTrackingUserId(mlState.user?.id);
    }
  } catch (error) {
    console.warn("[DataLakehouse] Unable to resolve ML tracking user:", error);
  }

  let authenticatedUserId: string | null = null;
  try {
    authenticatedUserId = normalizeTrackingUserId(useAuthStore.getState().user?.id);
  } catch (error) {
    console.warn("[DataLakehouse] Unable to resolve auth tracking user:", error);
  }

  return resolveTrackingUserId({
    explicitUserId: payloadUserId,
    selectedPersonaUserId,
    authenticatedUserId,
    sessionId,
  });
};

const getEventCategory = (payload: EventPayload): string | null => {
  const category =
    payload.category ??
    payload.category_main ??
    payload.productCategory ??
    payload.product_category ??
    payload.categoryName;

  return typeof category === "string" && category.trim()
    ? category.trim()
    : null;
};

/**
 * Log an event to the Data Lakehouse
 * Uses fire-and-forget async HTTP POST - does NOT block the UI
 */
export function logEvent(type: EventType, payload: EventPayload = {}): void {
  const sessionId = getSessionId();
  const userId = getActiveTrackingUserId(payload.userId, sessionId);
  const eventCategory = getEventCategory(payload);
  const categoryFields =
    type === "product_click" || type === "product_view"
      ? {
          ...(eventCategory ? { category: eventCategory, category_main: eventCategory } : {}),
        }
      : {};

  const enrichedPayload = {
    eventId: generateEventId(),
    timestamp: new Date().toISOString(),
    sessionId,
    eventType: type,
    ...payload,
    ...categoryFields,
    userId,
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
  fetch("/api/track", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(enrichedPayload),
  })
    .then(async (response) => {
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        console.error("[DataLakehouse] Tracking API failed:", {
          status: response.status,
          body,
        });
      }
    })
    .catch((error) => {
      console.error("Tracking API is unreachable:", error);
    });
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
