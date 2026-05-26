const frontendBaseUrl = (process.env.FRONTEND_BASE_URL ?? 'http://localhost:3000').replace(/\/$/, '');
const eventId = `evt_frontend_smoke_${Date.now()}`;

const payload = {
  eventId,
  timestamp: new Date().toISOString(),
  sessionId: `session_frontend_smoke_${Date.now()}`,
  eventType: 'page_view',
  userId: 'USER_FRONTEND_SMOKE',
  pageName: 'frontend_track_route_smoke',
  context: {
    userAgent: 'frontend-track-route-smoke',
    url: frontendBaseUrl,
    referrer: '',
  },
};

const response = await fetch(`${frontendBaseUrl}/api/track`, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify(payload),
});

const body = await response.json().catch(() => ({}));

if (!response.ok) {
  console.error(`[FAIL] POST /api/track -> ${response.status}`, body);
  process.exit(1);
}

if (!body.success || !body.forwarded || body.eventId !== eventId) {
  console.error('[FAIL] Unexpected tracking route response:', body);
  process.exit(1);
}

console.log('[OK] POST /api/track forwarded to backend:', body);
