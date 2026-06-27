export const normalizeTrackingUserId = (value: unknown): string | null => {
  if (typeof value !== "string" && typeof value !== "number") {
    return null;
  }

  const normalizedValue = String(value).trim();
  return normalizedValue.length > 0 ? normalizedValue : null;
};

interface TrackingIdentityCandidates {
  explicitUserId: unknown;
  selectedPersonaUserId: unknown;
  authenticatedUserId: unknown;
  sessionId: unknown;
}

export const resolveTrackingUserId = ({
  explicitUserId,
  selectedPersonaUserId,
  authenticatedUserId,
  sessionId,
}: TrackingIdentityCandidates): string => {
  const resolvedUserId =
    normalizeTrackingUserId(explicitUserId) ??
    normalizeTrackingUserId(selectedPersonaUserId) ??
    normalizeTrackingUserId(authenticatedUserId);

  if (resolvedUserId) {
    return resolvedUserId;
  }

  const resolvedSessionId = normalizeTrackingUserId(sessionId) ?? "server_session";
  return `guest:${resolvedSessionId}`;
};
