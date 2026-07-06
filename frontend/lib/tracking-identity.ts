export const normalizeTrackingUserId = (value: unknown): string | null => {
  if (typeof value !== "string" && typeof value !== "number") {
    return null;
  }

  const normalizedValue = String(value).trim();
  return normalizedValue.length > 0 ? normalizedValue : null;
};

export const DEFAULT_RECOMMENDATION_USER_ID = "1515915625355805313";

interface ActiveRecommendationIdentityCandidates {
  explicitUserId: unknown;
  selectedPersonaUserId: unknown;
  authenticatedUserId: unknown;
}

export const resolveActiveRecommendationUserId = ({
  explicitUserId,
  selectedPersonaUserId,
  authenticatedUserId,
}: ActiveRecommendationIdentityCandidates): string => {
  const resolvedUserId =
    normalizeTrackingUserId(explicitUserId) ??
    normalizeTrackingUserId(selectedPersonaUserId) ??
    normalizeTrackingUserId(authenticatedUserId);

  return resolvedUserId ?? DEFAULT_RECOMMENDATION_USER_ID;
};
