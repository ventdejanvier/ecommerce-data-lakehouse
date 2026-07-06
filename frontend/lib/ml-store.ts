"use client";

import { create } from 'zustand';
import { useAuthStore } from './auth-store';
import { logEvent } from './tracking';
import { resolveActiveRecommendationUserId } from './tracking-identity';

const BACKEND_API_BASE_URL = (
  process.env.NEXT_PUBLIC_BACKEND_API_URL ?? 'http://localhost:8000'
).replace(/\/$/, '');

const RECOMMENDATION_CACHE_TTL_MS = 20_000;
const RECOMMENDATION_REFRESH_DEBOUNCE_MS = 300;

export interface AIRecommendation {
  id: string;
  name: string;
  price: number;
  category: string;
  cluster_total_score: number;
  reranked_score?: number;
  candidate_source?: string;
  recent_match_category?: string;
}

interface MLState {
  isAiEnabled: boolean;
  mlStrategy: string;
  isShowtimeActive: boolean;
  aiRecommendations: AIRecommendation[];
  isLoading: boolean;
  latestRecommendationRequestId: number;
  toggleAi: () => void;
  setMlStrategy: (strategy: string) => void;
  setShowtimeActive: (val: boolean) => void;
  setRecommendations: (data: AIRecommendation[]) => void;
  setLoading: (status: boolean) => void;
  // Backward-compatible aliases for existing rendering components.
  isMLEnabled: boolean;
  toggleML: () => void;
  setMLEnabled: (enabled: boolean) => void;
}

export const useMLStore = create<MLState>((set, get) => ({
  isAiEnabled: false,
  mlStrategy: 'als',
  isShowtimeActive: false,
  aiRecommendations: [],
  isLoading: false,
  latestRecommendationRequestId: 0,
  isMLEnabled: false,

  toggleAi: () => {
    const newState = !get().isAiEnabled;

    logEvent('ML_TOGGLE', {
      enabled: newState,
      previousState: get().isAiEnabled,
      timestamp: new Date().toISOString(),
      context: 'recommendation_engine',
    });

    set({
      isAiEnabled: newState,
      isMLEnabled: newState,
    });
  },

  setMlStrategy: (strategy: string) => {
    const previousStrategy = get().mlStrategy;

    if (strategy !== previousStrategy) {
      set({ mlStrategy: strategy });

      logEvent('ML_STRATEGY_CHANGE', {
        strategy,
        previousStrategy,
        context: 'strategy_dropdown',
        timestamp: new Date().toISOString(),
      });

      if (get().isAiEnabled) {
        void fetchRecommendations(null, true, { reason: 'strategy_change' });
      }
    }
  },

  setShowtimeActive: (val: boolean) => {
    set({ isShowtimeActive: val });
  },

  setRecommendations: (data: AIRecommendation[]) => {
    set({ aiRecommendations: data });
  },

  setLoading: (status: boolean) => {
    set({ isLoading: status });
  },

  toggleML: () => {
    get().toggleAi();
  },

  setMLEnabled: (enabled: boolean) => {
    logEvent('ML_TOGGLE', {
      enabled,
      previousState: get().isAiEnabled,
      timestamp: new Date().toISOString(),
      context: 'recommendation_engine',
    });

    set({
      isAiEnabled: enabled,
      isMLEnabled: enabled,
    });
  },
}));

interface FetchRecommendationOptions {
  forceRefresh?: boolean;
  reason?: string;
}

interface RecommendationCacheEntry {
  data: AIRecommendation[];
  expiresAt: number;
}

let currentFetchController: AbortController | null = null;
let currentFetchKey: string | null = null;
let recommendationRequestSeq = 0;
let recommendationRefreshTimer: ReturnType<typeof setTimeout> | null = null;
const recommendationCache = new Map<string, RecommendationCacheEntry>();
const inFlightRecommendationRequests = new Map<string, Promise<AIRecommendation[]>>();

const debugRecommendations = (
  message: string,
  details: Record<string, unknown>,
) => {
  if (process.env.NODE_ENV === 'development') {
    console.debug(`[Recommendations] ${message}`, details);
  }
};

const getRecommendationResponseDetails = (data: AIRecommendation[]) => ({
  topIds: data.slice(0, 10).map((item) => item.id),
  recentCategoryCount: data.filter(
    (item) => item.candidate_source === 'recent_category',
  ).length,
});

const isLatestRecommendationRequest = (requestId: number) =>
  useMLStore.getState().latestRecommendationRequestId === requestId;

const applyRecommendationResponse = (
  requestId: number,
  data: AIRecommendation[],
  setRecommendations: (recommendations: AIRecommendation[]) => void,
): boolean => {
  const latestRequestId = useMLStore.getState().latestRecommendationRequestId;
  if (latestRequestId !== requestId) {
    debugRecommendations('Response ignored as stale', {
      requestId,
      latestRequestId,
      ...getRecommendationResponseDetails(data),
    });
    return false;
  }

  setRecommendations(data);
  debugRecommendations('Response applied', {
    requestId,
    ...getRecommendationResponseDetails(data),
  });
  return true;
};

export const getActiveRecommendationUserId = (
  explicitUserId: string | null = null,
): string => {
  const authenticatedUserId = useAuthStore.getState().user?.id ?? null;
  return resolveActiveRecommendationUserId({
    explicitUserId,
    // Demo persona selection is represented by the authenticated user store.
    selectedPersonaUserId: null,
    authenticatedUserId,
  });
};

const getRecommendationCacheKey = (
  userId: string,
  strategy: string,
  isAiEnabled: boolean,
) => `${userId}::${strategy}::${String(isAiEnabled)}`;

export const fetchRecommendations = async (
  userId: string | null,
  isAiEnabled: boolean,
  options: FetchRecommendationOptions = {},
): Promise<AIRecommendation[]> => {
  const requestId = ++recommendationRequestSeq;
  const forceRefresh = Boolean(options.forceRefresh);
  const reason = options.reason ?? 'direct';
  const { mlStrategy, setRecommendations, setLoading } = useMLStore.getState();
  const activeUserId = getActiveRecommendationUserId(userId);
  const cacheKey = getRecommendationCacheKey(activeUserId, mlStrategy, isAiEnabled);
  useMLStore.setState({ latestRecommendationRequestId: requestId });

  if (forceRefresh) {
    recommendationCache.delete(cacheKey);
  }

  const cachedEntry = recommendationCache.get(cacheKey);
  const query = new URLSearchParams({
    is_ml_enabled: String(Boolean(isAiEnabled)),
    strategy: mlStrategy,
  });
  if (forceRefresh) {
    query.set('refresh_nonce', `${Date.now()}-${requestId}`);
  }
  const endpoint = `${BACKEND_API_BASE_URL}/api/recommend/home/${encodeURIComponent(activeUserId)}?${query.toString()}`;

  debugRecommendations('Request start', {
    requestId,
    reason,
    forceRefresh,
    userId: activeUserId,
    isAiEnabled,
    strategy: mlStrategy,
    url: endpoint,
  });

  if (!forceRefresh && cachedEntry && cachedEntry.expiresAt > Date.now()) {
    if (currentFetchKey !== cacheKey) {
      currentFetchController?.abort();
    }
    applyRecommendationResponse(requestId, cachedEntry.data, setRecommendations);
    if (isLatestRecommendationRequest(requestId)) {
      setLoading(false);
    }
    debugRecommendations('Using cached response', { requestId, cacheKey });
    return cachedEntry.data;
  }

  const existingRequest = inFlightRecommendationRequests.get(cacheKey);
  if (!forceRefresh && existingRequest) {
    setLoading(true);
    debugRecommendations('Reusing in-flight response', { requestId, cacheKey });
    try {
      const data = await existingRequest;
      if (applyRecommendationResponse(requestId, data, setRecommendations)) {
        recommendationCache.set(cacheKey, {
          data,
          expiresAt: Date.now() + RECOMMENDATION_CACHE_TTL_MS,
        });
      }
      if (isLatestRecommendationRequest(requestId)) {
        setLoading(false);
      }
      return data;
    } catch (error) {
      if (!(error instanceof Error && error.name === 'AbortError')) {
        console.error('Recommendation request failed:', error);
      }
      if (isLatestRecommendationRequest(requestId)) {
        setLoading(false);
      }
      return useMLStore.getState().aiRecommendations;
    }
  }

  currentFetchController?.abort();
  const controller = new AbortController();
  currentFetchController = controller;
  currentFetchKey = cacheKey;
  setLoading(true);

  const requestStartedAt = Date.now();
  const request = (async () => {
    const response = await fetch(endpoint, {
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(`Recommendation API failed with status ${response.status}`);
    }
    return (await response.json()) as AIRecommendation[];
  })();
  inFlightRecommendationRequests.set(cacheKey, request);

  try {
    const data = await request;
    if (applyRecommendationResponse(requestId, data, setRecommendations)) {
      recommendationCache.set(cacheKey, {
        data,
        expiresAt: Date.now() + RECOMMENDATION_CACHE_TTL_MS,
      });
    }
    return data;
  } catch (error) {
    if (!(error instanceof Error && error.name === 'AbortError')) {
      console.error('Recommendation request failed:', error);
    }
    return useMLStore.getState().aiRecommendations;
  } finally {
    debugRecommendations('Fetch finished', {
      requestId,
      cacheKey,
      durationMs: Date.now() - requestStartedAt,
      superseded: !isLatestRecommendationRequest(requestId),
    });
    if (inFlightRecommendationRequests.get(cacheKey) === request) {
      inFlightRecommendationRequests.delete(cacheKey);
    }
    if (currentFetchController === controller) {
      currentFetchController = null;
      currentFetchKey = null;
    }
    if (isLatestRecommendationRequest(requestId)) {
      setLoading(false);
    }
  }
};

export const scheduleRecommendationRefresh = (reason = 'eligible_interaction') => {
  if (typeof window === 'undefined') {
    return;
  }

  if (recommendationRefreshTimer) {
    clearTimeout(recommendationRefreshTimer);
  }

  debugRecommendations('Debounced refresh scheduled', {
    reason,
    delayMs: RECOMMENDATION_REFRESH_DEBOUNCE_MS,
  });

  recommendationRefreshTimer = setTimeout(() => {
    recommendationRefreshTimer = null;
    const currentState = useMLStore.getState();
    const currentUserId = getActiveRecommendationUserId();
    debugRecommendations('Debounced refresh executing', {
      reason,
      userId: currentUserId,
      isAiEnabled: currentState.isAiEnabled,
      strategy: currentState.mlStrategy,
    });
    void fetchRecommendations(currentUserId, currentState.isAiEnabled, {
      forceRefresh: true,
      reason,
    });
  }, RECOMMENDATION_REFRESH_DEBOUNCE_MS);
};
