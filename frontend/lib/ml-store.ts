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
}

interface MLState {
  isAiEnabled: boolean;
  mlStrategy: string;
  isShowtimeActive: boolean;
  aiRecommendations: AIRecommendation[];
  isLoading: boolean;
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
let latestRequestedKey: string | null = null;
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
  const { mlStrategy, setRecommendations, setLoading } = useMLStore.getState();
  const activeUserId = getActiveRecommendationUserId(userId);
  const cacheKey = getRecommendationCacheKey(activeUserId, mlStrategy, isAiEnabled);
  const cachedEntry = recommendationCache.get(cacheKey);
  latestRequestedKey = cacheKey;

  debugRecommendations('Request resolved', {
    activeRecommendationUserId: activeUserId,
    isMLEnabled: isAiEnabled,
    strategy: mlStrategy,
    reason: options.reason ?? 'direct',
  });

  if (!options.forceRefresh && cachedEntry && cachedEntry.expiresAt > Date.now()) {
    if (currentFetchKey !== cacheKey) {
      currentFetchController?.abort();
    }
    setRecommendations(cachedEntry.data);
    setLoading(false);
    debugRecommendations('Using cached response', { cacheKey });
    return cachedEntry.data;
  }

  const existingRequest = inFlightRecommendationRequests.get(cacheKey);
  if (!options.forceRefresh && existingRequest) {
    setLoading(true);
    debugRecommendations('Reusing in-flight response', { cacheKey });
    try {
      const data = await existingRequest;
      if (
        inFlightRecommendationRequests.get(cacheKey) === existingRequest &&
        latestRequestedKey === cacheKey
      ) {
        setRecommendations(data);
        setLoading(false);
      }
      return data;
    } catch (error) {
      if (!(error instanceof Error && error.name === 'AbortError')) {
        console.error('Recommendation request failed:', error);
      }
      if (
        inFlightRecommendationRequests.get(cacheKey) === existingRequest &&
        latestRequestedKey === cacheKey
      ) {
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

  const endpoint = `${BACKEND_API_BASE_URL}/api/recommend/home/${encodeURIComponent(activeUserId)}?is_ml_enabled=${String(Boolean(isAiEnabled))}&strategy=${encodeURIComponent(mlStrategy)}`;
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
    recommendationCache.set(cacheKey, {
      data,
      expiresAt: Date.now() + RECOMMENDATION_CACHE_TTL_MS,
    });
    if (currentFetchController === controller && latestRequestedKey === cacheKey) {
      setRecommendations(data);
    }
    return data;
  } catch (error) {
    if (!(error instanceof Error && error.name === 'AbortError')) {
      console.error('Recommendation request failed:', error);
    }
    return useMLStore.getState().aiRecommendations;
  } finally {
    if (inFlightRecommendationRequests.get(cacheKey) === request) {
      inFlightRecommendationRequests.delete(cacheKey);
    }
    if (currentFetchController === controller) {
      currentFetchController = null;
      currentFetchKey = null;
      if (latestRequestedKey === cacheKey) {
        setLoading(false);
      }
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

  const { isAiEnabled, mlStrategy } = useMLStore.getState();
  const activeUserId = getActiveRecommendationUserId();
  debugRecommendations('Debounced refresh scheduled', {
    reason,
    delayMs: RECOMMENDATION_REFRESH_DEBOUNCE_MS,
    activeRecommendationUserId: activeUserId,
    isMLEnabled: isAiEnabled,
    strategy: mlStrategy,
  });

  recommendationRefreshTimer = setTimeout(() => {
    recommendationRefreshTimer = null;
    const currentState = useMLStore.getState();
    const currentUserId = getActiveRecommendationUserId();
    void fetchRecommendations(currentUserId, currentState.isAiEnabled, {
      forceRefresh: true,
      reason,
    });
  }, RECOMMENDATION_REFRESH_DEBOUNCE_MS);
};
