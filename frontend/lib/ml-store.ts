"use client";

import { create } from 'zustand';
import { useAuthStore } from './auth-store';
import { logEvent } from './tracking';

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
  aiRecommendations: AIRecommendation[];
  isLoading: boolean;
  toggleAi: () => void;
  setMlStrategy: (strategy: string) => void;
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

      // Explicitly log the strategy change to fix N/A telemetry issues
      logEvent('ML_STRATEGY_CHANGE', {
        strategy: strategy,
        previousStrategy: previousStrategy,
        context: 'strategy_dropdown',
        timestamp: new Date().toISOString()
      });

      // Auto-fetch new recommendations when strategy changes
      const isAiEnabled = get().isAiEnabled;
      if (isAiEnabled) {
        void fetchRecommendations(null, true);
      }
    }
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

let currentFetchController: AbortController | null = null;

export const fetchRecommendations = async (
  userId: string | null,
  isAiEnabled: boolean
) => {
  const { mlStrategy, setRecommendations, setLoading } = useMLStore.getState();
  setLoading(true);
  setRecommendations([]);

  currentFetchController?.abort();
  const controller = new AbortController();
  currentFetchController = controller;

  try {
    const activeUserId = userId || useAuthStore.getState().user?.id || '1515915625355805313';
    const endpoint = `http://localhost:8000/api/recommend/home/${encodeURIComponent(activeUserId)}?is_ml_enabled=${String(Boolean(isAiEnabled))}&strategy=${encodeURIComponent(mlStrategy)}`;

    const response = await fetch(endpoint, {
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!response.ok) {
      console.warn("Lỗi API, hiển thị mảng rỗng");
      if (currentFetchController === controller) {
        setRecommendations([]);
      }
      return;
    }
    const data: AIRecommendation[] = await response.json();
    if (currentFetchController === controller) {
      setRecommendations(data);
    }
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') return;
    console.error('Lỗi:', error);
    if (currentFetchController === controller) {
      setRecommendations([]);
    }
  } finally {
    if (currentFetchController === controller) {
      setLoading(false);
    }
  }
};
