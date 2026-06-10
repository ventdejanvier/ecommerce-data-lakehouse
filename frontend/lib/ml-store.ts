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
    if (strategy === get().mlStrategy) {
      return;
    }

    set({ mlStrategy: strategy });

    logEvent('ML_TOGGLE', {
      action: 'strategy_changed',
      strategy,
      timestamp: new Date().toISOString(),
      context: 'recommendation_engine',
    });

    if (get().isAiEnabled) {
      void fetchRecommendations(null, true);
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

export const fetchRecommendations = async (
  _userId: string | null,
  isAiEnabled: boolean
) => {
  const { mlStrategy, setRecommendations, setLoading } = useMLStore.getState();
  setLoading(true);
  try {
    const currentUserId = useAuthStore.getState().user?.id;
    const hasLoggedInUser = typeof currentUserId === 'string' && currentUserId.length > 0;
    const mlEnabledParam = String(Boolean(isAiEnabled));
    const endpoint = isAiEnabled && hasLoggedInUser
      ? `http://localhost:8000/api/recommend/home/${encodeURIComponent(currentUserId)}?is_ml_enabled=${mlEnabledParam}&strategy=${encodeURIComponent(mlStrategy)}`
      : `http://localhost:8000/api/recommend/global`;

    const response = await fetch(endpoint);
    if (!response.ok) {
      console.warn("Lỗi API, hiển thị mảng rỗng");
      setRecommendations([]); 
      return;
    }
    const data: AIRecommendation[] = await response.json();
    setRecommendations(data);
  } catch (error) {
    console.error('Lỗi:', error);
    setRecommendations([]); 
  } finally {
    setLoading(false);
  }
};
