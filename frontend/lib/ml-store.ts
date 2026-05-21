"use client";

import { create } from 'zustand';
import { logEvent } from './tracking';

export interface AIRecommendation {
  cluster_id: number;
  product_id: number;
  display_name: string;
  cluster_total_score: number;
}

interface MLState {
  isAiEnabled: boolean;
  aiRecommendations: AIRecommendation[];
  isLoading: boolean;
  toggleAi: () => void;
  setRecommendations: (data: AIRecommendation[]) => void;
  setLoading: (status: boolean) => void;
  // Backward-compatible aliases for existing rendering components.
  isMLEnabled: boolean;
  toggleML: () => void;
  setMLEnabled: (enabled: boolean) => void;
}

export const useMLStore = create<MLState>((set, get) => ({
  isAiEnabled: false,
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
