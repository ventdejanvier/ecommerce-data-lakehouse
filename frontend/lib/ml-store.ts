"use client";

import { create } from 'zustand';
import { logEvent } from './tracking';

interface MLState {
  isMLEnabled: boolean;
  toggleML: () => void;
  setMLEnabled: (enabled: boolean) => void;
}

export const useMLStore = create<MLState>((set, get) => ({
  isMLEnabled: true, // Default to enabled
  
  toggleML: () => {
    const newState = !get().isMLEnabled;
    
    // Fire tracking event
    logEvent('ML_TOGGLE', {
      enabled: newState,
      previousState: get().isMLEnabled,
      timestamp: new Date().toISOString(),
      context: 'recommendation_engine',
    });
    
    set({ isMLEnabled: newState });
  },
  
  setMLEnabled: (enabled: boolean) => {
    logEvent('ML_TOGGLE', {
      enabled,
      previousState: get().isMLEnabled,
      timestamp: new Date().toISOString(),
      context: 'recommendation_engine',
    });
    
    set({ isMLEnabled: enabled });
  },
}));
