'use client';

import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface User {
  id: string;
  email: string;
  name: string;
  avatar?: string;
}

interface AuthState {
  user: User | null;
  isLoginModalOpen: boolean;
  hasHydrated: boolean;
  login: (email: string, password: string) => User;
  loginWithGoogle: () => User;
  logout: () => void;
  openLoginModal: () => void;
  closeLoginModal: () => void;
  setHasHydrated: (status: boolean) => void;
}

// Mock user generator
const generateMockUser = (email: string): User => ({
  id: `USER_${Math.random().toString(36).substring(2, 8).toUpperCase()}`,
  email,
  name: email.split('@')[0],
  avatar: undefined,
});

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      user: null,
      isLoginModalOpen: false,
      hasHydrated: false,

      login: (email: string) => {
        const user = generateMockUser(email);
        set({ user, isLoginModalOpen: false });
        return user;
      },

      loginWithGoogle: () => {
        const user = generateMockUser('user@gmail.com');
        set({ user, isLoginModalOpen: false });
        return user;
      },

      logout: () => {
        set({ user: null });
      },

      openLoginModal: () => set({ isLoginModalOpen: true }),
      closeLoginModal: () => set({ isLoginModalOpen: false }),
      setHasHydrated: (status: boolean) => set({ hasHydrated: status }),
    }),
    {
      name: 'ecommerce-auth-storage',
      partialize: (state) => ({ user: state.user }),
      onRehydrateStorage: () => (state) => {
        state?.setHasHydrated(true);
      },
    }
  )
);

export const hasAuthStoreHydrated = () => useAuthStore.persist.hasHydrated();
