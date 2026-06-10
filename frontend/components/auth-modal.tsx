'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useAuthStore } from '@/lib/auth-store';
import { logEvent } from '@/lib/tracking';
import { User } from 'lucide-react';

export function AuthModal() {
  const { isLoginModalOpen, closeLoginModal } = useAuthStore();
  const [inputUserId, setInputUserId] = useState('');

  const resetForm = () => {
    setInputUserId('');
  };

  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault();

    const trimmedUserId = inputUserId.trim();
    if (!trimmedUserId) {
      return;
    }

    const mockUser = {
      id: trimmedUserId,
      name: `Demo User (${trimmedUserId.slice(-4)})`,
      email: 'demo@lakehouse.local',
    };

    useAuthStore.setState({
      user: mockUser,
      isLoginModalOpen: false,
    });

    logEvent('LOGIN_SUCCESS', {
      userId: mockUser.id,
      email: mockUser.email,
      name: mockUser.name,
      method: 'demo_user_id',
      timestamp: new Date().toISOString(),
    });

    resetForm();
  };

  const handleOpenChange = (open: boolean) => {
    if (!open) {
      closeLoginModal();
      resetForm();
    }
  };

  return (
    <Dialog open={isLoginModalOpen} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-md p-0 overflow-hidden">
        <DialogHeader className="p-6 pb-2">
          <DialogTitle className="text-xl font-semibold text-foreground text-center">
            Demo User Impersonation
          </DialogTitle>
        </DialogHeader>

        <div className="p-6 pt-4">
          <motion.form
            onSubmit={handleLogin}
            className="space-y-4"
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.15 }}
          >
            <div className="space-y-2">
              <Label htmlFor="demo-user-id" className="text-sm font-medium text-foreground">
                User ID (from Training Data)
              </Label>
              <div className="relative">
                <User className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="demo-user-id"
                  type="text"
                  inputMode="numeric"
                  autoComplete="off"
                  placeholder="e.g., 1515915625540660259"
                  value={inputUserId}
                  onChange={(e) => setInputUserId(e.target.value)}
                  className="pl-10 bg-muted border-border focus:border-foreground/20"
                  required
                />
              </div>
            </div>

            <motion.div whileHover={{ scale: 1.01 }} whileTap={{ scale: 0.99 }}>
              <Button
                type="submit"
                className="w-full bg-primary text-primary-foreground hover:bg-primary/90"
                disabled={!inputUserId.trim()}
              >
                Start Demo Session
              </Button>
            </motion.div>

            <p className="text-xs leading-relaxed text-muted-foreground">
              💡 Hint: Paste a User ID from the Postgres `serving_als` or `serving_predictions` table to inspect personalized recommendation variations.
            </p>
          </motion.form>
        </div>
      </DialogContent>
    </Dialog>
  );
}
