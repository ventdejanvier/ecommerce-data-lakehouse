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
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useAuthStore } from '@/lib/auth-store';
import { logEvent } from '@/lib/tracking';

const DEMO_PERSONAS = [
  { id: '1515915625355805313', name: 'Champions (Cluster 1)', desc: 'VIP, high spend' },
  { id: '1515915625353561691', name: 'Loyal Customers (Cluster 2)', desc: 'Regular buyers' },
  { id: '1515915625353226922', name: 'At Risk (Cluster 3)', desc: 'Churn risk' },
  { id: '1515915625353236157', name: 'Recent Browsers (Cluster 0)', desc: 'Window shoppers' },
  { id: 'FRESH_USER', name: 'Fresh User (Cold Start)', desc: 'New dynamic ID for real-time demo' },
];

const resolveDemoPersona = (
  selectedPersonaId: string,
  timestamp: number,
) => {
  const selectedPersona =
    DEMO_PERSONAS.find((persona) => persona.id === selectedPersonaId) ?? DEMO_PERSONAS[0];

  if (selectedPersona.id === 'FRESH_USER') {
    return {
      finalUserId: `NEW_${timestamp}`,
      finalUserName: 'Demo Cold Start',
    };
  }

  return {
    finalUserId: selectedPersona.id,
    finalUserName: selectedPersona.name,
  };
};

export function AuthModal() {
  const { isLoginModalOpen, closeLoginModal } = useAuthStore();
  const [selectedPersonaId, setSelectedPersonaId] = useState<string>(DEMO_PERSONAS[0].id);

  const resetForm = () => {
    setSelectedPersonaId(DEMO_PERSONAS[0].id);
  };

  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault();

    const { finalUserId, finalUserName } = resolveDemoPersona(
      selectedPersonaId,
      Date.now(),
    );

    const mockUser = {
      id: finalUserId,
      name: finalUserName,
      email: 'demo@lakehouse.local',
    };

    useAuthStore.setState({
      user: mockUser,
      isLoginModalOpen: false,
    });

    logEvent('LOGIN_SUCCESS', {
      userId: finalUserId,
      email: mockUser.email,
      name: finalUserName,
      method: 'demo_persona',
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
              <Label htmlFor="demo-persona" className="text-sm font-medium text-foreground">
                Demo Persona
              </Label>
              <Select value={selectedPersonaId} onValueChange={setSelectedPersonaId}>
                <SelectTrigger id="demo-persona" className="w-full bg-muted border-border">
                  <SelectValue placeholder="Choose a demo persona" />
                </SelectTrigger>
                <SelectContent>
                  {DEMO_PERSONAS.map((persona) => (
                    <SelectItem key={persona.id} value={persona.id}>
                      <div className="flex flex-col gap-0.5">
                        <span>{persona.name}</span>
                        <span className="text-xs text-muted-foreground">{persona.desc}</span>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <motion.div whileHover={{ scale: 1.01 }} whileTap={{ scale: 0.99 }}>
              <Button
                type="submit"
                className="w-full bg-primary text-primary-foreground hover:bg-primary/90"
                disabled={!selectedPersonaId}
              >
                Start Demo Session
              </Button>
            </motion.div>

            <p className="text-xs leading-relaxed text-muted-foreground">
              Hint: Choose Fresh User to generate a disposable cold-start ID without reusing cached persona behavior.
            </p>
          </motion.form>
        </div>
      </DialogContent>
    </Dialog>
  );
}
