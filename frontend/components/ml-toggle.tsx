"use client";

import { motion } from 'framer-motion';
import { Sparkles } from 'lucide-react';
import { Switch } from '@/components/ui/switch';
import { useToast } from '@/hooks/use-toast';
import { useMLStore } from '@/lib/ml-store';

export function MLToggle() {
  const { toast } = useToast();
  const { isAiEnabled, toggleAi } = useMLStore();

  const handleToggle = (checked: boolean) => {
    if (checked === isAiEnabled) {
      return;
    }

    toggleAi();
    toast({
      title: checked
        ? '✨ AI Mode Activated! Searching behavior cluster...'
        : 'Switched back to general trending products.',
    });
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: -10 }}
      animate={{ opacity: 1, y: 0 }}
      className="flex items-center gap-3 px-4 py-2 rounded-full bg-muted/50 border border-border"
    >
      <div className="flex items-center gap-2">
        <Sparkles 
          className={`h-4 w-4 transition-colors duration-300 ${
            isAiEnabled ? 'text-amber-500' : 'text-muted-foreground'
          }`} 
        />
        <span className="text-sm font-medium text-foreground">
          ML Recommendations
        </span>
      </div>
      
      <Switch
        checked={isAiEnabled}
        onCheckedChange={handleToggle}
        className="data-[state=checked]:bg-amber-500"
      />
      
      {isAiEnabled && (
        <motion.span
          initial={{ opacity: 0, scale: 0.8 }}
          animate={{ opacity: 1, scale: 1 }}
          className="text-xs text-amber-600 font-medium"
        >
          Active
        </motion.span>
      )}
    </motion.div>
  );
}
