'use client';

import { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Terminal, Minimize2, Maximize2, X, Activity } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { subscribeTelemetry, EventType, EventPayload } from '@/lib/tracking';

interface TelemetryLog {
  id: string;
  timestamp: string;
  eventType: EventType;
  details: string;
}

export function TelemetryWidget() {
  const [isOpen, setIsOpen] = useState(true);
  const [isMinimized, setIsMinimized] = useState(false);
  const [logs, setLogs] = useState<TelemetryLog[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const unsubscribe = subscribeTelemetry((event) => {
      const newLog: TelemetryLog = {
        id: `log_${Date.now()}_${Math.random().toString(36).substring(2, 5)}`,
        timestamp: new Date().toLocaleTimeString('en-US', { 
          hour12: false, 
          hour: '2-digit', 
          minute: '2-digit', 
          second: '2-digit' 
        }),
        eventType: event.type,
        details: formatEventDetails(event.type, event.payload),
      };

      setLogs((prev) => [...prev.slice(-49), newLog]); // Keep last 50 logs
    });

    return () => unsubscribe();
  }, []);

  // Auto-scroll to bottom when new logs arrive
  useEffect(() => {
    if (scrollRef.current && !isMinimized) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs, isMinimized]);

  const formatEventDetails = (type: EventType, payload: EventPayload): string => {
    switch (type) {
      case 'add_to_cart':
      case 'CART_UPDATE':
        return `ProductID: ${payload.productId || payload.product?.id || 'N/A'}`;
      case 'product_click':
      case 'product_view':
        return `Product: ${payload.productName || payload.product?.name || 'N/A'}`;
      case 'search':
        return `Query: "${payload.query || payload.searchTerm || ''}"`;
      case 'category_filter':
        return `Category: ${payload.category || payload.categoryId || 'N/A'}`;
      case 'LOGIN_SUCCESS':
      case 'REGISTER_SUCCESS':
      case 'SIGNUP_SUCCESS':
        return `UserID: ${payload.userId || 'N/A'}`;
      case 'LOGOUT':
        return `UserID: ${payload.userId || 'N/A'}`;
      case 'page_view':
        return `Page: ${payload.pageName || 'N/A'}`;
      case 'NAVIGATE_HOME':
        return 'Scrolled to top';
      case 'CART_OPEN':
        return `Items: ${payload.itemCount || 0}`;
      case 'ML_TOGGLE':
        return `Enabled: ${payload.enabled ?? 'N/A'}`;
      default:
        return JSON.stringify(payload).slice(0, 50);
    }
  };

  if (!isOpen) {
    return (
      <motion.button
        initial={{ scale: 0, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        exit={{ scale: 0, opacity: 0 }}
        onClick={() => setIsOpen(true)}
        className="fixed bottom-4 left-4 z-50 flex h-12 w-12 items-center justify-center rounded-full bg-foreground/90 text-background shadow-lg backdrop-blur-sm hover:bg-foreground transition-colors"
      >
        <Terminal className="h-5 w-5" />
        {logs.length > 0 && (
          <span className="absolute -top-1 -right-1 flex h-5 w-5 items-center justify-center rounded-full bg-primary text-[10px] font-bold text-primary-foreground">
            {logs.length > 99 ? '99+' : logs.length}
          </span>
        )}
      </motion.button>
    );
  }

  return (
    <motion.div
      initial={{ y: 100, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      exit={{ y: 100, opacity: 0 }}
      transition={{ type: 'spring', damping: 20, stiffness: 300 }}
      className="fixed bottom-4 left-4 z-50 w-[360px] max-w-[calc(100vw-2rem)] overflow-hidden rounded-xl border border-border bg-foreground/95 shadow-2xl backdrop-blur-md"
    >
      {/* Header */}
      <div className="flex items-center justify-between border-b border-background/10 bg-foreground px-3 py-2">
        <div className="flex items-center gap-2">
          <Activity className="h-4 w-4 text-primary" />
          <span className="text-xs font-semibold text-background tracking-wide">
            LIVE TELEMETRY
          </span>
          <span className="flex h-2 w-2 rounded-full bg-primary animate-pulse" />
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 text-background/60 hover:text-background hover:bg-background/10"
            onClick={() => setIsMinimized(!isMinimized)}
          >
            {isMinimized ? (
              <Maximize2 className="h-3 w-3" />
            ) : (
              <Minimize2 className="h-3 w-3" />
            )}
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 text-background/60 hover:text-background hover:bg-background/10"
            onClick={() => setIsOpen(false)}
          >
            <X className="h-3 w-3" />
          </Button>
        </div>
      </div>

      {/* Terminal Content */}
      <AnimatePresence>
        {!isMinimized && (
          <motion.div
            initial={{ height: 0 }}
            animate={{ height: 200 }}
            exit={{ height: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div
              ref={scrollRef}
              className="h-[200px] overflow-y-auto p-3 font-mono text-xs scrollbar-thin scrollbar-thumb-background/20 scrollbar-track-transparent"
            >
              {logs.length === 0 ? (
                <div className="flex h-full items-center justify-center text-background/40">
                  <span>Waiting for events...</span>
                </div>
              ) : (
                <div className="space-y-1">
                  {logs.map((log, index) => (
                    <motion.div
                      key={log.id}
                      initial={{ opacity: 0, x: -10 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: index === logs.length - 1 ? 0 : 0 }}
                      className="flex items-start gap-2"
                    >
                      <span className="text-background/40 shrink-0">{log.timestamp}</span>
                      <span className="text-primary shrink-0">&gt;</span>
                      <span className="text-primary font-semibold shrink-0">
                        [{log.eventType}]
                      </span>
                      <span className="text-background/80 truncate">{log.details}</span>
                    </motion.div>
                  ))}
                </div>
              )}
            </div>

            {/* Footer */}
            <div className="flex items-center justify-between border-t border-background/10 bg-foreground/50 px-3 py-1.5">
              <span className="text-[10px] text-background/40">
                {logs.length} events captured
              </span>
              <Button
                variant="ghost"
                size="sm"
                className="h-5 px-2 text-[10px] text-background/40 hover:text-background hover:bg-background/10"
                onClick={() => setLogs([])}
              >
                Clear
              </Button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
