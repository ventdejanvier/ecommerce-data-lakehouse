'use client';

import { motion } from 'framer-motion';
import Link from 'next/link';
import { useEffect, useState } from 'react';
import { ArrowLeft, BarChart3 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Navbar } from '@/components/navbar';
import { AuthModal } from '@/components/auth-modal';
import { CartSheet } from '@/components/cart-sheet';
import { Toaster } from '@/components/ui/toaster';
import { TelemetryWidget } from '@/components/telemetry-widget';

const BATCH_DASHBOARD_URL =
  process.env.NEXT_PUBLIC_METABASE_BATCH_DASHBOARD_URL ||
  'http://localhost:3001/public/dashboard/d302d4cd-0406-4cad-a520-2ed114aaedb8#theme=transparent&bordered=false&titled=false';

const STREAMING_DASHBOARD_URL =
  process.env.NEXT_PUBLIC_METABASE_STREAMING_DASHBOARD_URL ||
  'http://localhost:3001/public/dashboard/a4c35d99-d882-459f-80fc-573f4ca42fa9#refresh=3';

// Quote URLs containing "#" in .env so the refresh fragment is preserved, for example:
// NEXT_PUBLIC_METABASE_STREAMING_DASHBOARD_URL="http://localhost:3001/public/dashboard/a4c35d99-d882-459f-80fc-573f4ca42fa9#refresh=3"
const DEFAULT_STREAMING_IFRAME_REFRESH_MS = 10_000;
const MIN_STREAMING_IFRAME_REFRESH_MS = 3_000;
const configuredStreamingRefreshMs = Number(
  process.env.NEXT_PUBLIC_METABASE_STREAMING_IFRAME_REFRESH_MS
);
const STREAMING_IFRAME_REFRESH_MS =
  Number.isFinite(configuredStreamingRefreshMs) &&
  configuredStreamingRefreshMs >= MIN_STREAMING_IFRAME_REFRESH_MS
    ? Math.floor(configuredStreamingRefreshMs)
    : DEFAULT_STREAMING_IFRAME_REFRESH_MS;

function appendIframeRefreshNonce(url: string, nonce: number): string {
  if (nonce <= 0) return url;

  const hashIndex = url.indexOf('#');
  const baseUrl = hashIndex >= 0 ? url.slice(0, hashIndex) : url;
  const hash = hashIndex >= 0 ? url.slice(hashIndex) : '';
  const separator = baseUrl.includes('?') ? '&' : '?';

  return `${baseUrl}${separator}iframe_refresh=${nonce}${hash}`;
}

export default function AnalyticsPage() {
  const [selectedDashboard, setSelectedDashboard] = useState<'batch' | 'streaming'>('batch');
  const [streamingRefreshNonce, setStreamingRefreshNonce] = useState(0);
  const isBatchDashboard = selectedDashboard === 'batch';
  const streamingIframeUrl = appendIframeRefreshNonce(
    STREAMING_DASHBOARD_URL,
    streamingRefreshNonce
  );

  useEffect(() => {
    if (selectedDashboard !== 'streaming') return;

    const intervalId = window.setInterval(() => {
      setStreamingRefreshNonce((currentNonce) => currentNonce + 1);
    }, STREAMING_IFRAME_REFRESH_MS);

    return () => window.clearInterval(intervalId);
  }, [selectedDashboard]);

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Navbar />
      <AuthModal />
      <CartSheet />

      <main className="container mx-auto px-4 py-8 max-w-7xl">
        {/* Back navigation */}
        <motion.div
          initial={{ opacity: 0, x: -10 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.3 }}
          className="mb-6"
        >
          <Link href="/">
            <Button variant="ghost" className="hover:bg-muted text-muted-foreground hover:text-foreground">
              <ArrowLeft className="mr-2 h-4 w-4" />
              Back to Store
            </Button>
          </Link>
        </motion.div>

        {/* Header section */}
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4 }}
          className="mb-8 space-y-3"
        >
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary/10 border border-primary/20 text-primary">
              <BarChart3 className="h-5 w-5" />
            </div>
            <div>
              <h1 className="text-3xl font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-indigo-500">
                Lakehouse Behavioral Analytics
              </h1>
              <div className="flex items-center gap-2 mt-1">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
                </span>
                <p className="text-muted-foreground text-sm font-medium">
                  Live Connection: Trino Query Engine via Gold Layer
                </p>
              </div>
            </div>
          </div>
        </motion.div>

        <div className="space-y-3">
          <div className="flex flex-col gap-2 sm:flex-row" role="tablist" aria-label="Analytics dashboard">
            <Button
              type="button"
              role="tab"
              aria-selected={isBatchDashboard}
              variant={isBatchDashboard ? 'default' : 'outline'}
              onClick={() => setSelectedDashboard('batch')}
            >
              Historical analytics
            </Button>
            <Button
              type="button"
              role="tab"
              aria-selected={!isBatchDashboard}
              variant={!isBatchDashboard ? 'default' : 'outline'}
              onClick={() => setSelectedDashboard('streaming')}
            >
              Realtime tracking
            </Button>
          </div>
          <p className="text-sm text-muted-foreground">
            {isBatchDashboard
              ? 'Historical analytics dashboard uses batch-processed historical data from the lakehouse pipeline.'
              : 'Realtime tracking dashboard shows near-real-time events generated from website interactions.'}
          </p>
          {!isBatchDashboard && (
            <p className="text-xs text-muted-foreground">
              Embedded view refreshes automatically during demo.
            </p>
          )}
        </div>

        {/* Dashboard View */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="w-full min-h-[1000px] mt-6 bg-card/50 backdrop-blur-xl border border-border/50 rounded-xl shadow-2xl overflow-hidden relative ring-1 ring-white/5"
        >
          <iframe
            src={isBatchDashboard ? BATCH_DASHBOARD_URL : streamingIframeUrl}
            className="w-full h-full min-h-[1000px] border-0"
            title={isBatchDashboard ? 'Historical analytics dashboard' : 'Realtime tracking dashboard'}
          />
        </motion.div>
      </main>

      <Toaster />
      <TelemetryWidget />
    </div>
  );
}
