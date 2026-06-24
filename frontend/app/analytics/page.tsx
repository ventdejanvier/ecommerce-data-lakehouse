'use client';

import { motion } from 'framer-motion';
import Link from 'next/link';
import { ArrowLeft, BarChart3 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Navbar } from '@/components/navbar';
import { AuthModal } from '@/components/auth-modal';
import { CartSheet } from '@/components/cart-sheet';
import { Toaster } from '@/components/ui/toaster';
import { TelemetryWidget } from '@/components/telemetry-widget';

export default function AnalyticsPage() {
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

        {/* Dashboard View */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="w-full min-h-[1000px] mt-6 bg-card/50 backdrop-blur-xl border border-border/50 rounded-xl shadow-2xl overflow-hidden relative ring-1 ring-white/5"
        >
          <iframe
            src="http://localhost:3001/public/dashboard/d302d4cd-0406-4cad-a520-2ed114aaedb8#theme=transparent&bordered=false&titled=false"
            className="w-full h-full min-h-[1000px] border-0"
            title="Metabase Dashboard"
          />
        </motion.div>
      </main>

      <Toaster />
      <TelemetryWidget />
    </div>
  );
}
