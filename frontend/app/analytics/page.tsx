'use client';

import { motion } from 'framer-motion';
import Link from 'next/link';
import { ArrowLeft, BarChart3, PieChart, TrendingUp } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Navbar } from '@/components/navbar';
import { AuthModal } from '@/components/auth-modal';
import { CartSheet } from '@/components/cart-sheet';
import { Toaster } from '@/components/ui/toaster';
import { TelemetryWidget } from '@/components/telemetry-widget';

export default function AnalyticsPage() {
  return (
    <div className="min-h-screen bg-background">
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
          className="mb-10 space-y-2"
        >
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary/10 border border-primary/20 text-primary">
              <BarChart3 className="h-5 w-5" />
            </div>
            <div>
              <h1 className="text-3xl font-semibold tracking-tight text-foreground">
                Data Lakehouse Analytics
              </h1>
              <p className="text-muted-foreground text-sm">
                Real-time Gold Layer Insights
              </p>
            </div>
          </div>
        </motion.div>

        {/* Dashboard Grid */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="grid grid-cols-1 md:grid-cols-2 gap-8"
        >
          {/* Zone 1: Cluster Distribution (Full Width) */}
          <Card className="md:col-span-2 border-border shadow-sm bg-card/50 backdrop-blur-sm overflow-hidden animate-in">
            <CardHeader className="flex flex-row items-center gap-4 space-y-0 pb-4">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary/10 text-primary border border-primary/20">
                <PieChart className="h-4 w-4" />
              </div>
              <div className="space-y-0.5">
                <CardTitle className="text-lg">Cluster Distribution</CardTitle>
                <CardDescription>Visualizing user clusters and segments across product categories</CardDescription>
              </div>
            </CardHeader>
            <CardContent>
              <div className="relative w-full h-[400px] bg-muted/20 border border-border/50 rounded-lg overflow-hidden">
                <iframe
                  src="about:blank"
                  className="w-full h-full border-0 rounded-md"
                  title="Cluster Distribution Chart"
                />
              </div>
            </CardContent>
          </Card>

          {/* Zone 2: Top Trending Products (Half Width) */}
          <Card className="border-border shadow-sm bg-card/50 backdrop-blur-sm overflow-hidden animate-in">
            <CardHeader className="flex flex-row items-center gap-4 space-y-0 pb-4">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary/10 text-primary border border-primary/20">
                <TrendingUp className="h-4 w-4" />
              </div>
              <div className="space-y-0.5">
                <CardTitle className="text-lg">Top Trending Products</CardTitle>
                <CardDescription>Most popular items based on real-time stream interactions</CardDescription>
              </div>
            </CardHeader>
            <CardContent>
              <div className="relative w-full h-[400px] bg-muted/20 border border-border/50 rounded-lg overflow-hidden">
                <iframe
                  src="about:blank"
                  className="w-full h-full border-0 rounded-md"
                  title="Top Trending Products Chart"
                />
              </div>
            </CardContent>
          </Card>

          {/* Zone 3: Recommendation Confidence Scores (Half Width) */}
          <Card className="border-border shadow-sm bg-card/50 backdrop-blur-sm overflow-hidden animate-in">
            <CardHeader className="flex flex-row items-center gap-4 space-y-0 pb-4">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary/10 text-primary border border-primary/20">
                <BarChart3 className="h-4 w-4" />
              </div>
              <div className="space-y-0.5">
                <CardTitle className="text-lg">Recommendation Confidence Scores</CardTitle>
                <CardDescription>Accuracy and confidence telemetry for recommendation strategies</CardDescription>
              </div>
            </CardHeader>
            <CardContent>
              <div className="relative w-full h-[400px] bg-muted/20 border border-border/50 rounded-lg overflow-hidden">
                <iframe
                  src="about:blank"
                  className="w-full h-full border-0 rounded-md"
                  title="Recommendation Confidence Scores Chart"
                />
              </div>
            </CardContent>
          </Card>
        </motion.div>
      </main>

      <Toaster />
      <TelemetryWidget />
    </div>
  );
}
