'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Navbar } from '@/components/navbar';
import { ProductGrid } from '@/components/product-grid';
import { ProductDetail } from '@/components/product-detail';
import { AuthModal } from '@/components/auth-modal';
import { CartSheet } from '@/components/cart-sheet';

import { TelemetryWidget } from '@/components/telemetry-widget';
import { Product } from '@/components/product-card';
import { logPageView } from '@/lib/tracking';
import { Toaster } from '@/components/ui/toaster';
import { useToast } from '@/hooks/use-toast';
import { Activity, Zap } from 'lucide-react';

export default function Home() {
  const [selectedProduct, setSelectedProduct] = useState<Product | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [activeCategory, setActiveCategory] = useState('all');
  const { toast } = useToast();

  // Log page view on mount
  useEffect(() => {
    logPageView('dashboard', {
      initialCategory: activeCategory,
      hasSearchQuery: false,
    });
  }, []);

  const handleSearch = (query: string) => {
    setSearchQuery(query);
    toast({
      title: 'Search Executed',
      description: `Searching for: "${query || 'all products'}". Check console for telemetry.`,
    });
  };

  const handleCategoryChange = (categoryId: string) => {
    setActiveCategory(categoryId);
    toast({
      title: 'Category Changed',
      description: `Filtering by: ${categoryId}. Check console for telemetry.`,
    });
  };

  const handleProductClick = (product: Product) => {
    setSelectedProduct(product);
  };

  const handleAddToCart = (product: Product) => {
    toast({
      title: 'Added to Cart',
      description: `${product.name} added. Check console for telemetry.`,
    });
  };

  const handleBackToProducts = () => {
    setSelectedProduct(null);
    logPageView('dashboard', {
      returnedFromProduct: true,
    });
  };

  return (
    <div className="min-h-screen bg-background">
      <Navbar />
      <AuthModal />
      <CartSheet />
      
      <main className="container mx-auto px-4 py-8">
        <AnimatePresence mode="wait">
          {selectedProduct ? (
            <motion.div
              key="product-detail"
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: -20 }}
              transition={{ duration: 0.3 }}
            >
              <ProductDetail
                product={selectedProduct}
                onBack={handleBackToProducts}
                onAddToCart={handleAddToCart}
              />
            </motion.div>
          ) : (
            <motion.div
              key="dashboard"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.3 }}
              className="space-y-8"
            >
              {/* Welcome Banner */}
              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.1 }}
                className="rounded-2xl bg-gradient-to-r from-primary/10 via-primary/5 to-transparent border border-primary/20 p-6"
              >
                <div className="flex items-center justify-between flex-wrap gap-4">
                  <div>
                    <h1 className="text-2xl font-semibold text-foreground tracking-tight">
                      Welcome to the Store
                    </h1>
                    <p className="text-muted-foreground text-sm mt-1">
                      Data Lakehouse demo with real-time ML recommendations
                    </p>
                  </div>
                  <div className="flex items-center gap-4">
                    <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-background border border-border">
                      <Activity className="h-4 w-4 text-primary" />
                      <span className="text-sm font-medium text-foreground">Live Session</span>
                    </div>
                    <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-background border border-border">
                      <Zap className="h-4 w-4 text-primary" />
                      <span className="text-sm font-medium text-foreground">Real-time Events</span>
                    </div>
                  </div>
                </div>
              </motion.div>

              {/* Product Grid */}
              <ProductGrid
                onProductClick={handleProductClick}
                onAddToCart={handleAddToCart}
                searchQuery={searchQuery}
                activeCategory={activeCategory}
                onSearch={handleSearch}
                onCategoryChange={handleCategoryChange}
              />
            </motion.div>
          )}
        </AnimatePresence>
      </main>

      <Toaster />
      <TelemetryWidget />
    </div>
  );
}
