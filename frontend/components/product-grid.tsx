'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ProductCard, Product } from './product-card';
import { SearchBar } from './search-bar';
import { CategoryFilters } from './category-filters';
import { Sparkles, Grid3X3, Loader2 } from 'lucide-react';
import { useMLStore } from '@/lib/ml-store';
import { Switch } from '@/components/ui/switch';
import { logEvent } from '@/lib/tracking';

// Standard catalog products (8 items) - controlled by search/category filters
const catalogProducts: Product[] = [
  {
    id: 'cat_001',
    name: 'Xiaomi 15T Pro',
    price: 799.99,
    originalPrice: 899.99,
    rating: 4.8,
    reviewCount: 2341,
    category: 'Smartphones',
    inStock: true,
  },
  {
    id: 'cat_002',
    name: 'Redmi Buds 6 Pro',
    price: 79.99,
    originalPrice: 99.99,
    rating: 4.5,
    reviewCount: 1892,
    category: 'Audio',
    inStock: true,
  },
  {
    id: 'cat_003',
    name: 'Smart Band 10',
    price: 49.99,
    rating: 4.6,
    reviewCount: 3567,
    category: 'Wearables',
    inStock: true,
  },
  {
    id: 'cat_004',
    name: 'Xiaomi G24i Gaming Monitor',
    price: 179.99,
    originalPrice: 229.99,
    rating: 4.4,
    reviewCount: 876,
    category: 'Monitors',
    inStock: true,
  },
  {
    id: 'cat_005',
    name: 'RedmiBook Pro 15',
    price: 999.99,
    originalPrice: 1199.99,
    rating: 4.7,
    reviewCount: 1234,
    category: 'Laptops',
    inStock: true,
  },
  {
    id: 'cat_006',
    name: 'Mechanical Gaming Keyboard RGB',
    price: 89.99,
    originalPrice: 119.99,
    rating: 4.6,
    reviewCount: 2156,
    category: 'Gaming',
    inStock: true,
  },
  {
    id: 'cat_007',
    name: 'Xiaomi Watch S4 Sport',
    price: 249.99,
    rating: 4.8,
    reviewCount: 945,
    category: 'Wearables',
    inStock: false,
  },
  {
    id: 'cat_008',
    name: 'Mi Curved Gaming Monitor 34"',
    price: 449.99,
    originalPrice: 549.99,
    rating: 4.7,
    reviewCount: 678,
    category: 'Monitors',
    inStock: true,
  },
];

// High-end recommendation products (4 items) - controlled by ML toggle
const recommendedProducts: Product[] = [
  {
    id: 'rec_001',
    name: 'Xiaomi 15 Ultra',
    price: 1299.99,
    originalPrice: 1499.99,
    rating: 4.9,
    reviewCount: 892,
    category: 'Smartphones',
    inStock: true,
  },
  {
    id: 'rec_002',
    name: 'RedmiBook Pro 16 4K',
    price: 1599.99,
    originalPrice: 1899.99,
    rating: 4.8,
    reviewCount: 456,
    category: 'Laptops',
    inStock: true,
  },
  {
    id: 'rec_003',
    name: 'Xiaomi Buds 5 Pro ANC',
    price: 199.99,
    originalPrice: 249.99,
    rating: 4.7,
    reviewCount: 1234,
    category: 'Audio',
    inStock: true,
  },
  {
    id: 'rec_004',
    name: 'Xiaomi Watch S5 Pro',
    price: 399.99,
    originalPrice: 449.99,
    rating: 4.9,
    reviewCount: 567,
    category: 'Wearables',
    inStock: true,
  },
];

// Alternative ML recommendations (shuffled when toggle is switched)
const altRecommendedProducts: Product[] = [
  {
    id: 'alt_001',
    name: 'Mi Ultra Gaming Monitor 49"',
    price: 1199.99,
    originalPrice: 1399.99,
    rating: 4.8,
    reviewCount: 234,
    category: 'Monitors',
    inStock: true,
  },
  {
    id: 'alt_002',
    name: 'Xiaomi Gaming Chair Pro',
    price: 449.99,
    originalPrice: 549.99,
    rating: 4.6,
    reviewCount: 789,
    category: 'Gaming',
    inStock: true,
  },
  {
    id: 'alt_003',
    name: 'RedmiBook Air 14 OLED',
    price: 1099.99,
    originalPrice: 1299.99,
    rating: 4.7,
    reviewCount: 345,
    category: 'Laptops',
    inStock: true,
  },
  {
    id: 'alt_004',
    name: 'Xiaomi SoundBar Theater',
    price: 299.99,
    originalPrice: 379.99,
    rating: 4.8,
    reviewCount: 678,
    category: 'Audio',
    inStock: true,
  },
];

interface ProductGridProps {
  onProductClick?: (product: Product) => void;
  onAddToCart?: (product: Product) => void;
  searchQuery: string;
  activeCategory: string;
  onSearch: (query: string) => void;
  onCategoryChange: (categoryId: string) => void;
}

export function ProductGrid({ 
  onProductClick, 
  onAddToCart,
  searchQuery,
  activeCategory,
  onSearch,
  onCategoryChange,
}: ProductGridProps) {
  const { isMLEnabled, toggleML } = useMLStore();
  const [isLoadingRecs, setIsLoadingRecs] = useState(false);
  const [currentRecs, setCurrentRecs] = useState(recommendedProducts);
  const [recVersion, setRecVersion] = useState(0);
  
  // Filter catalog products based on search and category
  const filteredCatalog = catalogProducts.filter((product) => {
    const matchesSearch = searchQuery === '' || 
      product.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      product.category.toLowerCase().includes(searchQuery.toLowerCase());
    
    const matchesCategory = activeCategory === 'all' || 
      product.category.toLowerCase() === activeCategory.toLowerCase();
    
    return matchesSearch && matchesCategory;
  });

  // Handle ML toggle with loading animation
  const handleMLToggle = () => {
    setIsLoadingRecs(true);
    toggleML();
    
    // Simulate API call for new recommendations
    setTimeout(() => {
      // Alternate between recommendation sets
      setRecVersion((prev) => prev + 1);
      setCurrentRecs((prev) => 
        prev === recommendedProducts ? altRecommendedProducts : recommendedProducts
      );
      setIsLoadingRecs(false);
    }, 1000);
  };

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.08,
        delayChildren: 0.1,
      },
    },
  };

  const itemVariants = {
    hidden: { opacity: 0, y: 20 },
    visible: { opacity: 1, y: 0 },
  };

  return (
    <div className="w-full space-y-12">
      {/* Section A: Recommended for You (ML FIRST - Core Feature) */}
      <div className="space-y-6">
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex items-center justify-between flex-wrap gap-4"
        >
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-primary/10">
              <Sparkles className={`h-5 w-5 transition-colors duration-300 ${
                isMLEnabled ? 'text-primary' : 'text-muted-foreground'
              }`} />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Recommended for You</h2>
              <p className="text-sm text-muted-foreground">AI-curated picks based on your preferences</p>
            </div>
          </div>
          
          {/* ML Toggle - positioned next to section title */}
          <div className="flex items-center gap-3 px-4 py-2 rounded-full bg-primary/5 border border-primary/20">
            <div className="flex items-center gap-2">
              <Sparkles 
                className={`h-4 w-4 transition-colors duration-300 ${
                  isMLEnabled ? 'text-primary' : 'text-muted-foreground'
                }`} 
              />
              <span className="text-sm font-medium text-foreground">
                ALS Model
              </span>
            </div>
            
            <Switch
              checked={isMLEnabled}
              onCheckedChange={handleMLToggle}
              className="data-[state=checked]:bg-primary"
            />
            
            {isMLEnabled && !isLoadingRecs && (
              <motion.span
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                className="text-xs text-primary font-medium"
              >
                Active
              </motion.span>
            )}
          </div>
        </motion.div>

        {/* Recommendations Grid with Loading State */}
        <div className="relative">
          <AnimatePresence mode="wait">
            {isLoadingRecs ? (
              <motion.div
                key="loading"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="flex items-center justify-center py-20"
              >
                <div className="flex flex-col items-center gap-3">
                  <Loader2 className="h-8 w-8 animate-spin text-primary" />
                  <p className="text-sm text-muted-foreground">Updating recommendations...</p>
                </div>
              </motion.div>
            ) : (
              <motion.div
                key={`recs-${recVersion}`}
                variants={containerVariants}
                initial="hidden"
                animate="visible"
                className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5"
              >
                {currentRecs.map((product) => (
                  <motion.div key={product.id} variants={itemVariants}>
                    <ProductCard
                      product={product}
                      onProductClick={onProductClick}
                      onAddToCart={onAddToCart}
                    />
                  </motion.div>
                ))}
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>

      {/* Divider */}
      <div className="relative">
        <div className="absolute inset-0 flex items-center">
          <div className="w-full border-t border-border" />
        </div>
        <div className="relative flex justify-center">
          <span className="bg-background px-4 text-sm text-muted-foreground">
            Browse All Products
          </span>
        </div>
      </div>

      {/* Section B: Product Catalog */}
      <div className="space-y-6">
        {/* Sticky Filter Bar - contextually placed above catalog */}
        <div className="sticky top-16 z-40 -mx-4 px-4 py-4 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 border-b border-border">
          <div className="space-y-4">
            <SearchBar onSearch={onSearch} />
            <CategoryFilters onCategoryChange={onCategoryChange} />
          </div>
        </div>

        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          className="flex items-center justify-between"
        >
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-muted">
              <Grid3X3 className="h-5 w-5 text-foreground" />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Product Catalog</h2>
              <p className="text-sm text-muted-foreground">Browse our full electronics collection</p>
            </div>
          </div>
          <span className="px-3 py-1.5 rounded-lg bg-muted border border-border text-sm text-muted-foreground">
            {filteredCatalog.length} products
          </span>
        </motion.div>

        <motion.div
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5"
        >
          {filteredCatalog.map((product) => (
            <motion.div key={product.id} variants={itemVariants}>
              <ProductCard
                product={product}
                onProductClick={onProductClick}
                onAddToCart={onAddToCart}
              />
            </motion.div>
          ))}
        </motion.div>

        {filteredCatalog.length === 0 && (
          <div className="text-center py-12">
            <p className="text-muted-foreground">No products found matching your criteria</p>
          </div>
        )}
      </div>
    </div>
  );
}
