'use client';

import { useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ProductCard, Product } from './product-card';
import { SearchBar } from './search-bar';
import {
  ProductFilter,
  type CategoryMainNode,
  type ProductFilterSelection,
} from './product-filter';
import { Sparkles, Grid3X3 } from 'lucide-react';
import { type AIRecommendation, useMLStore } from '@/lib/ml-store';
import { Switch } from '@/components/ui/switch';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

const BACKEND_API_BASE_URL = (
  process.env.NEXT_PUBLIC_BACKEND_API_URL ?? 'http://localhost:8000'
).replace(/\/$/, '');

interface ProductGridProps {
  items?: AIRecommendation[];
  onProductClick?: (product: Product) => void;
  onAddToCart?: (product: Product) => void;
  searchQuery: string;
  onSearch: (query: string) => void;
  onCategoryChange?: (categoryId: string) => void;
  onAiToggle?: (enabled: boolean) => void;
}

function mapRecommendationToProduct(
  recommendation: AIRecommendation
): Product {
  return {
    id: recommendation.id,  
    name: recommendation.name,  
    price: recommendation.price,  
    originalPrice: undefined,
    rating: 4.5,
    reviewCount: Math.max(1, Math.round(recommendation.cluster_total_score)),
    category: recommendation.category,
    inStock: true,
  };
}

export function ProductGrid({ 
  items,
  onProductClick, 
  onAddToCart,
  searchQuery,
  onSearch,
  onCategoryChange,
  onAiToggle,
}: ProductGridProps) {
  const [categoryTree, setCategoryTree] = useState<CategoryMainNode[]>([]);
  const [catalogProducts, setCatalogProducts] = useState<Product[]>([]);
  const [filterSelection, setFilterSelection] = useState<ProductFilterSelection>({
    categoryMain: 'all',
    categorySub: null,
    categoryDetail: null,
  });
  const [isCategoryLoading, setIsCategoryLoading] = useState(false);
  const [isCatalogLoading, setIsCatalogLoading] = useState(false);
  const {
    isAiEnabled,
    aiRecommendations,
    isLoading,
    mlStrategy,
    setMlStrategy,
    toggleML,
    setLoading,
  } = useMLStore();
  const recommendationSource = items ?? aiRecommendations;
 
  const safeSource = Array.isArray(recommendationSource) ? recommendationSource : [];
  const displayedRecommendations = safeSource.map(mapRecommendationToProduct); 
  const hasAiRecommendations = isAiEnabled && displayedRecommendations.length > 0;

  useEffect(() => {
    let isMounted = true;

    const fetchCategories = async () => {
      setIsCategoryLoading(true);
      try {
        const response = await fetch(`${BACKEND_API_BASE_URL}/api/category-tree`);
        if (!response.ok) {
          throw new Error(`Failed to fetch categories: ${response.status}`);
        }
        const data = (await response.json()) as CategoryMainNode[];
        if (isMounted) {
          setCategoryTree(data);
        }
      } catch (error) {
        console.error('Failed to fetch categories:', error);
      } finally {
        if (isMounted) {
          setIsCategoryLoading(false);
        }
      }
    };

    void fetchCategories();
    return () => {
      isMounted = false;
    };
  }, []);

  useEffect(() => {
    let isMounted = true;

    const fetchProductsByCategory = async () => {
      setIsCatalogLoading(true);
      try {
        const params = new URLSearchParams();
        params.set('category', 'all');
        if (filterSelection.categoryMain !== 'all') {
          params.set('category_main', filterSelection.categoryMain);
        }
        if (filterSelection.categorySub) {
          params.set('category_sub', filterSelection.categorySub);
        }
        if (filterSelection.categoryDetail) {
          params.set('category_detail', filterSelection.categoryDetail);
        }
        const trimmedSearchQuery = searchQuery.trim();
        const queryString = trimmedSearchQuery
          ? `${params.toString()}&query=${encodeURIComponent(trimmedSearchQuery)}`
          : params.toString();
        const response = await fetch(
          `${BACKEND_API_BASE_URL}/api/products?${queryString}`
        );
        if (!response.ok) {
          throw new Error(`Failed to fetch products: ${response.status}`);
        }
        const data = (await response.json()) as Product[];
        if (isMounted) {
          setCatalogProducts(data);
        }
      } catch (error) {
        console.error('Failed to fetch products:', error);
        if (isMounted) {
          setCatalogProducts([]);
        }
      } finally {
        if (isMounted) {
          setIsCatalogLoading(false);
        }
      }
    };

    void fetchProductsByCategory();
    return () => {
      isMounted = false;
    };
  }, [filterSelection, searchQuery]);

  const filteredCatalog = catalogProducts;

  const handleAiToggle = (checked: boolean) => {
    if (checked === isAiEnabled) {
      return;
    }

    if (!checked) {
      setLoading(false);
    }

    toggleML();
    onAiToggle?.(checked);
  };

  const handleFilterChange = (selection: ProductFilterSelection) => {
    setFilterSelection(selection);
    const path = [
      selection.categoryMain,
      selection.categorySub,
      selection.categoryDetail,
    ]
      .filter(Boolean)
      .join(' > ');
    onCategoryChange?.(path);
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
                isAiEnabled ? 'text-primary' : 'text-muted-foreground'
              }`} />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-foreground">Recommended for You</h2>
              <p className="text-sm text-muted-foreground">AI-curated picks based on your preferences</p>
            </div>
          </div>
          
          {/* ML Toggle - positioned next to section title */}
          <div className="flex flex-wrap items-center gap-3 rounded-full border border-primary/20 bg-primary/5 px-4 py-2">
            <div className="flex items-center gap-2">
              <Sparkles 
                className={`h-4 w-4 transition-colors duration-300 ${
                  isAiEnabled ? 'text-primary' : 'text-muted-foreground'
                }`} 
              />
              <span className="text-sm font-medium text-foreground">
                AI Model
              </span>
            </div>

            <div className="flex items-center gap-3">
              <Switch
                checked={isAiEnabled}
                onCheckedChange={handleAiToggle}
                className="data-[state=checked]:bg-primary"
              />

              {isAiEnabled && (
                <Select value={mlStrategy} onValueChange={setMlStrategy}>
                  <SelectTrigger size="sm" className="w-[250px] bg-background">
                    <SelectValue placeholder="Select ML strategy" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="als">ALS (Collaborative Filtering)</SelectItem>
                    <SelectItem value="cluster">K-Means (Cluster Profiling)</SelectItem>
                  </SelectContent>
                </Select>
              )}

              {isAiEnabled && !isLoading && (
                <motion.span
                  initial={{ opacity: 0, scale: 0.8 }}
                  animate={{ opacity: 1, scale: 1 }}
                  className="text-xs font-medium text-primary"
                >
                  Active
                </motion.span>
              )}
              {isAiEnabled && isLoading && displayedRecommendations.length > 0 && (
                <span className="text-xs font-medium text-muted-foreground">
                  Refreshing...
                </span>
              )}
            </div>
          </div>
        </motion.div>

        {/* Recommendations Grid with Loading State */}
        <div className="relative">
          <AnimatePresence mode="wait">
            {isLoading && displayedRecommendations.length === 0 ? (
              <motion.div
                key="loading"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5"
              >
                {Array.from({ length: 10 }).map((_, index) => (
                  <div
                    key={`recommendation-skeleton-${index}`}
                    className="overflow-hidden rounded-xl bg-card border border-border"
                  >
                    <Skeleton className="aspect-square w-full rounded-none" />
                    <div className="p-4 space-y-3">
                      <Skeleton className="h-3 w-20" />
                      <Skeleton className="h-10 w-full" />
                      <Skeleton className="h-5 w-28" />
                      <Skeleton className="h-6 w-24" />
                      <Skeleton className="h-9 w-full rounded-lg" />
                    </div>
                  </div>
                ))}
              </motion.div>
            ) : (
              <motion.div
                key={hasAiRecommendations ? 'ai-recommendations' : 'default-recommendations'}
                variants={containerVariants}
                initial="hidden"
                animate="visible"
                className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5"
              >
                {displayedRecommendations.map((product) => (
                  <motion.div key={product.id} variants={itemVariants}>
                    <ProductCard
                      product={product}
                      isAiPicked={hasAiRecommendations}
                      onProductClick={onProductClick}
                      onAddToCart={onAddToCart}
                    />
                  </motion.div>
                ))}
              </motion.div>
            )}
          </AnimatePresence>
          {!isLoading && isAiEnabled && displayedRecommendations.length === 0 && (
            <div className="rounded-xl border border-dashed border-border p-6 text-center text-sm text-muted-foreground">
              No AI recommendations available yet.
            </div>
          )}
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
        <div className="sticky top-16 z-40 -mx-4 border-b border-border bg-background/95 px-4 py-4 backdrop-blur supports-[backdrop-filter]:bg-background/60">
          <SearchBar onSearch={onSearch} />
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

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-[280px_1fr]">
          <div className="lg:sticky lg:top-32 lg:self-start">
            <ProductFilter
              categoryTree={categoryTree}
              selection={filterSelection}
              isLoading={isCategoryLoading}
              onFilterChange={handleFilterChange}
            />
          </div>

          <motion.div
            variants={containerVariants}
            initial="hidden"
            animate="visible"
            className="grid grid-cols-1 gap-5 sm:grid-cols-2 xl:grid-cols-3"
          >
            {isCatalogLoading ? (
              Array.from({ length: 9 }).map((_, index) => (
                <div
                  key={`catalog-skeleton-${index}`}
                  className="overflow-hidden rounded-xl border border-border bg-card"
                >
                  <Skeleton className="aspect-square w-full rounded-none" />
                  <div className="space-y-3 p-4">
                    <Skeleton className="h-3 w-20" />
                    <Skeleton className="h-10 w-full" />
                    <Skeleton className="h-5 w-28" />
                    <Skeleton className="h-6 w-24" />
                    <Skeleton className="h-9 w-full rounded-lg" />
                  </div>
                </div>
              ))
            ) : (
              filteredCatalog.map((product) => (
                <motion.div key={product.id} variants={itemVariants}>
                  <ProductCard
                    product={product}
                    onProductClick={onProductClick}
                    onAddToCart={onAddToCart}
                  />
                </motion.div>
              ))
            )}
          </motion.div>
        </div>

        {!isCatalogLoading && filteredCatalog.length === 0 && (
          <div className="text-center py-12">
            <p className="text-muted-foreground">No products found matching your criteria</p>
          </div>
        )}
      </div>
    </div>
  );
}
