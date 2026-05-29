'use client';

import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Skeleton } from '@/components/ui/skeleton';
import { ArrowLeft, ShoppingCart, Heart, Share2, Star, Truck, Shield, RotateCcw, Smartphone, Laptop, Headphones, Watch, Monitor, Gamepad2, Sparkles } from 'lucide-react';
import { logEvent } from '@/lib/tracking';
import { useCartStore } from '@/lib/cart-store';
import { Product } from './product-card';

const BACKEND_API_BASE_URL = (
  process.env.NEXT_PUBLIC_BACKEND_API_URL ?? 'http://localhost:8000'
).replace(/\/$/, '');

interface ProductDetailProps {
  product: Product;
  onBack?: () => void;
  onAddToCart?: (product: Product) => void;
}

interface SimilarProductResponse {
  id?: string;
  name?: string;
  product_id?: string | number;
  display_name?: string;
  price?: number;
  category?: string;
  category_name?: string;
  cluster_total_score?: number;
}

const categoryIcons: Record<string, React.ElementType> = {
  Smartphones: Smartphone,
  Laptops: Laptop,
  Audio: Headphones,
  Wearables: Watch,
  Monitors: Monitor,
  Gaming: Gamepad2,
};

export function ProductDetail({ product, onBack, onAddToCart }: ProductDetailProps) {
  const { addItem, openCart } = useCartStore();
  const [similarProducts, setSimilarProducts] = useState<Product[]>([]);
  const [isLoadingSimilar, setIsLoadingSimilar] = useState(false);

  // Log product view on mount
  useEffect(() => {
    logEvent('product_view', {
      action: 'product_detail_viewed',
      productId: product.id,
      productName: product.name,
      productPrice: product.price,
      productCategory: product.category,
      productRating: product.rating,
      viewDuration: 0,
    });
  }, [product]);

  useEffect(() => {
    let isMounted = true;

    const mapSimilarProduct = (item: SimilarProductResponse): Product | null => {
      const rawId = item.id ?? item.product_id;
      if (rawId === undefined || rawId === null) {
        return null;
      }

      const id = String(rawId);
      const score = Number(item.cluster_total_score ?? 1);

      return {
        id,
        name: String(item.name ?? item.display_name ?? `Product ${id}`),
        price: Number(item.price ?? 0),
        rating: 4.5,
        reviewCount: Math.max(1, Math.round(score)),
        category: String(item.category ?? item.category_name ?? 'Recommended'),
        inStock: true,
      };
    };

    const fetchSimilarProducts = async () => {
      setIsLoadingSimilar(true);
      try {
        const response = await fetch(
          `${BACKEND_API_BASE_URL}/api/recommend/product/${encodeURIComponent(product.id)}`,
          { cache: 'no-store' }
        );
        if (!response.ok) {
          throw new Error(`Failed to fetch similar products: ${response.status}`);
        }

        const data = (await response.json()) as SimilarProductResponse[];
        if (isMounted) {
          setSimilarProducts(
            data
              .map(mapSimilarProduct)
              .filter((item): item is Product => item !== null && item.id !== product.id)
          );
        }
      } catch (error) {
        console.error('Failed to fetch similar products:', error);
        if (isMounted) {
          setSimilarProducts([]);
        }
      } finally {
        if (isMounted) {
          setIsLoadingSimilar(false);
        }
      }
    };

    void fetchSimilarProducts();

    return () => {
      isMounted = false;
    };
  }, [product.id]);

  const handleBack = () => {
    logEvent('back_click', {
      action: 'back_to_products',
      fromProductId: product.id,
      fromProductName: product.name,
    });
    onBack?.();
  };

  const handleAddToCart = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    
    // Add to cart store (strictly once)
    addItem(product);

    logEvent('add_to_cart', {
      action: 'add_to_cart_from_detail',
      productId: product.id,
      productName: product.name,
      productPrice: product.price,
      productCategory: product.category,
      quantity: 1,
      source: 'product_detail',
    });

    // Log CART_UPDATE event
    logEvent('CART_UPDATE', {
      productId: product.id,
      productName: product.name,
      action: 'add',
      timestamp: new Date().toISOString(),
    });

    // Open cart sheet
    openCart();
    
    onAddToCart?.(product);
  };

  const handleWishlist = () => {
    logEvent('product_click', {
      action: 'wishlist_clicked',
      productId: product.id,
      productName: product.name,
    });
  };

  const handleShare = () => {
    logEvent('product_click', {
      action: 'share_clicked',
      productId: product.id,
      productName: product.name,
      shareMethod: 'button',
    });
  };

  const discount = product.originalPrice 
    ? Math.round((1 - product.price / product.originalPrice) * 100) 
    : 0;

  const IconComponent = categoryIcons[product.category] || Smartphone;

  return (
    <div className="w-full max-w-4xl mx-auto">
      {/* Back Button */}
      <motion.div whileHover={{ x: -3 }} whileTap={{ scale: 0.98 }}>
        <Button
          variant="ghost"
          size="sm"
          onClick={handleBack}
          className="mb-4 text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Products
        </Button>
      </motion.div>

      <Card className="border-border bg-card overflow-hidden">
        <CardContent className="p-0">
          <div className="grid md:grid-cols-2 gap-0">
            {/* Image Section */}
            <div className="aspect-square bg-muted flex items-center justify-center relative">
              <IconComponent className="h-32 w-32 text-muted-foreground/30" />
              {discount > 0 && (
                <span className="absolute top-4 left-4 bg-chart-3 text-white text-sm font-semibold px-3 py-1.5 rounded-lg">
                  -{discount}% OFF
                </span>
              )}
            </div>

            {/* Info Section */}
            <div className="p-8 space-y-6">
              {/* Category & Title */}
              <div>
                <p className="text-xs text-muted-foreground uppercase tracking-widest">
                  {product.category}
                </p>
                <h1 className="text-2xl font-semibold text-foreground mt-2">
                  {product.name}
                </h1>
              </div>

              {/* Rating */}
              <div className="flex items-center gap-2">
                <div className="flex items-center px-2 py-1 rounded-lg bg-muted">
                  {[...Array(5)].map((_, i) => (
                    <Star
                      key={i}
                      className={`h-4 w-4 ${
                        i < Math.floor(product.rating)
                          ? 'fill-chart-4 text-chart-4'
                          : 'text-muted-foreground/30'
                      }`}
                    />
                  ))}
                </div>
                <span className="text-sm font-medium text-foreground">{product.rating.toFixed(1)}</span>
                <span className="text-sm text-muted-foreground">
                  ({product.reviewCount.toLocaleString()} reviews)
                </span>
              </div>

              {/* Price */}
              <div className="flex items-baseline gap-3">
                <span className="text-3xl font-semibold text-foreground">
                  ${product.price.toFixed(2)}
                </span>
                {product.originalPrice && (
                  <span className="text-lg text-muted-foreground line-through">
                    ${product.originalPrice.toFixed(2)}
                  </span>
                )}
              </div>

              {/* Stock Status */}
              <div className={`text-sm font-medium ${product.inStock ? 'text-chart-2' : 'text-destructive'}`}>
                {product.inStock ? 'In Stock' : 'Out of Stock'}
              </div>

              {/* Action Buttons */}
              <div className="flex gap-3">
                <motion.div className="flex-1" whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                  <Button
                    size="lg"
                    className="w-full bg-primary text-primary-foreground hover:bg-primary/90"
                    onClick={handleAddToCart}
                    disabled={!product.inStock}
                  >
                    <ShoppingCart className="mr-2 h-5 w-5" />
                    Add to Cart
                  </Button>
                </motion.div>
                <motion.div whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}>
                  <Button
                    variant="outline"
                    size="lg"
                    onClick={handleWishlist}
                    className="border-border hover:bg-muted"
                  >
                    <Heart className="h-5 w-5" />
                  </Button>
                </motion.div>
                <motion.div whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}>
                  <Button
                    variant="outline"
                    size="lg"
                    onClick={handleShare}
                    className="border-border hover:bg-muted"
                  >
                    <Share2 className="h-5 w-5" />
                  </Button>
                </motion.div>
              </div>

              {/* Features */}
              <div className="border-t border-border pt-6 space-y-3">
                <div className="flex items-center gap-3 text-sm text-muted-foreground">
                  <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-muted">
                    <Truck className="h-4 w-4" />
                  </div>
                  <span>Free shipping on orders over $50</span>
                </div>
                <div className="flex items-center gap-3 text-sm text-muted-foreground">
                  <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-muted">
                    <Shield className="h-4 w-4" />
                  </div>
                  <span>2-year warranty included</span>
                </div>
                <div className="flex items-center gap-3 text-sm text-muted-foreground">
                  <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-muted">
                    <RotateCcw className="h-4 w-4" />
                  </div>
                  <span>30-day return policy</span>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Product Description Placeholder */}
      <Card className="mt-6 border-border bg-card">
        <CardHeader>
          <CardTitle className="text-lg font-semibold">Product Description</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground leading-relaxed">
            This is a placeholder for the product description. In a production environment, 
            this would contain detailed information about the product including specifications, 
            features, and usage instructions. The Data Lakehouse would track how users interact 
            with this content to improve recommendations and search results.
          </p>
        </CardContent>
      </Card>

      <Separator className="my-8" />

      <section className="space-y-4">
        <div>
          <h2 className="text-lg font-semibold text-foreground">Similar Products</h2>
          <p className="text-sm text-muted-foreground">Content-based recommendations for this item</p>
        </div>

        {isLoadingSimilar ? (
          <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
            {Array.from({ length: 4 }).map((_, index) => (
              <div
                key={`similar-skeleton-${index}`}
                className="overflow-hidden rounded-lg border border-border bg-card"
              >
                <Skeleton className="aspect-[4/3] w-full rounded-none" />
                <div className="space-y-2 p-3">
                  <Skeleton className="h-3 w-16" />
                  <Skeleton className="h-9 w-full" />
                  <Skeleton className="h-5 w-20" />
                </div>
              </div>
            ))}
          </div>
        ) : similarProducts.length > 0 ? (
          <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
            {similarProducts.map((similarProduct) => {
              const SimilarIcon = categoryIcons[similarProduct.category] || Smartphone;

              return (
                <motion.div
                  key={similarProduct.id}
                  whileHover={{ y: -3 }}
                  transition={{ type: 'spring', stiffness: 300, damping: 22 }}
                  className="overflow-hidden rounded-lg border border-border bg-card"
                >
                  <div className="relative flex aspect-[4/3] items-center justify-center bg-muted">
                    <SimilarIcon className="h-12 w-12 text-muted-foreground/40" />
                    <Badge className="absolute left-2 top-2 gap-1 text-[10px]">
                      <Sparkles className="h-3 w-3" />
                      Similar
                    </Badge>
                  </div>
                  <div className="space-y-2 p-3">
                    <p className="text-[10px] uppercase text-muted-foreground">
                      {similarProduct.category}
                    </p>
                    <h3 className="line-clamp-2 min-h-10 text-sm font-medium text-foreground">
                      {similarProduct.name}
                    </h3>
                    <p className="text-sm font-semibold text-foreground">
                      ${similarProduct.price.toFixed(2)}
                    </p>
                  </div>
                </motion.div>
              );
            })}
          </div>
        ) : (
          <div className="rounded-lg border border-dashed border-border p-4 text-sm text-muted-foreground">
            No similar products available yet.
          </div>
        )}
      </section>
    </div>
  );
}
