'use client';

import { useEffect } from 'react';
import { motion } from 'framer-motion';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { ArrowLeft, ShoppingCart, Heart, Share2, Star, Truck, Shield, RotateCcw, Smartphone, Laptop, Headphones, Watch, Monitor, Gamepad2 } from 'lucide-react';
import { logEvent } from '@/lib/tracking';
import { useCartStore } from '@/lib/cart-store';
import { Product } from './product-card';

interface ProductDetailProps {
  product: Product;
  onBack?: () => void;
  onAddToCart?: (product: Product) => void;
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
    </div>
  );
}
