'use client';

import { motion } from 'framer-motion';
import { Button } from '@/components/ui/button';
import { ShoppingCart, Star, TrendingUp, Smartphone, Laptop, Headphones, Watch, Monitor, Gamepad2 } from 'lucide-react';
import { logEvent } from '@/lib/tracking';
import { useCartStore } from '@/lib/cart-store';

export interface Product {
  id: string;
  name: string;
  price: number;
  originalPrice?: number;
  rating: number;
  reviewCount: number;
  category: string;
  imageUrl?: string;
  inStock: boolean;
}

interface ProductCardProps {
  product: Product;
  onProductClick?: (product: Product) => void;
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

export function ProductCard({ product, onProductClick, onAddToCart }: ProductCardProps) {
  const { addItem, openCart } = useCartStore();

  const handleCardClick = () => {
    logEvent('product_click', {
      action: 'product_card_clicked',
      productId: product.id,
      productName: product.name,
      productPrice: product.price,
      productCategory: product.category,
      productRating: product.rating,
      inStock: product.inStock,
    });
    onProductClick?.(product);
  };

  const handleAddToCart = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    
    // Add item to cart (strictly once)
    addItem(product);
    
    // Log add_to_cart event
    logEvent('add_to_cart', {
      action: 'add_to_cart_clicked',
      productId: product.id,
      productName: product.name,
      productPrice: product.price,
      productCategory: product.category,
      quantity: 1,
      cartAction: 'add',
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

  const discount = product.originalPrice 
    ? Math.round((1 - product.price / product.originalPrice) * 100) 
    : 0;

  const IconComponent = categoryIcons[product.category] || Smartphone;

  return (
    <motion.div
      whileHover={{ y: -6 }}
      whileTap={{ scale: 0.98 }}
      transition={{ type: 'spring', stiffness: 300, damping: 20 }}
      onClick={handleCardClick}
      className="group cursor-pointer h-full overflow-hidden rounded-xl bg-card border border-border hover:border-foreground/20 hover:shadow-xl hover:shadow-foreground/5 transition-all duration-300"
    >
      {/* Image Area */}
      <div className="relative aspect-square bg-muted flex items-center justify-center overflow-hidden">
        <motion.div
          initial={{ scale: 1 }}
          whileHover={{ scale: 1.1 }}
          transition={{ duration: 0.4 }}
          className="flex items-center justify-center"
        >
          <IconComponent className="h-16 w-16 text-muted-foreground/40" />
        </motion.div>
        
        {/* Badges */}
        <div className="absolute top-3 left-3 flex flex-col gap-2">
          {discount > 0 && (
            <motion.span
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              className="bg-chart-3 text-white text-xs font-semibold px-2 py-1 rounded-md"
            >
              -{discount}%
            </motion.span>
          )}
          {product.rating >= 4.7 && (
            <motion.span
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.1 }}
              className="bg-chart-2 text-white text-xs font-semibold px-2 py-1 rounded-md flex items-center gap-1"
            >
              <TrendingUp className="h-3 w-3" />
              Popular
            </motion.span>
          )}
        </div>

        {/* Out of Stock Overlay */}
        {!product.inStock && (
          <div className="absolute inset-0 bg-background/80 flex items-center justify-center">
            <span className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">
              Out of Stock
            </span>
          </div>
        )}
      </div>

      {/* Content */}
      <div className="p-4 space-y-3">
        {/* Category */}
        <span className="text-[10px] text-muted-foreground uppercase tracking-widest">
          {product.category}
        </span>

        {/* Title */}
        <h3 className="font-medium text-foreground text-sm line-clamp-2 group-hover:text-foreground/80 transition-colors min-h-[2.5rem]">
          {product.name}
        </h3>

        {/* Rating */}
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1 px-2 py-0.5 rounded-md bg-muted">
            <Star className="h-3 w-3 fill-chart-4 text-chart-4" />
            <span className="text-xs font-medium text-foreground">{product.rating.toFixed(1)}</span>
          </div>
          <span className="text-xs text-muted-foreground">
            ({product.reviewCount.toLocaleString()})
          </span>
        </div>

        {/* Price */}
        <div className="flex items-baseline gap-2">
          <span className="text-lg font-semibold text-foreground">
            ${product.price.toFixed(2)}
          </span>
          {product.originalPrice && (
            <span className="text-sm text-muted-foreground line-through">
              ${product.originalPrice.toFixed(2)}
            </span>
          )}
        </div>

        {/* Add to Cart */}
        <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
          <Button
            size="sm"
            className="w-full rounded-lg bg-primary text-primary-foreground hover:bg-primary/90"
            onClick={handleAddToCart}
            disabled={!product.inStock}
          >
            <ShoppingCart className="mr-2 h-4 w-4" />
            Add to Cart
          </Button>
        </motion.div>
      </div>
    </motion.div>
  );
}
