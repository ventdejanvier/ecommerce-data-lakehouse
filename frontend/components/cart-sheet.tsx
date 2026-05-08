'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
  SheetFooter,
} from '@/components/ui/sheet';
import { Button } from '@/components/ui/button';
import { useCartStore } from '@/lib/cart-store';
import { logEvent } from '@/lib/tracking';
import { useToast } from '@/hooks/use-toast';
import {
  Plus,
  Minus,
  Trash2,
  ShoppingBag,
  CreditCard,
  Smartphone,
  Laptop,
  Headphones,
  Watch,
  Monitor,
  Gamepad2,
  CheckCircle2,
  Loader2,
} from 'lucide-react';

const categoryIcons: Record<string, React.ElementType> = {
  Smartphones: Smartphone,
  Laptops: Laptop,
  Audio: Headphones,
  Wearables: Watch,
  Monitors: Monitor,
  Gaming: Gamepad2,
};

export function CartSheet() {
  const {
    items,
    isOpen,
    closeCart,
    increaseQuantity,
    decreaseQuantity,
    removeItem,
    clearCart,
    getTotalPrice,
    getTotalItems,
  } = useCartStore();
  const { toast } = useToast();
  const [isCheckingOut, setIsCheckingOut] = useState(false);
  const [isMounted, setIsMounted] = useState(false);
  
  // Hydration fix: only render cart data after client mount
  useEffect(() => {
    setIsMounted(true);
  }, []);

  const handleIncreaseQuantity = (productId: string, productName: string) => {
    increaseQuantity(productId);
    logEvent('CART_UPDATE', {
      productId,
      productName,
      action: 'increase',
      timestamp: new Date().toISOString(),
    });
  };

  const handleDecreaseQuantity = (productId: string, productName: string) => {
    decreaseQuantity(productId);
    logEvent('CART_UPDATE', {
      productId,
      productName,
      action: 'decrease',
      timestamp: new Date().toISOString(),
    });
  };

  const handleRemoveItem = (productId: string, productName: string) => {
    removeItem(productId);
    logEvent('CART_UPDATE', {
      productId,
      productName,
      action: 'remove',
      timestamp: new Date().toISOString(),
    });
  };

  const handleCheckout = async () => {
    const totalValue = getTotalPrice();
    const itemCount = getTotalItems();

    // CRITICAL: Log CHECKOUT_INITIATE event
    logEvent('CHECKOUT_INITIATE', {
      totalValue,
      itemCount,
      items: items.map((item) => ({
        productId: item.product.id,
        productName: item.product.name,
        quantity: item.quantity,
        price: item.product.price,
      })),
      timestamp: new Date().toISOString(),
    });

    setIsCheckingOut(true);

    // Simulate checkout process
    await new Promise((resolve) => setTimeout(resolve, 1500));

    // CRITICAL: Log PURCHASE_COMPLETED event
    logEvent('PURCHASE_COMPLETED', {
      totalValue,
      itemCount,
      orderId: `ORD_${Date.now()}_${Math.random().toString(36).substring(2, 8).toUpperCase()}`,
      items: items.map((item) => ({
        productId: item.product.id,
        productName: item.product.name,
        quantity: item.quantity,
        price: item.product.price,
        subtotal: item.product.price * item.quantity,
      })),
      timestamp: new Date().toISOString(),
    });

    // Clear cart and close
    clearCart();
    closeCart();
    setIsCheckingOut(false);

    // Show success toast
    toast({
      title: 'Order Placed Successfully!',
      description: `Your order of $${totalValue.toFixed(2)} has been confirmed. Check console for telemetry.`,
    });
  };

  const totalPrice = isMounted ? getTotalPrice() : 0;
  const totalItems = isMounted ? getTotalItems() : 0;
  const cartItems = isMounted ? items : [];

  // Show loading skeleton while hydrating
  if (!isMounted) {
    return (
      <Sheet open={isOpen} onOpenChange={(open) => !open && closeCart()}>
        <SheetContent className="flex flex-col w-full sm:max-w-lg">
          <SheetHeader>
            <SheetTitle className="flex items-center gap-2">
              <ShoppingBag className="h-5 w-5" />
              Shopping Cart
            </SheetTitle>
            <SheetDescription>Loading cart...</SheetDescription>
          </SheetHeader>
          <div className="flex-1 flex items-center justify-center">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        </SheetContent>
      </Sheet>
    );
  }

  return (
    <Sheet open={isOpen} onOpenChange={(open) => !open && closeCart()}>
      <SheetContent className="flex flex-col w-full sm:max-w-lg">
        <SheetHeader>
          <SheetTitle className="flex items-center gap-2">
            <ShoppingBag className="h-5 w-5" />
            Shopping Cart
            {totalItems > 0 && (
              <span className="ml-2 px-2 py-0.5 text-xs font-medium bg-primary text-primary-foreground rounded-full">
                {totalItems}
              </span>
            )}
          </SheetTitle>
          <SheetDescription>
            {totalItems === 0
              ? 'Your cart is empty'
              : `${totalItems} item${totalItems > 1 ? 's' : ''} in your cart`}
          </SheetDescription>
        </SheetHeader>

        {/* Cart Items */}
        <div className="flex-1 overflow-y-auto py-4">
          <AnimatePresence mode="popLayout">
            {cartItems.length === 0 ? (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="flex flex-col items-center justify-center h-full text-center px-4"
              >
                <div className="flex h-16 w-16 items-center justify-center rounded-full bg-muted mb-4">
                  <ShoppingBag className="h-8 w-8 text-muted-foreground" />
                </div>
                <p className="text-muted-foreground text-sm">
                  Start adding products to your cart
                </p>
              </motion.div>
            ) : (
              <div className="space-y-4 px-1">
                {cartItems.map((item) => {
                  const IconComponent = categoryIcons[item.product.category] || Smartphone;
                  return (
                    <motion.div
                      key={item.product.id}
                      layout
                      initial={{ opacity: 0, x: 20 }}
                      animate={{ opacity: 1, x: 0 }}
                      exit={{ opacity: 0, x: -20 }}
                      className="flex gap-4 p-3 rounded-xl bg-muted/50 border border-border"
                    >
                      {/* Product Icon */}
                      <div className="flex h-16 w-16 items-center justify-center rounded-lg bg-background border border-border flex-shrink-0">
                        <IconComponent className="h-8 w-8 text-muted-foreground" />
                      </div>

                      {/* Product Info */}
                      <div className="flex-1 min-w-0">
                        <h4 className="font-medium text-sm text-foreground truncate">
                          {item.product.name}
                        </h4>
                        <p className="text-xs text-muted-foreground">
                          {item.product.category}
                        </p>
                        <p className="text-sm font-semibold text-foreground mt-1">
                          ${item.product.price.toFixed(2)}
                        </p>
                      </div>

                      {/* Quantity Controls */}
                      <div className="flex flex-col items-end gap-2">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-6 w-6 text-muted-foreground hover:text-destructive hover:bg-destructive/10"
                          onClick={() => handleRemoveItem(item.product.id, item.product.name)}
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </Button>
                        <div className="flex items-center gap-1">
                          <Button
                            variant="outline"
                            size="icon"
                            className="h-7 w-7 border-border"
                            onClick={() => handleDecreaseQuantity(item.product.id, item.product.name)}
                          >
                            <Minus className="h-3 w-3" />
                          </Button>
                          <span className="w-8 text-center text-sm font-medium text-foreground">
                            {item.quantity}
                          </span>
                          <Button
                            variant="outline"
                            size="icon"
                            className="h-7 w-7 border-border"
                            onClick={() => handleIncreaseQuantity(item.product.id, item.product.name)}
                          >
                            <Plus className="h-3 w-3" />
                          </Button>
                        </div>
                      </div>
                    </motion.div>
                  );
                })}
              </div>
            )}
          </AnimatePresence>
        </div>

        {/* Footer with Total & Checkout */}
        {cartItems.length > 0 && (
          <SheetFooter className="border-t border-border pt-4">
            <div className="w-full space-y-4">
              {/* Price Breakdown */}
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">Subtotal</span>
                  <span className="text-foreground">${totalPrice.toFixed(2)}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">Shipping</span>
                  <span className="text-chart-2 font-medium">Free</span>
                </div>
                <div className="flex justify-between text-base font-semibold pt-2 border-t border-border">
                  <span className="text-foreground">Total</span>
                  <span className="text-foreground">${totalPrice.toFixed(2)}</span>
                </div>
              </div>

              {/* Checkout Button */}
              <motion.div whileHover={{ scale: 1.01 }} whileTap={{ scale: 0.99 }}>
                <Button
                  className="w-full h-12 text-base bg-primary text-primary-foreground hover:bg-primary/90"
                  onClick={handleCheckout}
                  disabled={isCheckingOut}
                >
                  {isCheckingOut ? (
                    <>
                      <motion.div
                        animate={{ rotate: 360 }}
                        transition={{ repeat: Infinity, duration: 1, ease: 'linear' }}
                      >
                        <CheckCircle2 className="mr-2 h-5 w-5" />
                      </motion.div>
                      Processing...
                    </>
                  ) : (
                    <>
                      <CreditCard className="mr-2 h-5 w-5" />
                      Proceed to Checkout
                    </>
                  )}
                </Button>
              </motion.div>
            </div>
          </SheetFooter>
        )}
      </SheetContent>
    </Sheet>
  );
}
