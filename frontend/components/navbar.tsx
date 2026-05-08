'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Button } from '@/components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Cpu, User, UserCircle, Activity, ShoppingCart, LogOut } from 'lucide-react';
import { logEvent } from '@/lib/tracking';
import { useAuthStore } from '@/lib/auth-store';
import { useCartStore } from '@/lib/cart-store';

export function Navbar() {
  const { user, openLoginModal, logout } = useAuthStore();
  const { openCart, getTotalItems } = useCartStore();
  const [isMounted, setIsMounted] = useState(false);
  
  // Hydration fix: only render cart count after client mount
  useEffect(() => {
    setIsMounted(true);
  }, []);
  
  const totalItems = isMounted ? getTotalItems() : 0;

  const handleLoginClick = () => {
    logEvent('login_click', {
      action: 'login_button_pressed',
      location: 'navbar',
      buttonText: 'Login',
    });
    openLoginModal();
  };

  const handleGuestClick = () => {
    logEvent('guest_click', {
      action: 'guest_button_pressed',
      location: 'navbar',
      buttonText: 'Continue as Guest',
    });
  };

  const handleLogout = () => {
    logEvent('LOGOUT', {
      userId: user?.id,
      action: 'logout_clicked',
      location: 'navbar',
    });
    logout();
  };

  const handleCartClick = () => {
    logEvent('CART_OPEN', {
      action: 'cart_opened',
      location: 'navbar',
      itemCount: totalItems,
    });
    openCart();
  };

  const handleLogoClick = () => {
    // Smooth scroll to top
    window.scrollTo({ top: 0, behavior: 'smooth' });
    
    // Log NAVIGATE_HOME event
    logEvent('NAVIGATE_HOME', {
      action: 'logo_clicked',
      location: 'navbar',
      timestamp: new Date().toISOString(),
    });
  };

  return (
    <motion.nav
      initial={{ y: -20, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      transition={{ duration: 0.4, ease: 'easeOut' }}
      className="sticky top-0 z-50 w-full border-b border-border bg-background/80 backdrop-blur-md"
    >
      <div className="container mx-auto flex h-16 items-center justify-between px-4">
        {/* Logo / Brand */}
        <motion.button
          onClick={handleLogoClick}
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          className="flex items-center gap-3 text-foreground"
        >
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary">
            <Cpu className="h-5 w-5 text-primary-foreground" />
          </div>
          <div className="flex flex-col items-start">
            <span className="text-lg font-semibold tracking-tight">TechStore</span>
            <span className="text-[10px] text-muted-foreground uppercase tracking-widest">
              Data Lakehouse Demo
            </span>
          </div>
        </motion.button>

        {/* Center Badge */}
        <motion.div
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ delay: 0.2 }}
          className="hidden md:flex items-center gap-2 px-4 py-1.5 rounded-full bg-muted border border-border"
        >
          <Activity className="h-3.5 w-3.5 text-chart-2" />
          <span className="text-xs font-medium text-muted-foreground">Telemetry Active</span>
        </motion.div>

        {/* Right Section */}
        <div className="flex items-center gap-2">
          {/* Cart Button */}
          <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
            <Button
              variant="ghost"
              size="icon"
              onClick={handleCartClick}
              className="relative text-muted-foreground hover:text-foreground hover:bg-muted"
            >
              <ShoppingCart className="h-5 w-5" />
              {totalItems > 0 && (
                <motion.span
                  initial={{ scale: 0 }}
                  animate={{ scale: 1 }}
                  className="absolute -top-1 -right-1 flex h-5 w-5 items-center justify-center rounded-full bg-primary text-primary-foreground text-[10px] font-semibold"
                >
                  {totalItems > 99 ? '99+' : totalItems}
                </motion.span>
              )}
            </Button>
          </motion.div>

          {/* Auth Section */}
          {user ? (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                  <Button
                    size="sm"
                    className="bg-primary text-primary-foreground hover:bg-primary/90"
                  >
                    <div className="flex h-5 w-5 items-center justify-center rounded-full bg-primary-foreground/20 mr-2">
                      <span className="text-xs font-semibold">
                        {user.name.charAt(0).toUpperCase()}
                      </span>
                    </div>
                    {user.name}
                  </Button>
                </motion.div>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-48">
                <div className="px-2 py-1.5">
                  <p className="text-sm font-medium text-foreground">{user.name}</p>
                  <p className="text-xs text-muted-foreground">{user.email}</p>
                </div>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={handleLogout} className="text-destructive focus:text-destructive">
                  <LogOut className="mr-2 h-4 w-4" />
                  Sign out
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          ) : (
            <>
              <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={handleGuestClick}
                  className="text-muted-foreground hover:text-foreground hover:bg-muted"
                >
                  <UserCircle className="mr-2 h-4 w-4" />
                  Guest
                </Button>
              </motion.div>
              <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
                <Button
                  size="sm"
                  onClick={handleLoginClick}
                  className="bg-primary text-primary-foreground hover:bg-primary/90"
                >
                  <User className="mr-2 h-4 w-4" />
                  Login
                </Button>
              </motion.div>
            </>
          )}
        </div>
      </div>
    </motion.nav>
  );
}
