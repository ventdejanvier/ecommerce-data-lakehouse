'use client';

import { motion } from 'framer-motion';
import { logEvent } from '@/lib/tracking';

export interface CategoryWithBrands {
  category_main: string;
  brands: string[];
}

interface CategoryFiltersProps {
  categoriesWithBrands: CategoryWithBrands[];
  selectedCategory: string;
  selectedBrand: string | null;
  isLoading?: boolean;
  onFilterChange: (category: string, brand: string | null) => void;
}

export function CategoryFilters({
  categoriesWithBrands,
  selectedCategory,
  selectedBrand,
  isLoading = false,
  onFilterChange,
}: CategoryFiltersProps) {
  const handleSelectAll = () => {
    logEvent('category_filter', {
      action: 'category_selected',
      categoryId: 'all',
      categoryName: 'All',
      previousCategory: selectedCategory,
      timestamp: new Date().toISOString(),
    });
    onFilterChange('all', null);
  };

  const handleCategoryClick = (categoryName: string) => {
    logEvent('category_filter', {
      action: 'category_selected',
      categoryId: categoryName,
      categoryName,
      previousCategory: selectedCategory,
      timestamp: new Date().toISOString(),
    });
    onFilterChange(categoryName, null);
  };

  const handleBrandClick = (categoryName: string, brandName: string) => {
    const nextBrand =
      selectedCategory === categoryName && selectedBrand === brandName ? null : brandName;
    onFilterChange(categoryName, nextBrand);
  };

  return (
    <div className="rounded-xl border border-border bg-card p-4">
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-foreground">Filter by Category / Brand</h3>
        {isLoading && <span className="text-xs text-muted-foreground">Loading...</span>}
      </div>

      <button
        type="button"
        onClick={handleSelectAll}
        className={`mb-4 w-full rounded-md px-3 py-2 text-left text-sm transition-colors ${
          selectedCategory === 'all'
            ? 'bg-primary text-primary-foreground'
            : 'bg-muted text-muted-foreground hover:text-foreground'
        }`}
      >
        All Products
      </button>

      <div className="space-y-3">
        {categoriesWithBrands.map((group) => {
          const isCategorySelected = selectedCategory === group.category_main;

          return (
            <motion.div
              key={group.category_main}
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              className="rounded-lg border border-border/60 p-3"
            >
              <button
                type="button"
                onClick={() => handleCategoryClick(group.category_main)}
                className={`w-full rounded-md px-2 py-1.5 text-left text-sm font-medium transition-colors ${
                  isCategorySelected
                    ? 'bg-primary/10 text-foreground'
                    : 'text-muted-foreground hover:bg-muted'
                }`}
              >
                {group.category_main}
              </button>

              <ul className="mt-2 space-y-1.5">
                {group.brands.map((brand) => {
                  const checked = isCategorySelected && selectedBrand === brand;
                  return (
                    <li key={`${group.category_main}-${brand}`}>
                      <label className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-sm text-muted-foreground hover:bg-muted">
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => handleBrandClick(group.category_main, brand)}
                          className="h-4 w-4 rounded border-border"
                        />
                        <span>{brand}</span>
                      </label>
                    </li>
                  );
                })}
              </ul>
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}
