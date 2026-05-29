'use client';

import { Filter, Layers3 } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

export interface CategoryDetailNode {
  category_detail: string;
}

export interface CategorySubNode {
  category_sub: string;
  details: CategoryDetailNode[];
}

export interface CategoryMainNode {
  category_main: string;
  subcategories: CategorySubNode[];
}

export interface ProductFilterSelection {
  categoryMain: string;
  categorySub: string | null;
  categoryDetail: string | null;
}

interface ProductFilterProps {
  categoryTree: CategoryMainNode[];
  selection: ProductFilterSelection;
  isLoading?: boolean;
  onFilterChange: (selection: ProductFilterSelection) => void;
}

export function ProductFilter({
  categoryTree,
  selection,
  isLoading = false,
  onFilterChange,
}: ProductFilterProps) {
  const handleSelectAll = () => {
    onFilterChange({
      categoryMain: 'all',
      categorySub: null,
      categoryDetail: null,
    });
  };

  const handleSelectMain = (categoryMain: string) => {
    onFilterChange({
      categoryMain,
      categorySub: null,
      categoryDetail: null,
    });
  };

  return (
    <aside className="w-full rounded-2xl border border-border bg-card p-4">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Filter className="h-4 w-4 text-primary" />
          <h3 className="text-sm font-semibold text-foreground">Product Filters</h3>
        </div>
        {isLoading && <span className="text-xs text-muted-foreground">Loading...</span>}
      </div>

      <button
        type="button"
        onClick={handleSelectAll}
        className={cn(
          'mb-4 flex w-full items-center justify-between rounded-lg border px-3 py-2 text-sm transition-colors',
          selection.categoryMain === 'all'
            ? 'border-primary bg-primary/10 text-foreground'
            : 'border-border text-muted-foreground hover:bg-muted'
        )}
      >
        <span className="flex items-center gap-2">
          <Layers3 className="h-4 w-4" />
          All Products
        </span>
        {selection.categoryMain === 'all' && (
          <Badge className="text-[10px] uppercase tracking-wide">Active</Badge>
        )}
      </button>

      <div className="flex flex-col gap-2">
        {categoryTree.map((mainNode) => {
          const isActive = selection.categoryMain === mainNode.category_main;

          return (
            <button
              key={mainNode.category_main}
              type="button"
              onClick={() => handleSelectMain(mainNode.category_main)}
              className={cn(
                'flex w-full items-center justify-between rounded-lg border px-3 py-2.5 text-left text-sm font-medium transition-colors',
                isActive
                  ? 'border-primary bg-primary/10 text-foreground'
                  : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
              )}
            >
              <span>{mainNode.category_main}</span>
              {isActive && (
                <Badge className="text-[10px] uppercase tracking-wide">Active</Badge>
              )}
            </button>
          );
        })}
      </div>
    </aside>
  );
}
