'use client';

import { Filter, Layers3 } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion';
import { Checkbox } from '@/components/ui/checkbox';
import { cn } from '@/lib/utils';

export interface CategoryDetailNode {
  category_detail: string;
  brands: string[];
}

export interface CategorySubNode {
  category_sub: string;
  brands: string[];
  details: CategoryDetailNode[];
}

export interface CategoryMainNode {
  category_main: string;
  brands: string[];
  subcategories: CategorySubNode[];
}

export interface ProductFilterSelection {
  categoryMain: string;
  categorySub: string | null;
  categoryDetail: string | null;
  brands: string[];
}

interface ProductFilterProps {
  categoryTree: CategoryMainNode[];
  selection: ProductFilterSelection;
  isLoading?: boolean;
  onFilterChange: (selection: ProductFilterSelection) => void;
}

function uniqueBrands(brands: string[]) {
  return Array.from(new Set(brands));
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
      brands: [],
    });
  };

  const handleSelectMain = (categoryMain: string) => {
    onFilterChange({
      categoryMain,
      categorySub: null,
      categoryDetail: null,
      brands: [],
    });
  };

  const handleSelectSub = (categoryMain: string, categorySub: string) => {
    onFilterChange({
      categoryMain,
      categorySub,
      categoryDetail: null,
      brands: [],
    });
  };

  const handleSelectDetail = (
    categoryMain: string,
    categorySub: string,
    categoryDetail: string
  ) => {
    onFilterChange({
      categoryMain,
      categorySub,
      categoryDetail,
      brands: [],
    });
  };

  const handleToggleBrand = (
    path: {
      categoryMain: string;
      categorySub: string | null;
      categoryDetail: string | null;
    },
    brand: string,
    checked: boolean
  ) => {
    const isSamePath =
      selection.categoryMain === path.categoryMain &&
      selection.categorySub === path.categorySub &&
      selection.categoryDetail === path.categoryDetail;

    if (!isSamePath) {
      onFilterChange({
        categoryMain: path.categoryMain,
        categorySub: path.categorySub,
        categoryDetail: path.categoryDetail,
        brands: checked ? [brand] : [],
      });
      return;
    }

    const nextBrands = checked
      ? uniqueBrands([...selection.brands, brand])
      : selection.brands.filter((item) => item !== brand);

    onFilterChange({
      categoryMain: selection.categoryMain,
      categorySub: selection.categorySub,
      categoryDetail: selection.categoryDetail,
      brands: nextBrands,
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

      <Accordion
        type="multiple"
        defaultValue={categoryTree.slice(0, 1).map((group) => group.category_main)}
        className="space-y-1"
      >
        {categoryTree.map((mainNode) => {
          const isMainActive =
            selection.categoryMain === mainNode.category_main &&
            selection.categorySub === null &&
            selection.categoryDetail === null;
          const isMainPath = selection.categoryMain === mainNode.category_main;

          return (
            <AccordionItem key={mainNode.category_main} value={mainNode.category_main} className="border-b-0">
              <AccordionTrigger
                className={cn(
                  'rounded-lg px-2 py-2.5 hover:no-underline',
                  isMainPath ? 'bg-primary/10 text-foreground' : 'hover:bg-muted'
                )}
              >
                <div className="flex w-full items-center justify-between">
                  <span className="text-sm font-medium">{mainNode.category_main}</span>
                  <Badge variant="secondary" className="text-[10px]">
                    {mainNode.subcategories.length}
                  </Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent className="space-y-3 pb-2">
                <button
                  type="button"
                  onClick={() => handleSelectMain(mainNode.category_main)}
                  className={cn(
                    'w-full rounded-md px-2 py-1.5 text-left text-xs font-medium transition-colors',
                    isMainActive ? 'bg-primary/15 text-foreground' : 'text-muted-foreground hover:bg-muted'
                  )}
                >
                  Only {mainNode.category_main}
                </button>

                <div className="space-y-1">
                  {mainNode.brands.map((brand) => {
                    const checked =
                      isMainActive && selection.brands.includes(brand);
                    return (
                      <label
                        key={`${mainNode.category_main}-brand-${brand}`}
                        className="flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-sm text-muted-foreground hover:bg-muted"
                      >
                        <Checkbox
                          checked={checked}
                          onCheckedChange={(next) =>
                            handleToggleBrand(
                              {
                                categoryMain: mainNode.category_main,
                                categorySub: null,
                                categoryDetail: null,
                              },
                              brand,
                              next === true
                            )
                          }
                        />
                        <span>{brand}</span>
                      </label>
                    );
                  })}
                </div>

                <Accordion type="multiple" className="space-y-1 border-l border-border/70 pl-3">
                  {mainNode.subcategories.map((subNode) => {
                    const isSubActive =
                      selection.categoryMain === mainNode.category_main &&
                      selection.categorySub === subNode.category_sub &&
                      selection.categoryDetail === null;
                    const isSubPath =
                      selection.categoryMain === mainNode.category_main &&
                      selection.categorySub === subNode.category_sub;

                    return (
                      <AccordionItem
                        key={`${mainNode.category_main}-${subNode.category_sub}`}
                        value={`${mainNode.category_main}-${subNode.category_sub}`}
                        className="border-b-0"
                      >
                        <AccordionTrigger
                          className={cn(
                            'rounded-md px-2 py-2 text-sm hover:no-underline',
                            isSubPath ? 'bg-primary/10 text-foreground' : 'hover:bg-muted'
                          )}
                        >
                          <div className="flex w-full items-center justify-between">
                            <span>{subNode.category_sub}</span>
                            <Badge variant="secondary" className="text-[10px]">
                              {subNode.details.length}
                            </Badge>
                          </div>
                        </AccordionTrigger>
                        <AccordionContent className="space-y-2 pb-1">
                          <button
                            type="button"
                            onClick={() => handleSelectSub(mainNode.category_main, subNode.category_sub)}
                            className={cn(
                              'w-full rounded-md px-2 py-1 text-left text-xs font-medium transition-colors',
                              isSubActive
                                ? 'bg-primary/15 text-foreground'
                                : 'text-muted-foreground hover:bg-muted'
                            )}
                          >
                            Only {subNode.category_sub}
                          </button>

                          <div className="space-y-1">
                            {subNode.brands.map((brand) => {
                              const checked =
                                isSubActive && selection.brands.includes(brand);
                              return (
                                <label
                                  key={`${mainNode.category_main}-${subNode.category_sub}-brand-${brand}`}
                                  className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-sm text-muted-foreground hover:bg-muted"
                                >
                                  <Checkbox
                                    checked={checked}
                                    onCheckedChange={(next) =>
                                      handleToggleBrand(
                                        {
                                          categoryMain: mainNode.category_main,
                                          categorySub: subNode.category_sub,
                                          categoryDetail: null,
                                        },
                                        brand,
                                        next === true
                                      )
                                    }
                                  />
                                  <span>{brand}</span>
                                </label>
                              );
                            })}
                          </div>

                          <Accordion type="multiple" className="space-y-1 border-l border-border/70 pl-3">
                            {subNode.details.map((detailNode) => {
                              const isDetailActive =
                                selection.categoryMain === mainNode.category_main &&
                                selection.categorySub === subNode.category_sub &&
                                selection.categoryDetail === detailNode.category_detail;

                              return (
                                <AccordionItem
                                  key={`${mainNode.category_main}-${subNode.category_sub}-${detailNode.category_detail}`}
                                  value={`${mainNode.category_main}-${subNode.category_sub}-${detailNode.category_detail}`}
                                  className="border-b-0"
                                >
                                  <AccordionTrigger
                                    className={cn(
                                      'rounded-md px-2 py-1.5 text-sm hover:no-underline',
                                      isDetailActive ? 'bg-primary/10 text-foreground' : 'hover:bg-muted'
                                    )}
                                  >
                                    <div className="flex w-full items-center justify-between">
                                      <span>{detailNode.category_detail}</span>
                                      <Badge variant="secondary" className="text-[10px]">
                                        {detailNode.brands.length}
                                      </Badge>
                                    </div>
                                  </AccordionTrigger>
                                  <AccordionContent className="space-y-1 pb-1">
                                    <button
                                      type="button"
                                      onClick={() =>
                                        handleSelectDetail(
                                          mainNode.category_main,
                                          subNode.category_sub,
                                          detailNode.category_detail
                                        )
                                      }
                                      className={cn(
                                        'w-full rounded-md px-2 py-1 text-left text-xs font-medium transition-colors',
                                        isDetailActive
                                          ? 'bg-primary/15 text-foreground'
                                          : 'text-muted-foreground hover:bg-muted'
                                      )}
                                    >
                                      Only {detailNode.category_detail}
                                    </button>

                                    {detailNode.brands.map((brand) => {
                                      const checked =
                                        isDetailActive && selection.brands.includes(brand);
                                      return (
                                        <label
                                          key={`${mainNode.category_main}-${subNode.category_sub}-${detailNode.category_detail}-brand-${brand}`}
                                          className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-sm text-muted-foreground hover:bg-muted"
                                        >
                                          <Checkbox
                                            checked={checked}
                                            onCheckedChange={(next) =>
                                              handleToggleBrand(
                                                {
                                                  categoryMain: mainNode.category_main,
                                                  categorySub: subNode.category_sub,
                                                  categoryDetail: detailNode.category_detail,
                                                },
                                                brand,
                                                next === true
                                              )
                                            }
                                          />
                                          <span>{brand}</span>
                                        </label>
                                      );
                                    })}
                                  </AccordionContent>
                                </AccordionItem>
                              );
                            })}
                          </Accordion>
                        </AccordionContent>
                      </AccordionItem>
                    );
                  })}
                </Accordion>
              </AccordionContent>
            </AccordionItem>
          );
        })}
      </Accordion>
    </aside>
  );
}
