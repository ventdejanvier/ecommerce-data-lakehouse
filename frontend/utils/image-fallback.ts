export const CATEGORY_IMAGE_MAP: Record<string, string> = {
  accessories: '/images/categories/accessories.jpg',
  apparel: '/images/categories/apparel.jpg',
  appliances: '/images/categories/appliances.jpg',
  auto: '/images/categories/auto.jpg',
  computers: '/images/categories/computers.jpg',
  construction: '/images/categories/construction.jpg',
  country_yard: '/images/categories/country_yard.jpg',
  electronics: '/images/categories/electronics.jpg',
  furniture: '/images/categories/furniture.jpg',
  jewelry: '/images/categories/jewelry.jpg',
  kids: '/images/categories/kids.jpg',
  medicine: '/images/categories/medicine.jpg',
  sport: '/images/categories/sport.jpg',
  stationery: '/images/categories/stationery.jpg',
  uncategorized: '/images/categories/uncategorized.jpg',
};

const normalizeCategoryKey = (category: string): string =>
  category
    .trim()
    .toLowerCase()
    .replace(/&/g, ' ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

export const resolveProductImage = (
  imageUrl: string | null,
  category: string,
  productId: string | number
): string => {
  void imageUrl;
  void productId;

  const categoryKey = normalizeCategoryKey(category);
  return CATEGORY_IMAGE_MAP[categoryKey] ?? CATEGORY_IMAGE_MAP.uncategorized;
};
