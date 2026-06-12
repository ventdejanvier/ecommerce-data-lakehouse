const INVALID_IMAGE_VALUES = new Set(['', 'null', 'undefined', 'nan', 'none']);
const ALLOWED_REMOTE_IMAGE_HOSTS = new Set(['picsum.photos', 'loremflickr.com']);

export const CATEGORY_IMAGE_MAP: Record<string, string> = {
  recommended: '/placeholder.jpg',
};

const normalizeCategoryKey = (category: string): string =>
  category
    .trim()
    .toLowerCase()
    .replace(/&/g, 'and')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');

const resolveValidImageUrl = (imageUrl: string | null): string | null => {
  const trimmedUrl = imageUrl?.trim() ?? '';
  if (INVALID_IMAGE_VALUES.has(trimmedUrl.toLowerCase())) {
    return null;
  }

  if (trimmedUrl.startsWith('/')) {
    return trimmedUrl;
  }

  try {
    const parsedUrl = new URL(trimmedUrl);
    if (
      parsedUrl.protocol === 'https:' &&
      ALLOWED_REMOTE_IMAGE_HOSTS.has(parsedUrl.hostname)
    ) {
      return trimmedUrl;
    }
  } catch {
    return null;
  }

  return null;
};

export const resolveProductImage = (
  imageUrl: string | null,
  category: string,
  productId: string | number
): string => {
  const validImageUrl = resolveValidImageUrl(imageUrl);
  if (validImageUrl) {
    return validImageUrl;
  }

  const categoryKey = normalizeCategoryKey(category);
  const categoryImage = CATEGORY_IMAGE_MAP[categoryKey];
  if (categoryImage) {
    return categoryImage;
  }

  const numericLock = String(productId).replace(/\D/g, '').slice(-5) || '1';
  return `https://loremflickr.com/320/240/tech,${categoryKey}?lock=${numericLock}`;
};
