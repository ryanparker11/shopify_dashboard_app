// frontend/src/lib/api.ts

const API_BASE = import.meta.env.VITE_API_BASE;

// CDN exposes idToken through window.shopify
interface ShopifyGlobal {
  idToken: () => Promise<string>;
}

declare global {
  interface Window {
    shopify?: ShopifyGlobal;
  }
}

async function getToken(): Promise<string> {
  console.log('🔐 Requesting session token from Shopify CDN...');
  
  if (!window.shopify?.idToken) {
    throw new Error('Shopify App Bridge not available');
  }

  try {
    const token = await window.shopify.idToken();
    console.log('✅ Got token from Shopify CDN:', token.substring(0, 50) + '...');
    return token;
  } catch (error) {
    console.error('❌ Failed to get token:', error);
    throw error;
  }
}

export async function authenticatedFetch<T = unknown>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;
  
  console.log('🚀 Request to:', endpoint);
  
  const token = await getToken();
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      ...options.headers,
    },
  });

  console.log(`✅ Response: ${response.status}`);

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }

  return response.json();
}

export const api = {
  get: <T = unknown>(endpoint: string) => 
    authenticatedFetch<T>(endpoint, { method: 'GET' }),
  post: <T = unknown>(endpoint: string, data?: unknown) => 
    authenticatedFetch<T>(endpoint, { method: 'POST', body: JSON.stringify(data) }),
  put: <T = unknown>(endpoint: string, data?: unknown) => 
    authenticatedFetch<T>(endpoint, { method: 'PUT', body: JSON.stringify(data) }),
  delete: <T = unknown>(endpoint: string) => 
    authenticatedFetch<T>(endpoint, { method: 'DELETE' }),
};