// frontend/src/lib/api.ts

const API_BASE = import.meta.env.VITE_API_BASE;

interface ShopifyGlobal {
  idToken: () => Promise<string>;
  config: {
    apiKey: string;
  };
}

declare global {
  interface Window {
    shopify?: ShopifyGlobal;
  }
}

// Initialize Shopify config on module load
//const params = new URLSearchParams(window.location.search);
const apiKey = import.meta.env.VITE_SHOPIFY_API_KEY;

console.log('🔧 Shopify CDN Initialization Check');
console.log('window.shopify exists:', !!window.shopify);
console.log('API Key from env:', apiKey);

if (window.shopify && apiKey) {
  console.log('Current config.apiKey:', window.shopify.config?.apiKey || 'NOT SET');
  
  // Ensure config object exists
  if (!window.shopify.config) {
    window.shopify.config = { apiKey: '' };
  }
  
  // Set the API key if missing or empty
  if (!window.shopify.config.apiKey || window.shopify.config.apiKey === '') {
    window.shopify.config.apiKey = apiKey;
    console.log('✅ Set apiKey in window.shopify.config:', apiKey);
  } else {
    console.log('ℹ️ apiKey already set:', window.shopify.config.apiKey);
  }
}

async function getToken(): Promise<string> {
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🔐 TOKEN ACQUISITION START');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  
  if (!window.shopify) {
    throw new Error('window.shopify not available');
  }
  
  console.log('Config check:');
  console.log('  - apiKey:', window.shopify.config?.apiKey || 'MISSING');
  console.log('  - idToken function:', typeof window.shopify.idToken);
  
  if (typeof window.shopify.idToken !== 'function') {
    throw new Error('idToken function not available');
  }
  
  console.log('Calling window.shopify.idToken()...');
  console.log('Timestamp:', new Date().toISOString());
  
  const tokenPromise = window.shopify.idToken();
  console.log('Promise created, awaiting...');
  
  const token = await tokenPromise;
  
  console.log('✅ Token received!');
  console.log('Token length:', token?.length);
  console.log('Token preview:', token?.substring(0, 50) + '...');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  
  return token;
}

export async function authenticatedFetch<T = unknown>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;
  
  console.log('🚀 API REQUEST:', endpoint);
  
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