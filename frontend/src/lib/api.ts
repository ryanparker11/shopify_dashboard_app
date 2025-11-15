// frontend/src/lib/api.ts

const API_BASE = import.meta.env.VITE_API_BASE;

interface ShopifyGlobal {
  idToken: () => Promise<string>;
}

declare global {
  interface Window {
    shopify?: ShopifyGlobal;
  }
}

async function getToken(): Promise<string> {
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('🔐 TOKEN ACQUISITION START');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  
  console.log('Step 1: Checking if window.shopify exists...');
  console.log('window.shopify:', window.shopify ? 'EXISTS ✅' : 'MISSING ❌');
  
  if (!window.shopify) {
    console.error('❌ FATAL: window.shopify is undefined');
    throw new Error('Shopify App Bridge not available');
  }
  
  console.log('Step 2: Checking if window.shopify.idToken exists...');
  console.log('window.shopify.idToken:', typeof window.shopify.idToken !== 'undefined' ? 'EXISTS ✅' : 'MISSING ❌');
  console.log('typeof window.shopify.idToken:', typeof window.shopify.idToken);
  
  if (!window.shopify.idToken) {
    console.error('❌ FATAL: window.shopify.idToken is undefined');
    throw new Error('idToken function not available');
  }
  
  console.log('Step 3: Calling window.shopify.idToken()...');
  console.log('Timestamp before call:', new Date().toISOString());
  
  try {
    const tokenPromise = window.shopify.idToken();
    console.log('Step 4: Promise created:', tokenPromise);
    console.log('Promise type:', typeof tokenPromise);
    console.log('Is Promise?:', tokenPromise instanceof Promise);
    
    console.log('Step 5: Awaiting promise...');
    const token = await tokenPromise;
    
    console.log('Step 6: Promise resolved!');
    console.log('Timestamp after resolution:', new Date().toISOString());
    console.log('Token type:', typeof token);
    console.log('Token length:', token?.length);
    console.log('Token preview:', token ? token.substring(0, 50) + '...' : 'EMPTY');
    
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('✅ TOKEN ACQUISITION SUCCESS');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    
    return token;
  } catch (error) {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.error('❌ TOKEN ACQUISITION FAILED');
    console.error('Error type:', error instanceof Error ? error.constructor.name : typeof error);
    console.error('Error message:', error instanceof Error ? error.message : String(error));
    console.error('Error stack:', error instanceof Error ? error.stack : 'N/A');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    throw error;
  }
}

export async function authenticatedFetch<T = unknown>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;
  
  console.log('');
  console.log('🚀 API REQUEST:', endpoint);
  console.log('Full URL:', url);
  
  const token = await getToken();
  
  console.log('📤 Sending request with token...');
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      ...options.headers,
    },
  });

  console.log(`📥 Response received: ${response.status} ${response.statusText}`);

  if (!response.ok) {
    const errorText = await response.text();
    console.error('❌ Request failed:', errorText);
    throw new Error(`Request failed: ${response.status}`);
  }

  const data = await response.json();
  console.log('✅ Request successful, data received');
  
  return data;
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