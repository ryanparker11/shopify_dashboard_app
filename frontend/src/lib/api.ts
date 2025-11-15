// frontend/src/lib/api.ts

const API_BASE = import.meta.env.VITE_API_BASE;

console.log('🔧 App Bridge API Module Loaded');
console.log('API Base URL:', API_BASE);

export async function authenticatedFetch<T = unknown>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;
  
  console.log('🚀 API REQUEST:', endpoint);
  
  // App Bridge CDN automatically intercepts fetch() and adds Authorization header
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });

  console.log(`📥 Response: ${response.status}`);

  // DO NOT handle 401 here - let it bubble up so App Bridge can retry
  // App Bridge will automatically retry with a fresh token when it sees:
  // - 401 status
  // - X-Shopify-Retry-Invalid-Session-Request header

  if (!response.ok) {
    const errorText = await response.text().catch(() => 'Unknown error');
    console.error('❌ Request failed:', errorText);
    throw new Error(`Request failed: ${response.status}`);
  }

  const data = await response.json();
  console.log('✅ Request successful');
  
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