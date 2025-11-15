// frontend/src/lib/api.ts

const API_BASE = import.meta.env.VITE_API_BASE;

// No need to manually get tokens - App Bridge handles it automatically!

export async function authenticatedFetch<T = unknown>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;
  
  console.log('🚀 API REQUEST:', endpoint);
  console.log('📍 Full URL:', url);
  
  // App Bridge CDN automatically adds Authorization header
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
      // DO NOT manually add Authorization header
      // App Bridge adds it automatically
    },
  });

  console.log(`✅ Response: ${response.status}`);

  // Handle 401 with retry header (as per Shopify docs)
  if (response.status === 401) {
    const retryHeader = response.headers.get('X-Shopify-Retry-Invalid-Session-Request');
    if (retryHeader === '1') {
      console.log('🔄 Session expired, App Bridge will retry with new token');
      // App Bridge will automatically retry with fresh token
      throw new Error('Session expired - retrying');
    }
  }

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