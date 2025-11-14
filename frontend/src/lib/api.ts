// frontend/src/lib/api.ts
import { authenticatedFetch } from '@shopify/app-bridge/utilities';
import { useAppBridge } from '../hooks/useAppBridge';

const API_BASE = import.meta.env.VITE_API_BASE;

declare global {
  interface Window {
    testAuthenticatedFetch?: () => Promise<Response>;
  }
}

let globalAuthenticatedFetch: ReturnType<typeof authenticatedFetch> | null = null;

// Test function for authenticated fetch
export const testAuthenticatedFetch = async () => {
  if (!globalAuthenticatedFetch) {
    console.error('❌ TEST: Authenticated fetch not initialized yet.');
    throw new Error('Authenticated fetch not initialized');
  }

  console.log('🧪 TEST: Attempting authenticated fetch...');
  const start = performance.now();

  try {
    // Make a test request to your backend
    const response = await globalAuthenticatedFetch(`${API_BASE}/api/me`, {
      method: 'GET',
    });
    
    const elapsed = performance.now() - start;

    console.log('✅ TEST: Request completed successfully!');
    console.log(`⏱️  TEST: Time taken: ${elapsed.toFixed(0)}ms`);
    console.log(`📊 TEST: Status: ${response.status}`);
    console.log(`📋 TEST: Headers:`, response.headers);

    return response;
  } catch (error) {
    const elapsed = performance.now() - start;
    console.error(`❌ TEST: Failed after ${elapsed.toFixed(0)}ms`, error);
    throw error;
  }
};

window.testAuthenticatedFetch = testAuthenticatedFetch;

export const useAuthenticatedFetch = () => {
  const app = useAppBridge();

  console.log('🔧 Initializing Shopify authenticatedFetch...');
  
  // Create Shopify's authenticated fetch function
  const shopifyFetch = authenticatedFetch(app);
  globalAuthenticatedFetch = shopifyFetch;
  
  console.log('✅ Shopify authenticatedFetch initialized');

  async function fetch<T = unknown>(
    endpoint: string,
    options?: RequestInit,
    returnRawResponse?: false
  ): Promise<T>;

  async function fetch(
    endpoint: string,
    options: RequestInit,
    returnRawResponse: true
  ): Promise<Response>;

  async function fetch<T = unknown>(
    endpoint: string,
    options: RequestInit = {},
    returnRawResponse = false
  ): Promise<T | Response> {
    const url = `${API_BASE}${endpoint}`;

    try {
      console.log('🚀 Making authenticated request to:', endpoint);
      console.log('📍 Full URL:', url);

      const requestStart = performance.now();

      // Use Shopify's authenticatedFetch - it handles tokens internally
      console.log('🔐 Using Shopify authenticatedFetch (handles tokens automatically)...');
      const response = await shopifyFetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          ...options.headers,
        },
      });

      const requestElapsed = performance.now() - requestStart;
      console.log(`✅ Response received in ${requestElapsed.toFixed(0)}ms: ${response.status}`);

      if (returnRawResponse) {
        return response;
      }

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({
          detail: `Request failed with status ${response.status}`
        }));
        console.error('❌ Request failed:', errorData);
        throw new Error(errorData.detail || errorData.message || 'Request failed');
      }

      const data = await response.json();
      console.log('✅ Data received successfully');
      return data;
    } catch (error) {
      console.error('💥 API request failed:', error);
      if (error instanceof Error) {
        console.error('💥 Error message:', error.message);
        console.error('💥 Error stack:', error.stack);
      }
      throw error;
    }
  }

  return fetch;
};

export const useApi = () => {
  const authenticatedFetch = useAuthenticatedFetch();

  return {
    get: <T = unknown>(endpoint: string) =>
      authenticatedFetch<T>(endpoint, { method: 'GET' }),

    post: <T = unknown>(endpoint: string, data?: unknown) =>
      authenticatedFetch<T>(endpoint, {
        method: 'POST',
        body: JSON.stringify(data),
      }),

    put: <T = unknown>(endpoint: string, data?: unknown) =>
      authenticatedFetch<T>(endpoint, {
        method: 'PUT',
        body: JSON.stringify(data),
      }),

    delete: <T = unknown>(endpoint: string) =>
      authenticatedFetch<T>(endpoint, { method: 'DELETE' }),
  };
};