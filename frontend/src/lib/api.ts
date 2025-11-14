// frontend/src/lib/api.ts
import { getSessionToken } from '@shopify/app-bridge/utilities';
import { useAppBridge } from '../hooks/useAppBridge';

const API_BASE = import.meta.env.VITE_API_BASE;

declare global {
  interface Window {
    testGetToken?: () => Promise<string>;
  }
}

let globalAppInstance: ReturnType<typeof useAppBridge> | null = null;

// Token fetching state
let tokenFetchPromise: Promise<string> | null = null;

/**
 * Check if a JWT token is expired
 */
function isTokenExpired(token: string): boolean {
  try {
    const payload = JSON.parse(atob(token.split('.')[1]));
    const exp = payload.exp * 1000; // Convert to milliseconds
    const now = Date.now();
    // Add 5 second buffer to avoid edge cases
    return now >= (exp - 5000);
  } catch {
    return true; // If we can't decode it, consider it expired
  }
}

/**
 * Get session token from URL params (only if valid and not expired)
 */
function getTokenFromUrl(): string | null {
  const params = new URLSearchParams(window.location.search);
  const token = params.get('id_token');
  
  // Validate token exists and isn't expired
  if (token && !isTokenExpired(token)) {
    return token;
  }
  
  return null;
}

/**
 * Get a fresh session token from App Bridge with request queuing
 * Ensures only ONE token fetch happens at a time
 */
// frontend/src/lib/api.ts

async function getValidToken(app: ReturnType<typeof useAppBridge>): Promise<string> {
  // If a token fetch is already in progress, wait for it
  if (tokenFetchPromise) {
    console.log('⏳ Token fetch already in progress, waiting...');
    try {
      return await tokenFetchPromise;
    } catch {
      console.log('⚠️ Previous token fetch failed, retrying...');
    }
  }

  // Try URL token first
  const urlToken = getTokenFromUrl();
  if (urlToken) {
    console.log('✅ Using valid URL token');
    return urlToken;
  }

  // URL token missing or expired - fetch fresh token from App Bridge
  console.log('🔐 Fetching fresh token from App Bridge...');
  
  tokenFetchPromise = (async () => {
    try {
      console.log('🔐 Starting getSessionToken call...');
      
      const tokenPromise = getSessionToken(app);
      console.log('🔐 getSessionToken promise created:', tokenPromise);
      
      const token = await Promise.race([
        tokenPromise.then(t => {
          console.log('✅ getSessionToken resolved with token:', t.substring(0, 50) + '...');
          return t;
        }),
        new Promise<never>((_, reject) => {
          const timeoutId = setTimeout(() => {
            console.log('❌ Timeout fired after 8s');
            reject(new Error('Token fetch timeout after 8s'));
          }, 8000);
          console.log('⏰ Timeout scheduled, ID:', timeoutId);
        })
      ]);

      console.log('✅ Fresh token received from App Bridge');
      return token;
    } catch (error) {
      console.error('💥 Token fetch error:', error);
      throw error;
    } finally {
      setTimeout(() => {
        console.log('🧹 Clearing tokenFetchPromise');
        tokenFetchPromise = null;
      }, 100);
    }
  })();

  return tokenFetchPromise;
}


// Test function
export const testGetToken = async () => {
  if (!globalAppInstance) {
    console.error('❌ TEST: App Bridge not initialized yet.');
    throw new Error('App Bridge not initialized');
  }

  console.log('🧪 TEST: Attempting to get session token...');
  const start = performance.now();

  try {
    const token = await getValidToken(globalAppInstance);
    const elapsed = performance.now() - start;

    console.log('✅ TEST: Token received successfully!');
    console.log(`⏱️  TEST: Time taken: ${elapsed.toFixed(0)}ms`);
    console.log(`📏 TEST: Token length: ${token.length} characters`);

    return token;
  } catch (error) {
    const elapsed = performance.now() - start;
    console.error(`❌ TEST: Failed after ${elapsed.toFixed(0)}ms`, error);
    throw error;
  }
};

window.testGetToken = testGetToken;

export const useAuthenticatedFetch = () => {
  const app = useAppBridge();
  globalAppInstance = app;

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

      // Get FRESH session token for this request (with queuing)
      const tokenStart = performance.now();
      const token = await getValidToken(app);
      const tokenElapsed = performance.now() - tokenStart;
      console.log(`✅ Token retrieved in ${tokenElapsed.toFixed(0)}ms`);

      const response = await window.fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
          ...options.headers,
        },
      });

      console.log(`✅ Response: ${response.status}`);

      if (returnRawResponse) {
        return response;
      }

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({
          detail: `Request failed with status ${response.status}`
        }));
        throw new Error(errorData.detail || errorData.message || 'Request failed');
      }

      return await response.json();
    } catch (error) {
      console.error('💥 API request failed:', error);
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