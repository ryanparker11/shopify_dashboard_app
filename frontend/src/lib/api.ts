// frontend/src/lib/api.ts
import { getSessionToken } from '@shopify/app-bridge/utilities';
import { useAppBridge } from '../hooks/useAppBridge';

const API_BASE = import.meta.env.VITE_API_BASE;

// Extend Window interface for the test function
declare global {
  interface Window {
    testGetToken?: () => Promise<string>;
  }
}

// Store app instance globally for testing
let globalAppInstance: ReturnType<typeof useAppBridge> | null = null;

// Cache for session token
let cachedToken: string | null = null;


/**
 * Get session token from URL params (Shopify sends this on initial load)
 */
function getTokenFromUrl(): string | null {
  const params = new URLSearchParams(window.location.search);
  return params.get('id_token');
}

/**
 * Check if a JWT token is expired
 */
function isTokenExpired(token: string): boolean {
  try {
    const payload = JSON.parse(atob(token.split('.')[1]));
    const exp = payload.exp * 1000; // Convert to milliseconds
    return Date.now() >= exp - 10000; // 10 second buffer
  } catch {
    return true;
  }
}

/**
 * Get a valid session token (from cache, URL, or App Bridge)
 */
async function getValidToken(app: ReturnType<typeof useAppBridge>): Promise<string> {
  // Check if we have a valid cached token
  if (cachedToken && !isTokenExpired(cachedToken)) {
    console.log('✅ Using cached token');
    return cachedToken;
  }
  
  // Try to get token from URL first (Shopify sends this on initial load)
  const urlToken = getTokenFromUrl();
  if (urlToken && !isTokenExpired(urlToken)) {
    console.log('✅ Using token from URL');
    cachedToken = urlToken;
    return urlToken;
  }
  
  // Try to get from App Bridge with timeout
  console.log('🔐 Fetching new token from App Bridge...');
  try {
    const token = await Promise.race([
      getSessionToken(app),
      new Promise<never>((_, reject) => 
        setTimeout(() => reject(new Error('Token fetch timeout after 5s')), 5000)
      )
    ]);
    
    console.log('✅ Token received from App Bridge');
    cachedToken = token;
    return token;
  } catch (error) {
    console.error('❌ Failed to get token from App Bridge:', error);
    throw new Error('Unable to authenticate: Session token unavailable');
  }
}

// Standalone test function - available immediately
export const testGetToken = async () => {
  if (!globalAppInstance) {
    console.error('❌ TEST: App Bridge not initialized yet. Navigate to the app first, then try again.');
    throw new Error('App Bridge not initialized');
  }
  
  console.log('🧪 TEST: Attempting to get session token manually...');
  const start = performance.now();
  
  try {
    const token = await getValidToken(globalAppInstance);
    const elapsed = performance.now() - start;
    
    console.log('✅ TEST: Token received successfully!');
    console.log(`⏱️  TEST: Time taken: ${elapsed.toFixed(0)}ms`);
    console.log(`📏 TEST: Token length: ${token.length} characters`);
    console.log(`🎫 TEST: Token preview: ${token.substring(0, 50)}...`);
    
    // Decode and show expiry
    try {
      const payload = JSON.parse(atob(token.split('.')[1]));
      const exp = new Date(payload.exp * 1000);
      console.log(`⏰ TEST: Token expires at: ${exp.toLocaleString()}`);
    } catch {
      console.log('⚠️  Could not decode token expiry');
    }
    
    return token;
  } catch (error) {
    const elapsed = performance.now() - start;
    console.error(`❌ TEST: Token fetch failed after ${elapsed.toFixed(0)}ms`);
    console.error('❌ TEST: Error:', error);
    throw error;
  }
};

console.log('🔧 api.ts module loaded - about to set window.testGetToken');

// Expose test function globally
window.testGetToken = testGetToken;

console.log('🔧 window.testGetToken set:', typeof window.testGetToken);

/**
 * Hook to make authenticated API calls to your backend.
 * Manually adds session token to Authorization header.
 */
export const useAuthenticatedFetch = () => {
  console.log('🎣 useAuthenticatedFetch hook called');
  
  const app = useAppBridge();
  
  console.log('🎣 App Bridge instance received:', app);
  
  // Store app instance globally for testing
  globalAppInstance = app;
  
  console.log('🎣 globalAppInstance set, can now use window.testGetToken()');
  
  // Overload signatures for better type inference
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
  
  // Implementation
  async function fetch<T = unknown>(
    endpoint: string,
    options: RequestInit = {},
    returnRawResponse = false
  ): Promise<T | Response> {
    const url = `${API_BASE}${endpoint}`;
    
    try {
      console.log('🚀 Making authenticated request to:', endpoint);
      console.log('📍 Full URL:', url);
      
      // Get session token with fallback strategies
      console.log('🔐 Getting session token...');
      const tokenStart = performance.now();
      
      const token = await getValidToken(app);
      
      const tokenElapsed = performance.now() - tokenStart;
      console.log(`✅ Token retrieved in ${tokenElapsed.toFixed(0)}ms`);
      
      // Make request with manual Authorization header
      const requestStart = performance.now();
      
      const response = await window.fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
          ...options.headers,
        },
      });
      
      const requestElapsed = performance.now() - requestStart;
      console.log(`✅ Response received in ${requestElapsed.toFixed(0)}ms:`, response.status);
      
      // For blob downloads, return the raw response
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

/**
 * Helper hook for common HTTP methods
 */
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