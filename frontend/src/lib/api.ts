// frontend/src/lib/api.ts
import { getSessionToken } from '@shopify/app-bridge/utilities';
import { useAppBridge } from '../hooks/useAppBridge';

const API_BASE = import.meta.env.VITE_API_BASE;

/**
 * Hook to make authenticated API calls to your backend.
 * Automatically includes session token in Authorization header.
 */
export const useAuthenticatedFetch = () => {
  const app = useAppBridge();
  
  // Overload signatures for better type inference
  async function authenticatedFetch<T = unknown>(
    endpoint: string,
    options?: RequestInit,
    returnRawResponse?: false
  ): Promise<T>;
  
  async function authenticatedFetch(
    endpoint: string,
    options: RequestInit,
    returnRawResponse: true
  ): Promise<Response>;
  
  // Implementation
  async function authenticatedFetch<T = unknown>(
    endpoint: string,
    options: RequestInit = {},
    returnRawResponse = false
  ): Promise<T | Response> {
    const url = `${API_BASE}${endpoint}`;
    
    try {
      // Get session token with timeout
      console.log('🔐 Fetching session token...');
      const tokenStart = performance.now();
      
      // Add timeout to getSessionToken
      const tokenPromise = getSessionToken(app);
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => {
          reject(new Error('Session token timeout after 10 seconds'));
        }, 10000);
      });
      
      const token = await Promise.race([tokenPromise, timeoutPromise]);
      
      const tokenElapsed = performance.now() - tokenStart;
      console.log(`✅ Session token received in ${tokenElapsed.toFixed(0)}ms`);
      console.log(`🎫 Token length: ${token.length} characters`);
      
      // Make the request with the session token
      const response = await window.fetch(url, {
        ...options,
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
          ...options.headers,
        },
      });
      
      // For blob downloads, return the raw response
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
      if (error instanceof Error && error.message.includes('timeout')) {
        console.error('⏰ Session token request timed out - this usually means:');
        console.error('   1. App Bridge is not fully initialized');
        console.error('   2. The iframe needs user interaction first');
        console.error('   3. There might be a CORS or CSP issue');
      }
      throw error;
    }
  }
  
  return authenticatedFetch;
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