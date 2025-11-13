// frontend/src/lib/api.ts
import { authenticatedFetch as appBridgeAuthenticatedFetch } from '@shopify/app-bridge/utilities';
import { useAppBridge } from '../hooks/useAppBridge';

const API_BASE = import.meta.env.VITE_API_BASE;

/**
 * Hook to make authenticated API calls to your backend.
 * Automatically includes session token in Authorization header.
 */
export const useAuthenticatedFetch = () => {
  const app = useAppBridge();
  
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
    const authenticatedFetch = appBridgeAuthenticatedFetch(app);
    const url = `${API_BASE}${endpoint}`;
    
    try {
      const response = await authenticatedFetch(url, {
        ...options,
        headers: {
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
      console.error('API request failed:', error);
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