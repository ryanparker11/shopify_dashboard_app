// frontend/src/lib/api.ts
import { useAppBridge } from '../hooks/useAppBridge';

const API_BASE = import.meta.env.VITE_API_BASE;

function getTokenFromUrl(): string | null {
  const params = new URLSearchParams(window.location.search);
  return params.get('id_token');
}

export const useAuthenticatedFetch = () => {
  const app = useAppBridge();

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

      // FALLBACK STRATEGY:
      // 1. Try URL token (works immediately on page load)
      // 2. If not available, try getState() for cached token
      // 3. If still not available, fail gracefully
      const token = getTokenFromUrl();
      
      if (token) {
        console.log('✅ Using URL token');
      } else {
        // Try to get cached state from App Bridge
        console.log('🔍 Checking App Bridge state for cached token...');
        try {
          const state = app.getState();
          console.log('📊 App Bridge state:', state);
          // The state might have a session token cached
          // This is a guess - let's see what's in there
        } catch (e) {
          console.warn('⚠️ Could not get App Bridge state:', e);
        }
        
        throw new Error('No session token available - please refresh the page');
      }

      const response = await window.fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
          ...options.headers,
        },
      });

      console.log(`✅ Response received: ${response.status}`);

      if (returnRawResponse) {
        return response;
      }

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({
          detail: `Request failed with status ${response.status}`
        }));
        throw new Error(errorData.detail || errorData.message || 'Request failed');
      }

      const data = await response.json();
      console.log('✅ Data received successfully');
      return data;
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