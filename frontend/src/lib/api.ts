// frontend/src/lib/api.ts
import { useAppBridge } from '../hooks/useAppBridge';

const API_BASE = import.meta.env.VITE_API_BASE;

// Define the action type
interface AppBridgeAction {
  type: string;
  payload?: {
    sessionToken?: string;
    [key: string]: unknown;
  };
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
      console.log('🔐 App Bridge instance:', app);
      console.log('🔐 App Bridge methods:', Object.keys(app));
      
      console.log('🔐 Attempting token fetch...');
      
      const tokenPromise = new Promise<string>((resolve, reject) => {
        console.log('🔐 Setting up token request...');
        
        const unsubscribe = app.subscribe('APP::SESSION_TOKEN::RESPOND', (action: AppBridgeAction) => {
          console.log('📨 Received action from App Bridge:', action);
          
          if (action.type === 'APP::SESSION_TOKEN::RESPOND') {
            console.log('✅ Got session token response!');
            unsubscribe();
            if (action.payload?.sessionToken) {
              resolve(action.payload.sessionToken);
            } else {
              reject(new Error('No session token in response'));
            }
          }
        });
        
        console.log('📤 Dispatching session token request...');
        app.dispatch({ type: 'APP::SESSION_TOKEN::REQUEST' });
        
        setTimeout(() => {
          console.log('❌ Token request timed out');
          unsubscribe();
          reject(new Error('Token request timeout'));
        }, 5000);
      });

      const token = await tokenPromise;
      console.log('✅ Token received:', token.substring(0, 50) + '...');

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