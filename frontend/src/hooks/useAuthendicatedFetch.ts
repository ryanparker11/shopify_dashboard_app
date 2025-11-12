// useAuthenticatedFetch.ts
import { getSessionToken } from '@shopify/app-bridge/utilities';
import { useAppBridge } from '@/hooks/useAppBridge';

export function useAuthenticatedFetch() {
  const app = useAppBridge();

  const authenticatedFetch = async (url: string, options: RequestInit = {}) => {
    if (!app) {
      // If no App Bridge, make regular fetch (for non-embedded context)
      return fetch(url, options);
    }

    try {
      // Get fresh session token for this request
      const token = await getSessionToken(app);
      console.log('Session token retrieved for request');

      return fetch(url, {
        ...options,
        headers: {
          ...options.headers,
          'Authorization': `Bearer ${token}`,
        },
      });
    } catch (error) {
      console.error('Failed to get session token:', error);
      throw error;
    }
  };

  return authenticatedFetch;
}