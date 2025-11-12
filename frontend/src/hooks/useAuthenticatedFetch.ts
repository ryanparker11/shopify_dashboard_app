// useAuthenticatedFetch.ts - Using App Bridge's built-in fetch
import { useAppBridge } from '@/hooks/useAppBridge';
import { useCallback, useEffect, useRef } from 'react';
import { authenticatedFetch } from '@shopify/app-bridge/utilities';

export function useAuthenticatedFetch() {
  const app = useAppBridge();
  const appRef = useRef(app);

  useEffect(() => {
    appRef.current = app;
  }, [app]);

  const makeAuthenticatedRequest = useCallback(async (url: string, options: RequestInit = {}) => {
    console.log('🔐 Making authenticated request to:', url);
    
    const currentApp = appRef.current;
    
    if (!currentApp) {
      console.warn('⚠️  No App Bridge - falling back to regular fetch');
      return fetch(url, { credentials: 'include', ...options });
    }

    try {
      // Use App Bridge's authenticatedFetch utility
      // This handles token fetching automatically
      const fetchFunction = authenticatedFetch(currentApp);
      
      console.log('🔑 Using App Bridge authenticated fetch...');
      
      const response = await fetchFunction(url, options);
      
      console.log('✅ Request completed:', response.status);
      
      return response;
    } catch (error) {
      console.error('❌ Authenticated fetch error:', error);
      console.warn('⚠️  Falling back to regular fetch');
      return fetch(url, { credentials: 'include', ...options });
    }
  }, []);

  return makeAuthenticatedRequest;
}