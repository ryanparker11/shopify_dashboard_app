// frontend/src/components/AppBridgeProvider.tsx
import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { createApp } from '@shopify/app-bridge';
import type { ClientApplication, AppBridgeState } from '@shopify/app-bridge';
import { Banner } from '@shopify/polaris';
import { AppBridgeContext } from '../hooks/AppBridgeContext';

interface AppBridgeProviderProps {
  children: ReactNode;
}

export function AppBridgeProvider({ children }: AppBridgeProviderProps) {
  const [app, setApp] = useState<ClientApplication<AppBridgeState> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      const host = params.get('host');
      const apiKey = import.meta.env.VITE_SHOPIFY_API_KEY;

      // Validate required configuration
      if (!apiKey) {
        setError('Missing Shopify API key configuration. Please check your environment variables.');
        setLoading(false);
        return;
      }

      if (!host) {
        setError('Missing required configuration. Please access this app through your Shopify admin.');
        setLoading(false);
        return;
      }

      // Create App Bridge instance
      const appInstance = createApp({
        apiKey: apiKey,
        host: host,
        forceRedirect: true,
      });

      console.log('✅ App Bridge initialized successfully');
      setApp(appInstance);
      setError(null);
    } catch (err) {
      console.error('❌ Failed to initialize App Bridge:', err);
      setError('Failed to initialize App Bridge. Please try refreshing the page.');
    } finally {
      setLoading(false);
    }
  }, []);

  // Show loading state
  if (loading) {
    return (
      <div style={{ padding: '20px', textAlign: 'center' }}>
        <p>Loading Shopify App Bridge...</p>
      </div>
    );
  }

  // Show error banner if initialization failed
  if (error) {
    return (
      <div style={{ padding: '20px' }}>
        <Banner tone="critical" title="Configuration Error">
          <p>{error}</p>
        </Banner>
      </div>
    );
  }

  // Provide App Bridge instance to all children
  return (
    <AppBridgeContext.Provider value={app}>
      {children}
    </AppBridgeContext.Provider>
  );
}
