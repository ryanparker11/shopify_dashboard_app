// frontend/src/components/AppBridgeProvider.tsx
import { useEffect, useState, type PropsWithChildren } from 'react';
import createApp from '@shopify/app-bridge';
import type { ClientApplication } from '@shopify/app-bridge';
import { Banner } from '@shopify/polaris';
import { AppBridgeContext } from '../hooks/AppBridgeContext';

export function AppBridgeProvider({ children }: PropsWithChildren) {
  const [app, setApp] = useState<ClientApplication | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      const host = params.get('host');
      const apiKey = import.meta.env.VITE_SHOPIFY_API_KEY as string | undefined;

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

      const appInstance = createApp({ apiKey, host, forceRedirect: true });
      setApp(appInstance);
      setError(null);
    } catch {
      setError('Failed to initialize App Bridge. Please try refreshing the page.');
    } finally {
      setLoading(false);
    }
  }, []);

  if (loading) {
    return <div style={{ padding: 20, textAlign: 'center' }}>Loading Shopify App Bridge...</div>;
  }

  if (error) {
    return (
      <div style={{ padding: 20 }}>
        <Banner tone="critical" title="Configuration Error">
          <p>{error}</p>
        </Banner>
      </div>
    );
  }

  return <AppBridgeContext.Provider value={app}>{children}</AppBridgeContext.Provider>;
}
