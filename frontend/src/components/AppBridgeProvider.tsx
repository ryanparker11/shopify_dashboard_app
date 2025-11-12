// AppBridgeProvider.tsx
import { useEffect, useState } from 'react';
import createApp from '@shopify/app-bridge';
import type { ClientApplication } from '@shopify/app-bridge';
import { AppBridgeContext } from '@/hooks/AppBridgeContext';

export function AppBridgeProvider({ children }: { children: React.ReactNode }) {
  const [app, setApp] = useState<ClientApplication | null>(null);
  const [isReady, setIsReady] = useState(false);

  useEffect(() => {
    if (window.__SHOPIFY_APP__) {
      const appInstance = createApp(window.__SHOPIFY_APP__);
      setApp(appInstance);
      console.log('App Bridge initialized');
      
      // Mark as ready after a short delay to ensure everything is set up
      setTimeout(() => {
        setIsReady(true);
        console.log('App Bridge ready for use');
      }, 100);
    } else {
      // If not in Shopify context, mark as ready immediately
      setIsReady(true);
    }
  }, []);

  return (
    <AppBridgeContext.Provider value={app}>
      {isReady ? children : <div>Loading...</div>}
    </AppBridgeContext.Provider>
  );
}
