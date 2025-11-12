// AppBridgeProvider.tsx
import { useEffect, useState } from 'react';
import createApp from '@shopify/app-bridge';
import type { ClientApplication } from '@shopify/app-bridge';
import { AppBridgeContext } from '@/hooks/AppBridgeContext';

export function AppBridgeProvider({ children }: { children: React.ReactNode }) {
  const [app, setApp] = useState<ClientApplication | null>(null);

  useEffect(() => {
    if (window.__SHOPIFY_APP__) {
      const appInstance = createApp(window.__SHOPIFY_APP__);
      setApp(appInstance);
      console.log('App Bridge initialized');
    }
  }, []);

  return (
    <AppBridgeContext.Provider value={app}>
      {children}
    </AppBridgeContext.Provider>
  );
}