// AppBridgeProvider.tsx
import { useEffect, useState } from 'react';
import createApp from '@shopify/app-bridge';
import type { ClientApplication } from '@shopify/app-bridge';
import { AppBridgeContext } from '@/hooks/AppBridgeContext';

export function AppBridgeProvider({ children }: { children: React.ReactNode }) {
  const [app, setApp] = useState<ClientApplication | null>(null);

  useEffect(() => {
    console.log('🏗️ AppBridgeProvider mounting');
    if (window.__SHOPIFY_APP__) {
      const appInstance = createApp(window.__SHOPIFY_APP__);
      setApp(appInstance);
      console.log('App Bridge initialized');
      console.log('App instance:', appInstance);
    }
    
    return () => {
      console.log('🔥 AppBridgeProvider unmounting!');
    };
  }, []);
  
  useEffect(() => {
    console.log('📱 App state changed to:', app);
  }, [app]);

  return (
    <AppBridgeContext.Provider value={app}>
      {children}
    </AppBridgeContext.Provider>
  );
}