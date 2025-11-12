// AppBridgeProvider.tsx
import { useEffect, useState } from 'react';
import createApp from '@shopify/app-bridge';
import type { ClientApplication } from '@shopify/app-bridge';
import { AppBridgeContext } from '@/hooks/AppBridgeContext';

export function AppBridgeProvider({ children }: { children: React.ReactNode }) {
  const [app, setApp] = useState<ClientApplication | null>(null);

  useEffect(() => {
    console.log('🏗️ AppBridgeProvider mounting');
    
    // Check if we have the Shopify embedded app configuration
    if (!window.__SHOPIFY_APP__) {
      console.error('❌ window.__SHOPIFY_APP__ not found - not in embedded context');
      return;
    }
    
    console.log('📋 Shopify config:', window.__SHOPIFY_APP__);
    
    try {
      const appInstance = createApp({
        apiKey: window.__SHOPIFY_APP__.apiKey,
        host: window.__SHOPIFY_APP__.host,
        forceRedirect: true, // Important for embedded apps
      });
      
      setApp(appInstance);
      console.log('✅ App Bridge initialized');
      console.log('App instance:', appInstance);
    } catch (error) {
      console.error('❌ Failed to create App Bridge:', error);
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