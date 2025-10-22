// shopifyEmbedGate.tsx
import React from 'react';

interface ShopifyConfig {
  apiKey: string;
  host: string;
  forceRedirect: boolean;
}

declare global {
  interface Window {
    __SHOPIFY_APP__?: ShopifyConfig;
  }
}

function isEmbeddedShopify(): boolean {
  const urlParams = new URLSearchParams(window.location.search);
  return urlParams.has('shop') && urlParams.has('host');
}

export default function ShopifyEmbedGate({ children }: { children: React.ReactNode }) {
  const [ready, setReady] = React.useState(false);
  
  React.useEffect(() => {
    if (isEmbeddedShopify()) {
      const urlParams = new URLSearchParams(window.location.search);
      const host = urlParams.get('host');
      
      const appBridgeConfig: ShopifyConfig = {
        apiKey: import.meta.env.VITE_SHOPIFY_API_KEY || '',
        host: host || '',
        forceRedirect: true,
      };
      
      // Store config globally for components that need it
      window.__SHOPIFY_APP__ = appBridgeConfig;
    }
    setReady(true);
  }, []);
  
  if (!ready) return null;
  
  return <>{children}</>;
}