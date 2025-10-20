import React from 'react'
import { isEmbeddedShopify, initAppBridge } from '@/lib/shopify'

// Add this type declaration
declare global {
  interface Window {
    __SHOPIFY_APP__?: ReturnType<typeof initAppBridge>
  }
}

export default function ShopifyEmbedGate({ children }: { children: React.ReactNode }) {
  const [ready, setReady] = React.useState(false)
  
  React.useEffect(() => {
    if (isEmbeddedShopify()) {
      const app = initAppBridge()
      window.__SHOPIFY_APP__ = app
    }
    setReady(true)
  }, [])
  
  if (!ready) return null
  return <>{children}</>
}