import { createApp } from '@shopify/app-bridge'

export function isEmbeddedShopify(): boolean {
  const inIframe = window.self !== window.top
  const url = new URL(window.location.href)
  return inIframe && !!url.searchParams.get('host') && !!url.searchParams.get('shop')
}

export function initAppBridge() {
  const url = new URL(window.location.href)
  const host = url.searchParams.get('host')
  const shop = url.searchParams.get('shop')
  const apiKey = import.meta.env.VITE_SHOPIFY_API_KEY
  
  if (!apiKey || !host || !shop) return null
  
  return createApp({ apiKey, host, forceRedirect: true })
}