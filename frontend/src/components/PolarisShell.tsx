import React from 'react'
import { AppProvider, Frame } from '@shopify/polaris'
import en from '@shopify/polaris/locales/en.json'
import { isEmbeddedShopify } from '@/lib/shopify'

export default function PolarisShell({ children }: { children: React.ReactNode }) {
  if (!isEmbeddedShopify()) return <>{children}</>
  
  return (
    <AppProvider i18n={en}>
      <Frame>{children}</Frame>
    </AppProvider>
  )
}