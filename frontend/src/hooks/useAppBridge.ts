// useAppBridge.ts
import { useContext } from 'react';
import type { ClientApplication } from '@shopify/app-bridge';
import { AppBridgeContext } from '@/hooks/AppBridgeContext';

export function useAppBridge(): ClientApplication | null {
  const app = useContext(AppBridgeContext);
  if (!app) {
    console.warn('App Bridge not initialized - make sure you are in embedded Shopify context');
  }
  return app;
}