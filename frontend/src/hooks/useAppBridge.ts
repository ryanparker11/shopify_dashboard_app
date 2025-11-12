// frontend/src/hooks/useAppBridge.ts
import { useContext } from 'react';
import type { ClientApplication } from '@shopify/app-bridge';
import { AppBridgeContext } from '../hooks/AppBridgeContext';

export const useAppBridge = (): ClientApplication => {
  const app = useContext(AppBridgeContext);
  if (!app) {
    throw new Error('useAppBridge must be used within AppBridgeProvider');
  }
  return app;
};
