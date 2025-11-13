// frontend/src/hooks/useAppBridge.ts
import { useContext } from 'react';
import type { ClientApplication, AppBridgeState } from '@shopify/app-bridge';
import { AppBridgeContext } from '../hooks/AppBridgeContext';

/**
 * Custom hook to access the App Bridge instance
 * Must be used within AppBridgeProvider
 */
export const useAppBridge = (): ClientApplication<AppBridgeState> => {
  const app = useContext(AppBridgeContext);
  if (!app) {
    throw new Error('useAppBridge must be used within AppBridgeProvider');
  }
  return app;
};
