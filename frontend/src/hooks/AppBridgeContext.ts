// AppBridgeContext.ts
import { createContext } from 'react';
import type { ClientApplication, AppBridgeState } from '@shopify/app-bridge';

export const AppBridgeContext = createContext<ClientApplication<AppBridgeState> | null>(null);
