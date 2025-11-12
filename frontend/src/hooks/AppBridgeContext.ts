// AppBridgeContext.ts
import { createContext } from 'react';
import type { ClientApplication } from '@shopify/app-bridge';

export const AppBridgeContext = createContext<ClientApplication | null>(null);