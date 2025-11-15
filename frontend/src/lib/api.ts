// frontend/src/lib/api.ts
import { createApp } from '@shopify/app-bridge';
import { getSessionToken } from '@shopify/app-bridge/utilities';

const API_BASE = import.meta.env.VITE_API_BASE;

// Initialize App Bridge with npm package
const params = new URLSearchParams(window.location.search);
const host = params.get('host');
const apiKey = import.meta.env.VITE_SHOPIFY_API_KEY;

let app: ReturnType<typeof createApp> | null = null;

if (host && apiKey) {
  app = createApp({
    apiKey,
    host,
    forceRedirect: true,
  });
  console.log('✅ App Bridge initialized via npm');
}

async function getToken(): Promise<string> {
  if (!app) {
    throw new Error('App Bridge not initialized');
  }
  
  try {
    const token = await getSessionToken(app);
    return token;
  } catch (error) {
    console.error('Failed to get session token:', error);
    throw error;
  }
}

export async function getSessionTokenForApp(): Promise<string> {
  return getToken();
}

export async function authenticatedFetch<T = unknown>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;
  
  const token = await getToken();
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${token}`,
      ...options.headers,
    },
  });

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }

  return response.json();
}

export const api = {
  get: <T = unknown>(endpoint: string) => 
    authenticatedFetch<T>(endpoint, { method: 'GET' }),
  post: <T = unknown>(endpoint: string, data?: unknown) => 
    authenticatedFetch<T>(endpoint, { method: 'POST', body: JSON.stringify(data) }),
  put: <T = unknown>(endpoint: string, data?: unknown) => 
    authenticatedFetch<T>(endpoint, { method: 'PUT', body: JSON.stringify(data) }),
  delete: <T = unknown>(endpoint: string) => 
    authenticatedFetch<T>(endpoint, { method: 'DELETE' }),
};