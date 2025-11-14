// frontend/src/lib/api.ts

const API_BASE = import.meta.env.VITE_API_BASE;

// The CDN exposes createApp differently
const params = new URLSearchParams(window.location.search);
const host = params.get('host');
const apiKey = import.meta.env.VITE_SHOPIFY_API_KEY;

if (!host || !apiKey) {
  throw new Error('Missing Shopify parameters');
}

// CDN script exposes it as window.AppBridge
interface AppBridgeInstance {
  subscribe: (action: string, callback: (payload: AppBridgeAction) => void) => () => void;
  dispatch: (action: AppBridgeAction) => void;
}

declare global {
  interface Window {
    AppBridge?: {
      createApp: (config: {
        apiKey: string;
        host: string;
        forceRedirect?: boolean;
      }) => AppBridgeInstance;
    };
  }
}

// Wait for CDN to load
const getAppBridge = (): AppBridgeInstance => {
  if (window.AppBridge?.createApp) {
    return window.AppBridge.createApp({ apiKey, host, forceRedirect: true });
  }
  throw new Error('App Bridge CDN not loaded');
};

let app: AppBridgeInstance;
try {
  app = getAppBridge();
  console.log('✅ App Bridge from CDN:', app);
} catch (error) {
  console.error('❌ Failed to initialize App Bridge:', error);
  throw error;
}

interface AppBridgeAction {
  type: string;
  payload?: {
    sessionToken?: string;
    [key: string]: unknown;
  };
}

async function getToken(): Promise<string> {
  console.log('🔐 Requesting session token from CDN App Bridge...');
  
  return new Promise((resolve, reject) => {
    const unsubscribe = app.subscribe('APP::SESSION_TOKEN::RESPOND', (action: AppBridgeAction) => {
      console.log('✅ Got token response');
      unsubscribe();
      if (action.payload?.sessionToken) {
        resolve(action.payload.sessionToken);
      } else {
        reject(new Error('No token in response'));
      }
    });
    
    app.dispatch({ type: 'APP::SESSION_TOKEN::REQUEST' });
    
    setTimeout(() => {
      unsubscribe();
      reject(new Error('Token timeout'));
    }, 5000);
  });
}

export async function authenticatedFetch<T = unknown>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;
  
  console.log('🚀 Request to:', endpoint);
  
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