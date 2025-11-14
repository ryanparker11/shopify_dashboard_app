// frontend/src/lib/api.ts

const API_BASE = import.meta.env.VITE_API_BASE;

// Access App Bridge from window (loaded by CDN script)
declare global {
  interface Window {
    shopify: {
      environment: {
        embedded: boolean;
      };
    };
    createApp: (config: {
      apiKey: string;
      host: string;
      forceRedirect?: boolean;
    }) => {
      subscribe: (action: string, callback: (payload: AppBridgeAction) => void) => () => void;
      dispatch: (action: AppBridgeAction) => void;
    };
  }
}

interface AppBridgeAction {
  type: string;
  payload?: {
    sessionToken?: string;
    [key: string]: unknown;
  };
}

const params = new URLSearchParams(window.location.search);
const host = params.get('host');
const apiKey = import.meta.env.VITE_SHOPIFY_API_KEY;

if (!host || !apiKey) {
  throw new Error('Missing Shopify parameters');
}

// Use CDN App Bridge (from window.createApp)
const app = window.createApp({ apiKey, host, forceRedirect: true });

console.log('✅ App Bridge from CDN:', app);

async function getToken(): Promise<string> {
  console.log('🔐 Requesting session token from CDN App Bridge...');
  
  return new Promise((resolve, reject) => {
    // CDN App Bridge uses different API
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