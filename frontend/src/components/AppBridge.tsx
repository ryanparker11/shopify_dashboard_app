// ExampleComponent.tsx
import { useEffect } from 'react';
import createApp from '@shopify/app-bridge';
//import { Redirect } from '@shopify/app-bridge/actions';

export function ExampleComponent() {
  useEffect(() => {
    if (window.__SHOPIFY_APP__) {
      const _app = createApp(window.__SHOPIFY_APP__);
      // Use app bridge features here
    }
  }, []);

  return (
    <div>
      <h2>Your Component</h2>
    </div>
  );
}