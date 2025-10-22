// ExampleComponent.tsx
import { useEffect } from 'react';
import createApp from '@shopify/app-bridge';
//import { Redirect } from '@shopify/app-bridge/actions';
// ExampleComponent.tsx
import { Toast } from '@shopify/app-bridge/actions';

export function ExampleComponent() {
  useEffect(() => {
    if (window.__SHOPIFY_APP__) {
      const app = createApp(window.__SHOPIFY_APP__);
      
      // Example: Show a toast notification
      const toastNotice = Toast.create(app, {
        message: 'App loaded successfully',
        duration: 5000,
      });
      toastNotice.dispatch(Toast.Action.SHOW);
    }
  }, []);

  return (
    <div>
      <h2>Your Component</h2>
    </div>
  );
}