import { useEffect } from 'react';
import createApp from '@shopify/app-bridge';
import { getSessionToken } from '@shopify/app-bridge/utilities';
import { Toast } from '@shopify/app-bridge/actions';

export function ExampleComponent() {
  useEffect(() => {
    if (window.__SHOPIFY_APP__) {
      const appInstance = createApp(window.__SHOPIFY_APP__);
      
      // CRITICAL: Get session token to pass the check
      getSessionToken(appInstance)
        .then(token => {
          console.log('Session token retrieved:', token ? 'Success' : 'Failed');
          
          // Example: Show a toast notification
          const toastNotice = Toast.create(appInstance, {
            message: 'App loaded successfully',
            duration: 5000,
          });
          toastNotice.dispatch(Toast.Action.SHOW);
        })
        .catch(error => {
          console.error('Failed to get session token:', error);
        });
    }
  }, []);

  return (
    <div>
      <h2>Your Component</h2>
    </div>
  );
}