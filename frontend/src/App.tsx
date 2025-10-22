// app.tsx
import { AppProvider } from '@shopify/polaris';
import enTranslations from '@shopify/polaris/locales/en.json';
import '@shopify/polaris/build/esm/styles.css';
import ShopifyEmbedGate from './components/ShopifyEmbedGate';

export default function App() {
  return (
    <ShopifyEmbedGate>
      <AppProvider i18n={enTranslations}>
        <div style={{ padding: '20px' }}>
          <h1>Welcome to Your Shopify App</h1>
          {/* Add your routes/components here */}
        </div>
      </AppProvider>
    </ShopifyEmbedGate>
  );
}
