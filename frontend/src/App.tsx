// App.tsx
import { AppProvider, Card, Banner, ProgressBar, Text, BlockStack } from '@shopify/polaris';
import enTranslations from '@shopify/polaris/locales/en.json';
import '@shopify/polaris/build/esm/styles.css';
import ShopifyEmbedGate from './components/ShopifyEmbedGate';
import { useEffect, useState } from 'react';

interface SyncStatus {
  status: 'pending' | 'in_progress' | 'completed' | 'failed' | 'not_found';
  orders_synced: number;
  completed_at: string | null;
  error: string | null;
}

export default function App() {
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    // Get shop from URL params (Shopify passes ?shop=...)
    const params = new URLSearchParams(window.location.search);
    const shop = params.get('shop');

    if (!shop) {
      setIsLoading(false);
      return;
    }

    const checkSyncStatus = async () => {
      try {
        const response = await fetch(`/auth/sync-status/${shop}`);
        const data: SyncStatus = await response.json();
        
        setSyncStatus(data);
        setIsLoading(false);

        // If still syncing, check again in 3 seconds
        if (data.status === 'pending' || data.status === 'in_progress') {
          setTimeout(checkSyncStatus, 3000);
        }
      } catch (error) {
        console.error('Failed to fetch sync status:', error);
        setIsLoading(false);
      }
    };

    checkSyncStatus();
  }, []);

  const renderSyncBanner = () => {
    if (isLoading || !syncStatus) return null;

    switch (syncStatus.status) {
      case 'pending':
        return (
          <Banner tone="info">
            <BlockStack gap="200">
              <Text as="p">Your order history is being prepared for import...</Text>
            </BlockStack>
          </Banner>
        );

      case 'in_progress': {
        const progress = syncStatus.orders_synced > 0 ? Math.min((syncStatus.orders_synced / 10000) * 100, 95) : 0;
        return (
          <Banner tone="info">
            <BlockStack gap="300">
              <Text as="p" variant="bodyMd">
                Importing order history... {syncStatus.orders_synced.toLocaleString()} orders synced so far
              </Text>
              <ProgressBar progress={progress} size="small" />
              <Text as="p" variant="bodySm" tone="subdued">
                This may take a few minutes depending on your store size. You can use the app while this completes.
              </Text>
            </BlockStack>
          </Banner>
        );
      }
      case 'completed':
        return (
          <Banner tone="success" onDismiss={() => setSyncStatus(null)}>
            <Text as="p">
              ✅ Successfully imported {syncStatus.orders_synced.toLocaleString()} orders from your store history!
            </Text>
          </Banner>
        );

      case 'failed':
        return (
          <Banner tone="critical">
            <BlockStack gap="200">
              <Text as="p" fontWeight="semibold">Failed to import order history</Text>
              {syncStatus.error && (
                <Text as="p" variant="bodySm">Error: {syncStatus.error}</Text>
              )}
              <Text as="p" variant="bodySm">
                Don't worry - new orders will still be tracked. Contact support if this persists.
              </Text>
            </BlockStack>
          </Banner>
        );

      default:
        return null;
    }
  };

  return (
    <ShopifyEmbedGate>
      <AppProvider i18n={enTranslations}>
        <div style={{ padding: '20px' }}>
          {renderSyncBanner()}
          
          <div style={{ marginTop: '20px' }}>
            <Card>
              <BlockStack gap="400">
                <Text as="h1" variant="headingLg">
                  Welcome to Your Shopify App
                </Text>
                
                {syncStatus?.status === 'completed' && (
                  <Text as="p" tone="subdued">
                    Your store has {syncStatus.orders_synced.toLocaleString()} orders ready to analyze.
                  </Text>
                )}

                {/* Add your routes/components here */}
              </BlockStack>
            </Card>
          </div>
        </div>
      </AppProvider>
    </ShopifyEmbedGate>
  );
}