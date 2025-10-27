// App.tsx
import { AppProvider, Card, Banner, ProgressBar, Text, BlockStack, Layout } from '@shopify/polaris';
import enTranslations from '@shopify/polaris/locales/en.json';
import '@shopify/polaris/build/esm/styles.css';
import ShopifyEmbedGate from './components/ShopifyEmbedGate';
import { useEffect, useState } from 'react';
import Plot from 'react-plotly.js';

interface SyncStatus {
  status: 'pending' | 'in_progress' | 'completed' | 'failed' | 'not_found';
  orders_synced: number;
  completed_at: string | null;
  error: string | null;
}

interface ChartData {
  data: Plotly.Data[];
  layout: Partial<Plotly.Layout>;
}

export default function App() {
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const API_URL = import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';
  //const [shop, setShop] = useState<string | null>(null);

  useEffect(() => {
    // Get shop from URL params (Shopify passes ?shop=...)
    const params = new URLSearchParams(window.location.search);
    const shopParam = params.get('shop');
    //setShop(shopParam);

    if (!shopParam) {
      setIsLoading(false);
      return;
    }

    const checkSyncStatus = async () => {
      try {
        const response = await fetch(`${API_URL}/auth/sync-status/${shopParam}`);
        const data: SyncStatus = await response.json();
        
        setSyncStatus(data);
        setIsLoading(false);

        // If completed, fetch chart data
        if (data.status === 'completed') {
          fetchChartData(shopParam);
        }

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

  const fetchChartData = async (shopName: string) => {
    try {
      // Fetch your chart data from your backend API
      const response = await fetch(`${API_URL}/charts/${shopName}`);
      const data = await response.json();
      setChartData(data.charts || []);
    } catch (error) {
      console.error('Failed to fetch chart data:', error);
    }
  };

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

  const renderCharts = () => {
    if (syncStatus?.status !== 'completed' || chartData.length === 0) {
      return null;
    }

    return (
      <Layout>
        {chartData.map((chart, index) => (
          <Layout.Section key={index}>
            <Card>
              <Plot
                data={chart.data}
                layout={{
                  ...chart.layout,
                  autosize: true,
                }}
                config={{ responsive: true }}
                style={{ width: '100%', height: '400px' }}
                useResizeHandler={true}
              />
            </Card>
          </Layout.Section>
        ))}
      </Layout>
    );
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
              </BlockStack>
            </Card>

            {/* Display charts when sync is completed */}
            {renderCharts()}
          </div>
        </div>
      </AppProvider>
    </ShopifyEmbedGate>
  );
}