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
  const [totalOrders, setTotalOrders] = useState<number | null>(null);  // NEW: live count
  const [shop, setShop] = useState<string | null>(null);               // NEW: store shop
  const API_URL = import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

  // --- Helpers ---
  async function fetchOrdersSummary(shopDomain: string) {
    try {
      const res = await fetch(`${API_URL}/api/orders/summary?shop_domain=${encodeURIComponent(shopDomain)}`);
      if (!res.ok) return;
      const data = await res.json();
      setTotalOrders(data.total_orders ?? null);
    } catch (e) {
      console.error('Failed to fetch orders summary:', e);
    }
  }

  const fetchChartData = async (shopName: string) => {
    try {
      const response = await fetch(`${API_URL}/api/charts/${shopName}`);
      const data = await response.json();
      setChartData(data.charts || []);
    } catch (error) {
      console.error('Failed to fetch chart data:', error);
    }
  };

  // --- Effect: initial sync-status polling + initial data fetches ---
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const shopParam = params.get('shop');

    if (!shopParam) {
      setIsLoading(false);
      return;
    }
    setShop(shopParam);

    const checkSyncStatus = async () => {
      try {
        const response = await fetch(`${API_URL}/auth/sync-status/${shopParam}`);
        const data: SyncStatus = await response.json();

        setSyncStatus(data);
        setIsLoading(false);

        // When sync completes, load charts and live count
        if (data.status === 'completed') {
          fetchChartData(shopParam);
          fetchOrdersSummary(shopParam);
        }

        // Keep polling while pending/in_progress
        if (data.status === 'pending' || data.status === 'in_progress') {
          setTimeout(checkSyncStatus, 3000);
        }
      } catch (error) {
        console.error('Failed to fetch sync status:', error);
        setIsLoading(false);
      }
    };

    checkSyncStatus();
    // run once on mount
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // --- Effect: keep live order count fresh (interval + window focus) ---
  useEffect(() => {
    if (!shop) return;

    const refresh = () => fetchOrdersSummary(shop);

    // initial tick
    refresh();

    const id = setInterval(refresh, 15000);
    const onFocus = () => refresh();

    window.addEventListener('focus', onFocus);
    return () => {
      clearInterval(id);
      window.removeEventListener('focus', onFocus);
    };
  }, [shop]);

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
              {syncStatus.error && <Text as="p" variant="bodySm">Error: {syncStatus.error}</Text>}
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
    if (syncStatus?.status !== 'completed' || chartData.length === 0) return null;

    return (
      <div style={{ marginTop: '20px' }}>
        <Layout>
          {chartData.map((chart, index) => {
            const titleText = typeof chart.layout.title === 'string'
              ? chart.layout.title
              : chart.layout.title?.text || '';

            return (
              <Layout.Section key={index} variant="oneHalf">
                <Card>
                  <BlockStack gap="400">
                    <div style={{ padding: '16px 16px 0 16px' }}>
                      <Text as="h2" variant="headingMd">{titleText}</Text>
                    </div>
                    <Plot
                      data={chart.data}
                      layout={{
                        ...chart.layout,
                        title: undefined,
                        autosize: true,
                        margin: { t: 20, r: 40, b: 60, l: 60 }
                      }}
                      config={{ responsive: true, displayModeBar: false }}
                      style={{ width: '100%', height: '400px' }}
                      useResizeHandler
                    />
                  </BlockStack>
                </Card>
              </Layout.Section>
            );
          })}
        </Layout>
      </div>
    );
  };

  return (
    <ShopifyEmbedGate>
      <AppProvider i18n={enTranslations}>
        <div style={{ padding: '20px' }}>
          {renderSyncBanner()}

          <div style={{ marginTop: '300px' }}>
            <Card>
              <BlockStack gap="400">
                <Text as="h1" variant="headingLg">
                  Welcome to Your Shopify App
                </Text>

                {/* Use LIVE count from DB instead of the initial sync count */}
                {totalOrders !== null && (
                  <Text as="p" tone="subdued">
                    Your store has {totalOrders.toLocaleString()} orders ready to analyze.
                  </Text>
                )}
              </BlockStack>
            </Card>

            {renderCharts()}
          </div>
        </div>
      </AppProvider>
    </ShopifyEmbedGate>
  );
}