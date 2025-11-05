// App.tsx
import {
  AppProvider,
  Frame,
  Page,
  Card,
  Banner,
  ProgressBar,
  Text,
  BlockStack,
  Layout,
  Button,
  InlineStack,
  Box,
} from '@shopify/polaris';
import enTranslations from '@shopify/polaris/locales/en.json';
import '@shopify/polaris/build/esm/styles.css';
import ShopifyEmbedGate from './components/ShopifyEmbedGate';
import { COGSManagement } from './components/COGSManagement';
import { useEffect, useState } from 'react';
import Plot from 'react-plotly.js';

interface SyncStatus {
  status: 'pending' | 'in_progress' | 'completed' | 'failed' | 'not_found';
  orders_synced: number;
  completed_at: string | null;
  error: string | null;
}

interface ChartData {
  key?: string;
  data: Plotly.Data[];
  layout: Partial<Plotly.Layout & { title?: string | { text?: string } }>;
  export_url?: string;
}

export default function App() {
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const [totalOrders, setTotalOrders] = useState<number | null>(null);
  const [shop, setShop] = useState<string | null>(null);

  const API_URL = import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

  async function fetchOrdersSummary(shopDomain: string) {
    try {
      const res = await fetch(
        `${API_URL}/api/orders/summary?shop_domain=${encodeURIComponent(shopDomain)}`,
        { credentials: 'include' },
      );
      if (!res.ok) return;
      const data = await res.json();
      setTotalOrders(data.total_orders ?? null);
    } catch (e) {
      console.error('Failed to fetch orders summary:', e);
    }
  }

  const fetchChartData = async (shopName: string) => {
    try {
      const response = await fetch(`${API_URL}/api/charts/${encodeURIComponent(shopName)}`, {
        credentials: 'include',
      });
      if (!response.ok) throw new Error(`Charts fetch failed: ${response.status}`);
      const data = await response.json();
      setChartData(data.charts || []);
    } catch (error) {
      console.error('Failed to fetch chart data:', error);
    }
  };

  const resolveExportUrl = (exportUrl?: string) => {
    if (!exportUrl) return null;
    if (exportUrl.startsWith('/charts')) return `${API_URL}/api${exportUrl}`;
    if (exportUrl.startsWith('/api/')) return `${API_URL}${exportUrl}`;
    if (exportUrl.startsWith('http')) return exportUrl;
    return `${API_URL}/api/${exportUrl.replace(/^\/+/, '')}`;
  };

  const downloadChart = async (chart: ChartData) => {
    try {
      const url = resolveExportUrl(chart.export_url);
      if (!url) return;
      const res = await fetch(url, { credentials: 'include' });
      if (!res.ok) throw new Error(`Export failed: ${res.status}`);
      const blob = await res.blob();
      const filename = `${chart.key || 'chart'}_${new Date().toISOString().slice(0, 10)}.xlsx`.replace(
        /\s+/g,
        '_',
      );
      const objectUrl = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = objectUrl;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(objectUrl);
    } catch (err) {
      console.error('Download error:', err);
      alert('Failed to download chart data. Please try again.');
    }
  };

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
        const response = await fetch(`${API_URL}/auth/sync-status/${encodeURIComponent(shopParam)}`, {
          credentials: 'include',
        });
        const data: SyncStatus = await response.json();
        setSyncStatus(data);
        setIsLoading(false);

        if (data.status === 'completed') {
          fetchChartData(shopParam);
          fetchOrdersSummary(shopParam);
        }

        if (data.status === 'pending' || data.status === 'in_progress') {
          setTimeout(checkSyncStatus, 3000);
        }
      } catch (error) {
        console.error('Failed to fetch sync status:', error);
        setIsLoading(false);
      }
    };

    checkSyncStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!shop) return;
    const refresh = () => fetchOrdersSummary(shop);
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
        const progress =
          syncStatus.orders_synced > 0 ? Math.min((syncStatus.orders_synced / 10000) * 100, 95) : 0;
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
              <Text as="p" fontWeight="semibold">
                Failed to import order history
              </Text>
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
      <Box paddingBlockStart="400">
        <Layout>
          {chartData.map((chart, index) => {
            const titleText =
              typeof chart.layout.title === 'string'
                ? chart.layout.title
                : chart.layout.title?.text || '';
            return (
              <Layout.Section key={chart.key || index} variant="oneHalf">
                <Card>
                  <BlockStack gap="300">
                    <Box paddingInline="400" paddingBlockStart="300">
                      <InlineStack align="space-between" blockAlign="center">
                        <Text as="h2" variant="headingMd">
                          {titleText}
                        </Text>
                        {chart.export_url && (
                          <Button size="slim" onClick={() => downloadChart(chart)}>
                            Download
                          </Button>
                        )}
                      </InlineStack>
                    </Box>

                    <Plot
                      data={chart.data}
                      layout={{
                        ...chart.layout,
                        title: undefined,
                        autosize: true,
                        margin: { t: 20, r: 40, b: 60, l: 60 },
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
      </Box>
    );
  };

  return (
    <ShopifyEmbedGate>
      <AppProvider i18n={enTranslations}>
        <Frame>
          <Page title="Lodestar Analytics">
            {/* Center content horizontally with plain div wrapper */}
            <div style={{ maxWidth: 1600, margin: '0 auto' }}>
              <Box minHeight="100dvh" padding="400">
                <BlockStack gap="400">
                  {renderSyncBanner()}

                  <Card>
                    <BlockStack gap="400">
                      <Text as="h1" variant="headingLg">
                        Welcome to Your Shopify App
                      </Text>
                      {totalOrders !== null && (
                        <Text as="p" tone="subdued">
                          Your store has {totalOrders.toLocaleString()} orders ready to analyze.
                        </Text>
                      )}
                    </BlockStack>
                  </Card>

                  {shop && syncStatus?.status === 'completed' && (
                    <Card>
                      <Box padding="400">
                        <COGSManagement shopDomain={shop} apiUrl={API_URL} />
                      </Box>
                    </Card>
                  )}

                  {renderCharts()}
                </BlockStack>
              </Box>
            </div>
          </Page>
        </Frame>
      </AppProvider>
    </ShopifyEmbedGate>
  );
}
