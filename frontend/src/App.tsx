// App.tsx
import {
  AppProvider,
  Card,
  Banner,
  ProgressBar,
  Text,
  BlockStack,
  Layout,
  Button,
  InlineStack,
} from '@shopify/polaris';
import enTranslations from '@shopify/polaris/locales/en.json';
import '@shopify/polaris/build/esm/styles.css';

import ShopifyEmbedGate from './components/ShopifyEmbedGate';
import { AppBridgeProvider } from './components/AppBridgeProvider';
import { COGSManagement } from './components/COGSManagement';
import { useEffect, useRef, useState } from 'react';
import Plot from 'react-plotly.js';
import { useAuthenticatedFetch } from './lib/api';

declare global {
  interface Window {
    app?: {
      idToken: () => Promise<string>;
    };
  }
}

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

interface OrdersSummary {
  total_orders: number;
}

interface ChartsResponse {
  charts: ChartData[];
}

function AppContent() {
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const [totalOrders, setTotalOrders] = useState<number | null>(null);
  const [shop, setShop] = useState<string | null>(null);

  const [showBanner, setShowBanner] = useState(false);
  const prevStatusRef = useRef<SyncStatus['status'] | null>(null);

  // Use the authenticated fetch hook from api.ts
  const authenticatedFetch = useAuthenticatedFetch();

  const API_URL =
    import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

  // --------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------

  
  useEffect(() => {
  if (!shop) return;

  const testSessionToken = async () => {
    console.log('🧪 Testing session token fetch...');
    const start = performance.now();
    
    try {
      if (window.app) {
        const token = await window.app.idToken();
        const elapsed = performance.now() - start;
        console.log('✅ Session token received in', elapsed, 'ms');
        console.log('🎫 Token (first 20 chars):', token.substring(0, 20) + '...');
      } else {
        console.error('❌ window.app not found');
      }
    } catch (error) {
      const elapsed = performance.now() - start;
      console.error('💥 Session token failed after', elapsed, 'ms:', error);
    }
  };

  // Test after a delay
  setTimeout(testSessionToken, 2000);
}, [shop]);

  async function fetchOrdersSummary(shopDomain: string) {
    try {
      const data = await authenticatedFetch<OrdersSummary>(
        `/api/orders/summary?shop_domain=${encodeURIComponent(shopDomain)}`
      );
      setTotalOrders(data.total_orders ?? null);
    } catch (e) {
      console.error('Failed to fetch orders summary:', e);
    }
  }

  const fetchChartData = async (shopName: string) => {
    try {
      console.log('🔍 Fetching charts for shop:', shopName);
      console.log('🔍 Using authenticatedFetch:', typeof authenticatedFetch);
      
      const data = await authenticatedFetch<ChartsResponse>(
        `/api/charts/${encodeURIComponent(shopName)}`
      );

      console.log('✅ Charts response received:', data);
      console.log('✅ Charts loaded successfully:', data.charts?.length || 0, 'charts');
      setChartData(data.charts || []);
    } catch (error) {
      console.error('💥 Failed to fetch chart data:', error);
      if (error instanceof Error) {
      console.error('💥 Error name:', error.name);
      console.error('💥 Error message:', error.message);
      console.error('💥 Error stack:', error.stack);
    }
    }
  };

  const resolveExportUrl = (exportUrl?: string) => {
    if (!exportUrl) return null;

    if (exportUrl.startsWith('/charts'))
      return `/api${exportUrl}`;

    if (exportUrl.startsWith('/api/')) return exportUrl;

    if (exportUrl.startsWith('http')) return exportUrl;

    return `/api/${exportUrl.replace(/^\/+/, '')}`;
  };

  const downloadChart = async (chart: ChartData) => {
    try {
      const url = resolveExportUrl(chart.export_url);
      if (!url) {
        console.error('No export URL for chart:', chart);
        return;
      }

      console.log('Attempting to download from:', url);
      
      // Get raw response for blob download (third parameter = true)
      const response = await authenticatedFetch(url, {}, true);
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error('Download failed with error:', errorText);
        throw new Error(`Export failed: ${response.status} - ${errorText}`);
      }

      const blob = await response.blob();
      console.log('Blob created, size:', blob.size, 'type:', blob.type);

      const filename = `${chart.key || 'chart'}_${new Date()
        .toISOString()
        .slice(0, 10)}.xlsx`.replace(/\s+/g, '_');

      const objectUrl = window.URL.createObjectURL(blob);

      const a = document.createElement('a');
      a.href = objectUrl;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();

      window.URL.revokeObjectURL(objectUrl);
      
      console.log('Download completed successfully');
    } catch (err) {
      console.error('Download error:', err);
      alert(`Failed to download chart data: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }
  };

  // --------------------------------------------------------------------
  // Effect: initial sync-status polling + initial data fetches
  // --------------------------------------------------------------------

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const shopParam = params.get('shop');

    if (!shopParam) {
      setIsLoading(false);
      return;
    }

    setShop(shopParam);

    let isCancelled = false;

    // Wait for App Bridge to be fully initialized
    const waitForAppBridge = () => {
      return new Promise<void>((resolve) => {
        const checkInterval = setInterval(() => {
          if (window.__SHOPIFY_APP__) {
            clearInterval(checkInterval);
            console.log('✅ App Bridge confirmed ready');
            resolve();
          }
        }, 100);

        // Timeout after 10 seconds
        setTimeout(() => {
          clearInterval(checkInterval);
          console.warn('⚠️ App Bridge timeout - proceeding anyway');
          resolve();
        }, 10000);
      });
    };

    const checkSyncStatus = async () => {
      if (isCancelled) return;

      try {
        console.log('📡 Calling sync-status endpoint');
        console.log('📡 Shop param:', shopParam);
        console.log('📡 API URL:', API_URL);
      
        const response = await fetch(
          `${API_URL}/auth/sync-status/${encodeURIComponent(shopParam)}`,
          {
            method: 'GET',
            headers: {
              'Content-Type': 'application/json',
            },
          }
        );

        if (!response.ok) {
          throw new Error(`Sync status check failed: ${response.status}`);
        }

        const data: SyncStatus = await response.json();
        console.log('✅ Sync-status response:', data);
      
        if (isCancelled) return;
      
        setSyncStatus(data);
        setIsLoading(false);

        const was = prevStatusRef.current;
        const now = data.status;

        if (now === 'pending' || now === 'in_progress') {
          setShowBanner(true);
        } else if (
          (was === 'pending' || was === 'in_progress') &&
          (now === 'completed' || now === 'failed')
        ) {
          setShowBanner(true);
        } else {
          setShowBanner(false);
        }

        prevStatusRef.current = now;

        // Only fetch charts/orders when transitioning to completed AND App Bridge is ready
        if (data.status === 'completed' && was !== 'completed') {
          console.log('✅ Sync completed - waiting for App Bridge before fetching data');
        
          // Wait for App Bridge to be ready before making authenticated requests
          await waitForAppBridge();
        
          if (!isCancelled) {
            console.log('✅ App Bridge ready - fetching charts and orders');
            try {
              await Promise.all([
                fetchChartData(shopParam),
                fetchOrdersSummary(shopParam)
              ]);
              console.log('✅ All data fetched successfully');
            } catch (error) {
              console.error('💥 Error fetching data:', error);
            }
          }
        }

        // Continue polling if still in progress
        if (data.status === 'pending' || data.status === 'in_progress') {
          setTimeout(checkSyncStatus, 3000);
        }
      } catch (error) {
        console.error('💥 Failed to fetch sync status:', error);
        if (!isCancelled) {
          setIsLoading(false);
        }
      }
    };

    // Wait for App Bridge, then start checking sync status
    waitForAppBridge().then(() => {
      if (!isCancelled) {
        checkSyncStatus();
      }
    });

    // Cleanup
    return () => {
      isCancelled = true;
    };
  }, []);

  // --------------------------------------------------------------------
  // Effect: optimized order-count refresh (focus-based polling)
  // --------------------------------------------------------------------

  useEffect(() => {
    if (!shop) return;

    const refresh = () => fetchOrdersSummary(shop);

    // Initial fetch
    refresh();

    const onFocus = () => refresh();
    window.addEventListener('focus', onFocus);

    const intervalId = setInterval(refresh, 5 * 60 * 1000);

    return () => {
      window.removeEventListener('focus', onFocus);
      clearInterval(intervalId);
    };
  }, [shop]); // ONLY shop in dependencies, not authenticatedFetch

  // --------------------------------------------------------------------
  // Rendering helpers
  // --------------------------------------------------------------------

  const renderSyncBanner = () => {
    if (isLoading || !syncStatus || !showBanner) return null;

    switch (syncStatus.status) {
      case 'pending':
        return (
          <Banner tone="info">
            <BlockStack gap="200">
              <Text as="p">
                Your order history is being prepared for import...
              </Text>
            </BlockStack>
          </Banner>
        );

      case 'in_progress': {
        const progress =
          syncStatus.orders_synced > 0
            ? Math.min((syncStatus.orders_synced / 10000) * 100, 95)
            : 0;

        return (
          <Banner tone="info">
            <BlockStack gap="300">
              <Text as="p" variant="bodyMd">
                Importing order history...{' '}
                {syncStatus.orders_synced.toLocaleString()} orders synced so far
              </Text>

              <ProgressBar progress={progress} size="small" />

              <Text as="p" variant="bodySm" tone="subdued">
                This may take a few minutes depending on your store size. You
                can use the app while this completes.
              </Text>
            </BlockStack>
          </Banner>
        );
      }

      case 'completed':
        return (
          <Banner tone="success" onDismiss={() => setShowBanner(false)}>
            <Text as="p">
              ✅ Successfully imported{' '}
              {syncStatus.orders_synced.toLocaleString()} orders from your store
              history!
            </Text>
          </Banner>
        );

      case 'failed':
        return (
          <Banner tone="critical" onDismiss={() => setShowBanner(false)}>
            <BlockStack gap="200">
              <Text as="p" fontWeight="semibold">
                Failed to import order history
              </Text>

              {syncStatus.error && (
                <Text as="p" variant="bodySm">
                  Error: {syncStatus.error}
                </Text>
              )}

              <Text as="p" variant="bodySm">
                Don't worry — new orders will still be tracked. Contact support
                if this persists.
              </Text>
            </BlockStack>
          </Banner>
        );

      default:
        return null;
    }
  };

  const renderCharts = () => {
    if (syncStatus?.status !== 'completed' || chartData.length === 0)
      return null;

    return (
      <div style={{ marginTop: '20px' }}>
        <Layout>
          {chartData.map((chart, index) => {
            const titleText =
              typeof chart.layout.title === 'string'
                ? chart.layout.title
                : chart.layout.title?.text || '';

            return (
              <Layout.Section
                key={chart.key || index}
                variant="oneHalf"
              >
                <Card>
                  <BlockStack gap="300">
                    <div style={{ padding: '16px 16px 0 16px' }}>
                      <InlineStack
                        align="space-between"
                        blockAlign="center"
                      >
                        <Text as="h2" variant="headingMd">
                          {titleText}
                        </Text>

                        {chart.export_url && (
                          <Button
                            size="slim"
                            onClick={() => downloadChart(chart)}
                          >
                            Download
                          </Button>
                        )}
                      </InlineStack>
                    </div>

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
      </div>
    );
  };

  // --------------------------------------------------------------------
  // Main return
  // --------------------------------------------------------------------

  return (
    <AppProvider i18n={enTranslations}>
      <div style={{ padding: '20px' }}>
        {renderSyncBanner()}

        <div style={{ marginTop: '30px' }}>
          <Card>
            <BlockStack gap="400">
              <Text as="h1" variant="headingLg">
                Welcome to Lodestar
              </Text>

              {totalOrders !== null && (
                <Text as="p" tone="subdued">
                  Your store has{' '}
                  {totalOrders.toLocaleString()} orders ready to analyze.
                </Text>
              )}
            </BlockStack>
          </Card>

          {/* COGS module */}
          {shop && syncStatus?.status === 'completed' && (
            <div style={{ marginTop: '20px' }}>
              <COGSManagement shopDomain={shop} apiUrl={API_URL} />
            </div>
          )}

          {renderCharts()}
        </div>
      </div>
    </AppProvider>
  );
}

export default function App() {
  return (
    <ShopifyEmbedGate>
      <AppBridgeProvider>
        <AppContent />
      </AppBridgeProvider>
    </ShopifyEmbedGate>
  );
}