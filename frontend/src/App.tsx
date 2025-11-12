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
import { useEffect, useRef, useState, } from 'react';
import Plot from 'react-plotly.js';
import { useAppBridge } from '@/hooks/useAppBridge';
import { getSessionToken } from '@shopify/app-bridge/utilities';

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
  export_url?: string; // e.g. "/charts/{shop}/export/{key}"
}

function AppContent() {
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const [totalOrders, setTotalOrders] = useState<number | null>(null);
  const [shop, setShop] = useState<string | null>(null);

  // NEW: control the banner independently
  const [showBanner, setShowBanner] = useState(false);
  const prevStatusRef = useRef<SyncStatus['status'] | null>(null);

  // Get App Bridge instance directly
  const app = useAppBridge();
  
  // Store app in a ref so we always have the latest value
  const appRef = useRef(app);
  useEffect(() => {
    appRef.current = app;
    console.log('📝 App ref updated to:', app);
  }, [app]);
  
  // Function that reads from ref at call time
  const authenticatedFetch = async (url: string, options: RequestInit = {}) => {
    const currentApp = appRef.current; // Read current value from ref
    
    console.log('🔐 AUTH FETCH for:', url);
    console.log('🔐 App available:', !!currentApp);
    
    if (!currentApp) {
      console.warn('⚠️  No app - using regular fetch');
      return fetch(url, { credentials: 'include', ...options });
    }

    try {
      console.log('🔑 Getting token...');
      
      // Add timeout to prevent hanging forever
      const tokenPromise = getSessionToken(currentApp);
      const timeoutPromise = new Promise<never>((_, reject) => 
        setTimeout(() => reject(new Error('Token fetch timeout')), 5000)
      );
      
      const token = await Promise.race([tokenPromise, timeoutPromise]);
      console.log('✅ Got token, length:', token?.length);

      return fetch(url, {
        credentials: 'include',
        ...options,
        headers: {
          ...options.headers,
          'Authorization': `Bearer ${token}`,
        },
      });
    } catch (error) {
      console.error('❌ Token error:', error);
      console.warn('⚠️  Falling back to regular fetch');
      // Fallback to regular fetch if token fails
      return fetch(url, { credentials: 'include', ...options });
    }
  };

  const API_URL =
    import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

  // --------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------

  async function fetchOrdersSummary(shopDomain: string) {
    try {
      const res = await authenticatedFetch(
        `${API_URL}/api/orders/summary?shop_domain=${encodeURIComponent(
          shopDomain
        )}`
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
      console.log('🔍 Fetching charts for shop:', shopName);
      console.log('🔍 API_URL:', API_URL);
      console.log('🔍 authenticatedFetch function available:', typeof authenticatedFetch);
      
      const response = await authenticatedFetch(
        `${API_URL}/api/charts/${encodeURIComponent(shopName)}`
      );

      console.log('📊 Chart response status:', response.status);
      console.log('📊 Chart response headers:', Object.fromEntries(response.headers.entries()));

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`❌ Charts fetch failed: ${response.status}`, errorText);
        throw new Error(`Charts fetch failed: ${response.status}`);
      }

      const data = await response.json();
      console.log('✅ Charts loaded successfully:', data.charts?.length || 0, 'charts');
      setChartData(data.charts || []);
    } catch (error) {
      console.error('💥 Failed to fetch chart data:', error);
    }
  };

  const resolveExportUrl = (exportUrl?: string) => {
    if (!exportUrl) return null;

    if (exportUrl.startsWith('/charts'))
      return `${API_URL}/api${exportUrl}`;

    if (exportUrl.startsWith('/api/')) return `${API_URL}${exportUrl}`;

    if (exportUrl.startsWith('http')) return exportUrl;

    return `${API_URL}/api/${exportUrl.replace(/^\/+/, '')}`;
  };

  const downloadChart = async (chart: ChartData) => {
    try {
      const url = resolveExportUrl(chart.export_url);
      if (!url) {
        console.error('No export URL for chart:', chart);
        return;
      }

      console.log('Attempting to download from:', url);
      
      // Now using authenticatedFetch since backend validates session tokens
      const res = await authenticatedFetch(url);
      
      console.log('Download response status:', res.status);
      console.log('Download response headers:', res.headers);
      
      if (!res.ok) {
        const errorText = await res.text();
        console.error('Download failed with error:', errorText);
        throw new Error(`Export failed: ${res.status} - ${errorText}`);
      }

      const blob = await res.blob();
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

    // IMPORTANT: Wait for App Bridge to be ready before making API calls
    // We need a small delay to ensure App Bridge is fully set up
    const initializeData = async () => {
      // Wait a bit for App Bridge to initialize
      await new Promise(resolve => setTimeout(resolve, 500));
      
      console.log('🚀 Starting data initialization after App Bridge delay');
      
      const checkSyncStatus = async () => {
        try {
          console.log('📡 Calling sync-status endpoint');
          
          // Now using authenticatedFetch since backend validates session tokens
          const response = await authenticatedFetch(
            `${API_URL}/auth/sync-status/${encodeURIComponent(shopParam)}`
          );

          console.log('📡 Sync-status response:', response.status);

          const data: SyncStatus = await response.json();
          console.log('📡 Sync-status data:', data);
          
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

          if (data.status === 'completed') {
            console.log('✅ Sync completed - fetching charts and orders');
            fetchChartData(shopParam);
            fetchOrdersSummary(shopParam);
          }

          if (data.status === 'pending' || data.status === 'in_progress') {
            setTimeout(checkSyncStatus, 3000);
          }
        } catch (error) {
          console.error('💥 Failed to fetch sync status:', error);
          setIsLoading(false);
        }
      };

      checkSyncStatus();
    };

    initializeData();
  }, []); // Remove authenticatedFetch dependency since checkSyncStatus uses regular fetch

  // --------------------------------------------------------------------
  // Effect: optimized order-count refresh (focus-based polling)
  // --------------------------------------------------------------------

  useEffect(() => {
    if (!shop) return;

    const refresh = () => fetchOrdersSummary(shop);

    refresh();

    const onFocus = () => refresh();
    window.addEventListener('focus', onFocus);

    const intervalId = setInterval(refresh, 5 * 60 * 1000);

    return () => {
      window.removeEventListener('focus', onFocus);
      clearInterval(intervalId);
    };
  }, [shop]); // authenticatedFetch is stable and doesn't need to be in dependencies

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