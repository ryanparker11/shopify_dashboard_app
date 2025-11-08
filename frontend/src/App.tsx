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
import { COGSManagement } from './components/COGSManagement';
import { useEffect, useRef, useState } from 'react';
import Plot from 'react-plotly.js';

// === BILLING ADDITIONS START: imports ========================================
import BillingButton from './components/BillingButton';
import ProLock from './components/ProLock';
// === BILLING ADDITIONS END: imports ==========================================

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

export default function App() {
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const [totalOrders, setTotalOrders] = useState<number | null>(null);
  const [shop, setShop] = useState<string | null>(null);

  // NEW: control the banner independently
  const [showBanner, setShowBanner] = useState(false);
  const prevStatusRef = useRef<SyncStatus['status'] | null>(null);

  const API_URL = import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

  // === BILLING ADDITIONS START: state for Pro gating ==========================
  const [isPro, setIsPro] = useState<boolean>(false);
  // === BILLING ADDITIONS END: state for Pro gating ============================

  // --- Helpers ---
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

  // Resolve backend-provided export_url into a fully-qualified URL
  const resolveExportUrl = (exportUrl?: string) => {
    if (!exportUrl) return null;
    if (exportUrl.startsWith('/charts')) return `${API_URL}/api${exportUrl}`;
    if (exportUrl.startsWith('/api/')) return `${API_URL}${exportUrl}`;
    if (exportUrl.startsWith('http')) return exportUrl;
    return `${API_URL}/api/${exportUrl.replace(/^\/+/, '')}`;
  };

  // Download a single chart's dataset as Excel
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
        const response = await fetch(`${API_URL}/auth/sync-status/${encodeURIComponent(shopParam)}`, {
          credentials: 'include',
        });
        const data: SyncStatus = await response.json();

        setSyncStatus(data);
        setIsLoading(false);

        // Banner visibility rules:
        // - show while pending/in_progress
        // - show exactly once when transitioning to completed/failed in THIS session
        const was = prevStatusRef.current;
        const now = data.status;

        if (now === 'pending' || now === 'in_progress') {
          setShowBanner(true);
        } else if (
          (was === 'pending' || was === 'in_progress') &&
          (now === 'completed' || now === 'failed')
        ) {
          setShowBanner(true); // one-time success/failure banner after initial sync ends
        } else {
          // If we loaded already completed/failed/not_found, keep banner hidden
          setShowBanner(false);
        }
        prevStatusRef.current = now;

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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // === BILLING ADDITIONS START: fetch billing status ==========================
  useEffect(() => {
    if (!shop) return;
    (async () => {
      try {
        const r = await fetch(`${API_URL}/api/billing/status?shop=${encodeURIComponent(shop)}`, {
          credentials: 'include',
        });
        const j = await r.json();
        setIsPro(!!j.isPro);
      } catch (e) {
        console.error('Billing status error:', e);
        setIsPro(false);
      }
    })();
  }, [shop, API_URL]);
  // === BILLING ADDITIONS END: fetch billing status ============================

  // --- Effect: OPTIMIZED order count refresh with focus-based polling ---
  useEffect(() => {
    if (!shop) return;

    const refresh = () => fetchOrdersSummary(shop);

    // Initial fetch
    refresh();

    // Refresh when user returns to tab (saves ~90% of API calls)
    const onFocus = () => refresh();
    window.addEventListener('focus', onFocus);

    // Optional: Also refresh every 5 minutes for active users
    const intervalId = setInterval(refresh, 5 * 60 * 1000);

    return () => {
      window.removeEventListener('focus', onFocus);
      clearInterval(intervalId);
    };
  }, [shop]);

  const renderSyncBanner = () => {
    if (isLoading || !syncStatus || !showBanner) return null;

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
          <Banner tone="success" onDismiss={() => setShowBanner(false)}>
            <Text as="p">
              ✅ Successfully imported {syncStatus.orders_synced.toLocaleString()} orders from your store history!
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
            const titleText =
              typeof chart.layout.title === 'string'
                ? chart.layout.title
                : chart.layout.title?.text || '';

            return (
              <Layout.Section key={chart.key || index} variant="oneHalf">
                <Card>
                  <BlockStack gap="300">
                    <div style={{ padding: '16px 16px 0 16px' }}>
                      <InlineStack align="space-between" blockAlign="center">
                        <Text as="h2" variant="headingMd">
                          {titleText}
                        </Text>

                        {/* === BILLING ADDITIONS START: Pro-locked Excel download === */}
                        {chart.export_url && (
                          <InlineStack gap="200">
                            <ProLock
                              locked={!isPro}
                              tooltip="Excel downloads are a Pro feature — upgrade to unlock."
                            >
                              <span onClick={isPro ? () => downloadChart(chart) : undefined}>
                                <Button
                                  size="slim"
                                  disabled={!isPro}
                                  accessibilityLabel={
                                    isPro ? 'Download Excel' : 'Pro feature — upgrade to unlock'
                                  }
                                >
                                  Download
                                </Button>
                              </span>
                            </ProLock>

                            {!isPro && shop && (
                              <BillingButton shopDomain={shop} planName="Lodestar Pro" price={25} apiUrl={API_URL} />
                            )}
                          </InlineStack>
                        )}
                        {/* === BILLING ADDITIONS END: Pro-locked Excel download ===== */}
                      </InlineStack>
                    </div>

                    <Plot
                      data={chart.data}
                      layout={{
                        ...chart.layout,
                        title: undefined, // we render our own title row
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

  return (
    <ShopifyEmbedGate>
      <AppProvider i18n={enTranslations}>
        <div style={{ padding: '20px' }}>
          {renderSyncBanner()}

          <div style={{ marginTop: '30px' }}>
            <Card>
              <BlockStack gap="400">
                <Text as="h1" variant="headingLg">
                  Welcome to Lodestar
                </Text>

                {/* Use LIVE count from DB instead of the initial sync count */}
                {totalOrders !== null && (
                  <Text as="p" tone="subdued">
                    Your store has {totalOrders.toLocaleString()} orders ready to analyze.
                  </Text>
                )}

                {/* === BILLING ADDITIONS START: global upsell banner (optional) === */}
                {!isPro && shop && (
                  <Banner tone="info">
                    <InlineStack gap="300" align="space-between" blockAlign="center">
                      <Text as="p">
                        Excel downloads are part of the <b>Pro</b> plan. Upgrade to unlock one-click exports.
                      </Text>
                      <BillingButton shopDomain={shop} planName="Lodestar Pro" price={25} apiUrl ={API_URL}/>
                    </InlineStack>
                  </Banner>
                )}
                {/* === BILLING ADDITIONS END: global upsell banner (optional) ===== */}
              </BlockStack>
            </Card>

            {/* COGS management appears only after initial sync completes */}
            {shop && syncStatus?.status === 'completed' && (
              <div style={{ marginTop: '20px' }}>
                <COGSManagement shopDomain={shop} apiUrl={API_URL} />
              </div>
            )}

            {renderCharts()}
          </div>
        </div>
      </AppProvider>
    </ShopifyEmbedGate>
  );
}
