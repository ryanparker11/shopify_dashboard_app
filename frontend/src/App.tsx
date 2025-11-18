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
  DataTable,
  Badge,
} from '@shopify/polaris';
import enTranslations from '@shopify/polaris/locales/en.json';
import '@shopify/polaris/build/esm/styles.css';

import './lib/api';

import { COGSManagement } from './components/COGSManagement';
import { useEffect, useRef, useState } from 'react';
import type { ReactNode} from 'react';
import Plot from 'react-plotly.js';
import { authenticatedFetch, authenticatedBlobFetch } from './lib/api';

// Add type declaration for Shopify CDN
declare global {
  interface Window {
    shopify?: unknown;
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

interface Customer {
  customer_id: number;
  customer_name: string;
  customer_email: string;
  total_orders: number;
  total_revenue: number;
  total_profit: number | null;
  avg_order_value: number;
  last_order_date: string | null;
  profit_data_status: 'unavailable' | 'complete' | 'partial';
  profit_coverage: string | null;
}

interface CustomerLeaderboardResponse {
  customers: Customer[];
  summary: {
    total_customers: number;
    total_revenue: number;
    total_profit: number | null;
    profit_data_available: boolean;
    avg_revenue_per_customer: number;
    avg_profit_per_customer?: number | null;
  };
}

type SortDirection = 'ascending' | 'descending' | 'none';

// --------------------------------------------------------------------
// NEW: AuthGate component – checks /auth/check and redirects to /auth/start
// --------------------------------------------------------------------
function AuthGate({ children }: { children: ReactNode }) {
  const [isReady, setIsReady] = useState(false);
  const [hasToken, setHasToken] = useState(false);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const shop = params.get('shop');
    const host = params.get('host');

    if (!shop) {
      console.error('Missing ?shop= in URL – skipping auth check');
      // Let the app render anyway so you can show an error inside
      setHasToken(true);
      setIsReady(true);
      return;
    }

    const API_URL =
      import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

    (async () => {
      try {
        const res = await fetch(
          `${API_URL}/auth/check?shop=${encodeURIComponent(shop)}`,
          {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' },
            // credentials can be omitted; /auth/check just uses DB
          }
        );

        if (res.ok) {
          // We already have an access token for this shop
          setHasToken(true);
          setIsReady(true);
          return;
        }

        if (res.status === 401) {
          // No token yet → send through OAuth flow
          const authUrl =
            `${API_URL}/auth/start?shop=${encodeURIComponent(shop)}` +
            (host ? `&host=${encodeURIComponent(host)}` : '');

          if (window.top) {
            window.top.location.href = authUrl;
          } else {
            window.location.href = authUrl;
          }
          return;
        }

        console.error('Unexpected /auth/check status:', res.status);
        // As a fallback, let the app render so you can handle it gracefully
        setHasToken(true);
        setIsReady(true);
      } catch (err) {
        console.error('Error calling /auth/check:', err);
        // Fallback: attempt to go through OAuth anyway
        const authUrl =
          `${API_URL}/auth/start?shop=${encodeURIComponent(shop)}` +
          (host ? `&host=${encodeURIComponent(host)}` : '');

        if (window.top) {
          window.top.location.href = authUrl;
        } else {
          window.location.href = authUrl;
        }
      }
    })();
  }, []);

  if (!isReady) {
    return <div>Loading Lodestar…</div>;
  }

  if (!hasToken) {
    // In practice, we redirect before this ever shows.
    return null;
  }

  return <>{children}</>;
}

// --------------------------------------------------------------------
// Main AppContent – unchanged logic, just wrapped in AuthGate below
// --------------------------------------------------------------------
function AppContent() {
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const [totalOrders, setTotalOrders] = useState<number | null>(null);
  const [shop, setShop] = useState<string | null>(null);

  const [showBanner, setShowBanner] = useState(false);
  const prevStatusRef = useRef<SyncStatus['status'] | null>(null);

  // Customer leaderboard state
  const [customerData, setCustomerData] = useState<CustomerLeaderboardResponse | null>(null);
  const [sortedColumn, setSortedColumn] = useState<number | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>('descending');

  const API_URL =
    import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

  // --------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------

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

  const fetchCustomerLeaderboard = async (shopName: string) => {
    try {
      console.log('🔍 Fetching customer leaderboard for shop:', shopName);

      const data = await authenticatedFetch<CustomerLeaderboardResponse>(
        `/api/customers/leaderboard?shop_domain=${encodeURIComponent(shopName)}&limit=50`
      );

      console.log('✅ Customer leaderboard loaded successfully:', data.customers?.length || 0, 'customers');
      setCustomerData(data);
    } catch (error) {
      console.error('💥 Failed to fetch customer leaderboard:', error);
      setCustomerData(null);
    }
  };

  const fetchChartData = async (shopName: string) => {
    try {
      console.log('🔍 Fetching charts for shop:', shopName);

      const data = await authenticatedFetch<ChartsResponse>(
        `/api/charts/${encodeURIComponent(shopName)}`
      );

      console.log(
        '✅ Charts loaded successfully:',
        data.charts?.length || 0,
        'charts'
      );
      setChartData(data.charts || []);
    } catch (error) {
      console.error('💥 Failed to fetch chart data:', error);
      if (error instanceof Error) {
        console.error('💥 Error name:', error.name);
        console.error('💥 Error message:', error.message);
        console.error('💥 Error stack:', error.stack);
      }
      // Set empty charts on error
      setChartData([]);
    }
  };

  const resolveExportUrl = (exportUrl?: string) => {
    if (!exportUrl) return null;

    if (exportUrl.startsWith('/charts')) return `/api${exportUrl}`;

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

    const blob = await authenticatedBlobFetch(url);
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
    alert(
      `Failed to download chart data: ${
        err instanceof Error ? err.message : 'Unknown error'
      }`
    );
  }
};

  const downloadCustomerLeaderboard = async () => {
  if (!shop) return;

  try {
    const url = `/api/charts/${encodeURIComponent(shop)}/export/top_customers`;
    console.log('Attempting to download customer leaderboard from:', url);

    const blob = await authenticatedBlobFetch(url);
    console.log('Blob created, size:', blob.size, 'type:', blob.type);

    const filename = `customer_leaderboard_${new Date().toISOString().slice(0, 10)}.xlsx`;

    const objectUrl = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = objectUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(objectUrl);

    console.log('Customer leaderboard download completed successfully');
  } catch (err) {
    console.error('Download error:', err);
    alert(
      `Failed to download customer leaderboard: ${
        err instanceof Error ? err.message : 'Unknown error'
      }`
    );
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
    let checkInterval: number | null = null;
    let timeoutId: number | null = null;

    // Wait for App Bridge to be fully initialized
    const waitForAppBridge = () => {
      return new Promise<void>((resolve) => {
        // If already initialized, resolve immediately
        if (window.shopify) {
          console.log('✅ App Bridge already ready');
          resolve();
          return;
        }

        checkInterval = window.setInterval(() => {
          if (window.shopify) {
            if (checkInterval) clearInterval(checkInterval);
            if (timeoutId) clearTimeout(timeoutId);
            console.log('✅ App Bridge confirmed ready');
            resolve();
          }
        }, 100);

        // Timeout after 5 seconds
        timeoutId = window.setTimeout(() => {
          if (checkInterval) clearInterval(checkInterval);
          console.warn('⚠️ App Bridge timeout - proceeding anyway');
          resolve();
        }, 5000);
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

        // Only fetch charts/orders/customers when transitioning to completed
        if (data.status === 'completed' && was !== 'completed') {
          console.log('✅ Sync completed - fetching charts, orders, and customers');

          if (!isCancelled) {
            try {
              await Promise.all([
                fetchChartData(shopParam),
                fetchOrdersSummary(shopParam),
                fetchCustomerLeaderboard(shopParam),
              ]);
              console.log('✅ All data fetched successfully');
            } catch (error) {
              console.error('💥 Error fetching data:', error);
            }
          }
        }

        // Continue polling if still in progress
        if (data.status === 'pending' || data.status === 'in_progress') {
          window.setTimeout(checkSyncStatus, 3000);
        }
      } catch (error) {
        console.error('💥 Failed to fetch sync status:', error);
        if (!isCancelled) {
          setIsLoading(false);
        }
      }
    };

    // Wait for App Bridge ONCE, then start checking sync status
    waitForAppBridge().then(() => {
      if (!isCancelled) {
        checkSyncStatus();
      }
    });

    // Cleanup
    return () => {
      isCancelled = true;
      if (checkInterval) clearInterval(checkInterval);
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, []);

  // --------------------------------------------------------------------
  // Effect: optimized order-count refresh (focus-based polling)
  // --------------------------------------------------------------------

  useEffect(() => {
    if (!shop) return;

    const refresh = () => {
      fetchOrdersSummary(shop);
      fetchCustomerLeaderboard(shop);
    };

    // Initial fetch
    refresh();

    const onFocus = () => refresh();
    window.addEventListener('focus', onFocus);

    const intervalId = window.setInterval(refresh, 5 * 60 * 1000);

    return () => {
      window.removeEventListener('focus', onFocus);
      clearInterval(intervalId);
    };
  }, [shop]);

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

  const renderCustomerLeaderboard = () => {
    if (!customerData || customerData.customers.length === 0) return null;

    const { customers, summary } = customerData;

    // Prepare table data with sorting
    const getSortedCustomers = () => {
      if (sortedColumn === null || sortDirection === 'none') {
        return customers;
      }

      const sorted = [...customers];

      sorted.sort((a, b) => {
        let aVal: number | string | null;
        let bVal: number | string | null;

        switch (sortedColumn) {
          case 0: // Customer Name
            aVal = a.customer_name;
            bVal = b.customer_name;
            break;
          case 1: // Orders
            aVal = a.total_orders;
            bVal = b.total_orders;
            break;
          case 2: // Revenue
            aVal = a.total_revenue;
            bVal = b.total_revenue;
            break;
          case 3: // Profit
            aVal = a.total_profit ?? 0;
            bVal = b.total_profit ?? 0;
            break;
          case 4: // AOV
            aVal = a.avg_order_value;
            bVal = b.avg_order_value;
            break;
          case 5: // Last Order
            aVal = a.last_order_date || '';
            bVal = b.last_order_date || '';
            break;
          default:
            return 0;
        }

        if (aVal === null) aVal = 0;
        if (bVal === null) bVal = 0;

        if (typeof aVal === 'string' && typeof bVal === 'string') {
          return sortDirection === 'ascending'
            ? aVal.localeCompare(bVal)
            : bVal.localeCompare(aVal);
        }

        return sortDirection === 'ascending'
          ? (aVal as number) - (bVal as number)
          : (bVal as number) - (aVal as number);
      });

      return sorted;
    };

    const sortedCustomers = getSortedCustomers();

    const headings = [
      'Customer',
      'Orders',
      'Revenue',
      'Profit',
      'Avg Order Value',
      'Last Order',
    ];

    const rows = sortedCustomers.map((customer) => [
      <div key={`name-${customer.customer_id}`}>
        <Text as="p" variant="bodyMd" fontWeight="semibold">
          {customer.customer_name}
        </Text>
        <Text as="p" variant="bodySm" tone="subdued">
          {customer.customer_email}
        </Text>
      </div>,
      customer.total_orders.toLocaleString(),
      `$${customer.total_revenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      customer.total_profit !== null ? (
        <div key={`profit-${customer.customer_id}`}>
          <Text as="p" variant="bodyMd">
            ${customer.total_profit.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </Text>
          {customer.profit_data_status === 'partial' && (
            <Badge tone="warning" size="small">
              Partial data
            </Badge>
          )}
        </div>
      ) : (
        <Badge tone="info" size="small">
          No COGS data
        </Badge>
      ),
      `$${customer.avg_order_value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      customer.last_order_date
        ? new Date(customer.last_order_date).toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
          })
        : 'Never',
    ]);

    const handleSort = (index: number, direction: SortDirection) => {
      setSortedColumn(index);
      setSortDirection(direction);
    };

    return (
      <div style={{ marginTop: '20px' }}>
        <Card>
          <BlockStack gap="400">
            <InlineStack align="space-between" blockAlign="center">
              <div>
                <Text as="h2" variant="headingLg">
                  Customer Leaderboard
                </Text>
                <Text as="p" variant="bodySm" tone="subdued">
                  Top {summary.total_customers} customers by revenue
                </Text>
              </div>
              <Button onClick={downloadCustomerLeaderboard}>
                Download Excel
              </Button>
            </InlineStack>

            {/* Summary Cards */}
            <InlineStack gap="400" wrap={false}>
              <Card>
                <BlockStack gap="200">
                  <Text as="p" variant="bodySm" tone="subdued">
                    Total Revenue
                  </Text>
                  <Text as="p" variant="headingMd">
                    ${summary.total_revenue.toLocaleString('en-US', { minimumFractionDigits: 2 })}
                  </Text>
                </BlockStack>
              </Card>

              {summary.profit_data_available && (
                <Card>
                  <BlockStack gap="200">
                    <Text as="p" variant="bodySm" tone="subdued">
                      Total Profit
                    </Text>
                    <Text as="p" variant="headingMd">
                      ${(summary.total_profit ?? 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}
                    </Text>
                  </BlockStack>
                </Card>
              )}

              <Card>
                <BlockStack gap="200">
                  <Text as="p" variant="bodySm" tone="subdued">
                    Avg Revenue/Customer
                  </Text>
                  <Text as="p" variant="headingMd">
                    ${summary.avg_revenue_per_customer.toLocaleString('en-US', { minimumFractionDigits: 2 })}
                  </Text>
                </BlockStack>
              </Card>
            </InlineStack>

            {/* Data Table */}
            <DataTable
              columnContentTypes={['text', 'numeric', 'numeric', 'numeric', 'numeric', 'text']}
              headings={headings}
              rows={rows}
              sortable={[true, true, true, true, true, true]}
              defaultSortDirection="descending"
              initialSortColumnIndex={2} // Sort by revenue by default
              onSort={handleSort}
            />
          </BlockStack>
        </Card>
      </div>
    );
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
              <Layout.Section key={chart.key || index} variant="oneHalf">
                <Card>
                  <BlockStack gap="300">
                    <div style={{ padding: '16px 16px 0 16px' }}>
                      <InlineStack align="space-between" blockAlign="center">
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
              <InlineStack align="space-between" blockAlign="center">
                <Text as="h1" variant="headingLg">
                  Welcome to Lodestar
                </Text>
              </InlineStack>

              {totalOrders !== null && (
                <Text as="p" tone="subdued">
                  Your store has {totalOrders.toLocaleString()} orders ready to
                  analyze.
                </Text>
              )}
            </BlockStack>
          </Card>

          {/* COGS module */}
          {shop && syncStatus?.status === 'completed' && (
            <div style={{ marginTop: '20px' }}>
              <COGSManagement shopDomain={shop} />
            </div>
          )}

          {/* Customer Leaderboard */}
          {syncStatus?.status === 'completed' && renderCustomerLeaderboard()}

          {renderCharts()}
        </div>
      </div>
    </AppProvider>
  );
}

// --------------------------------------------------------------------
// Root App – now wrapped in AuthGate
// --------------------------------------------------------------------
export default function App() {
  return (
    <AuthGate>
      <AppContent />
    </AuthGate>
  );
}