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
  Tabs,
  Icon,
} from '@shopify/polaris';
import { CheckCircleIcon, ClockIcon } from '@shopify/polaris-icons';
import enTranslations from '@shopify/polaris/locales/en.json';
import '@shopify/polaris/build/esm/styles.css';

import './lib/api';

import { COGSManagement } from './components/COGSManagement';
import { ForecastsPage } from './components/ForecastsPage';
import { AttributionPage } from './components/AttributionPage';
import { SKUAnalyticsPage } from './components/SkuAnalyticsPage';
import { WhatIfScenariosPage } from './components/WhatIfScenariosPage';
import { useEffect, useRef, useState, useCallback } from 'react';
import type { ReactNode } from 'react';
import Plot from 'react-plotly.js';
import { authenticatedFetch, authenticatedBlobFetch } from './lib/api';

// Add type declaration for Shopify CDN
declare global {
  interface Window {
    shopify?: unknown;
  }
}

// ---------- Types for Insights / Alerts / Comparison ---------- //

interface TrendDelta {
  current: number;
  previous: number;
  delta_amount: number;
  delta_percent: number | null;
  direction: string;
}

interface ChartAlert {
  level: 'warning' | 'positive' | 'info' | string;
  metric: string;
  message: string;
}

interface ChartComparisonWindow {
  label?: string;
  x: string[];
  y: number[];
}

interface ChartComparison {
  previous_30d?: ChartComparisonWindow;
}

// ---------- Updated SyncStatus interface to match new backend ---------- //
interface SyncStatus {
  status: 'pending' | 'in_progress' | 'completed' | 'failed' | 'not_found';
  completed_at: string | null;
  error: string | null;

  // Detailed stage information
  current_stage: 'customers' | 'products' | 'orders' | 'line_items' | 'completed' | null;
  stage_status: 'pending' | 'in_progress' | 'completed' | 'failed' | null;

  // Counts for each stage
  customers_synced: number;
  products_synced: number;
  orders_synced: number;
  line_items_synced: number;

  // Completion flags
  customers_completed: boolean;
  products_completed: boolean;
  orders_completed: boolean;
  line_items_completed: boolean;

  // Overall progress
  progress_percent: number;
  stages_completed: number;
  total_stages: number;
}

interface ChartSummary {
  current_month?: string;
  previous_month?: string;
  delta?: TrendDelta;
  current_revenue?: number;
  previous_revenue?: number;

  total_orders_30d?: number;
  total_orders_prev_30d?: number;
  delta_30d?: TrendDelta;
  delta_7d?: TrendDelta;

  revenue_30d?: number;
  revenue_prev_30d?: number;

  total_top_revenue?: number;
  top_product?: {
    name: string;
    revenue: number;
    share_percent: number;
  };
  top_customer?: {
    name: string;
    revenue: number;
    share_percent: number;
  };

  formatted?: Record<string, string | null>;
}

interface ChartData {
  key?: string;
  data: Plotly.Data[];
  layout: Partial<Plotly.Layout & { title?: string | { text?: string } }>;
  export_url?: string;

  insights?: string[];
  alerts?: ChartAlert[];
  summary?: ChartSummary | null;
  comparison?: ChartComparison;
}

interface OrdersSummary {
  total_orders: number;
  total_revenue?: number;
  avg_order_value?: number;
  formatted?: {
    total_revenue: string;
    avg_order_value: string;
    orders_30d?: string;
    orders_prev_30d?: string;
    revenue_30d?: string;
    revenue_prev_30d?: string;
  };
  trend?: {
    revenue_30d?: TrendDelta;
    orders_30d?: TrendDelta;
  };
  insights?: string[];
  alerts?: ChartAlert[];
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
  formatted?: {
    total_revenue?: string;
    total_profit?: string | null;
    avg_order_value?: string;
  };
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
    formatted?: {
      total_revenue?: string;
      total_profit?: string | null;
      avg_revenue_per_customer?: string;
      avg_profit_per_customer?: string | null;
    };
    insights?: string[];
  };
}

type SortDirection = 'ascending' | 'descending' | 'none';

// --------------------------------------------------------------------
// AuthGate component
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
      setHasToken(true);
      setIsReady(true);
      return;
    }

    const API_URL = import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

    (async () => {
      try {
        const res = await fetch(
          `${API_URL}/auth/check?shop=${encodeURIComponent(shop)}`,
          {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' },
          }
        );

        if (res.ok) {
          setHasToken(true);
          setIsReady(true);
          return;
        }

        if (res.status === 401) {
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
        setHasToken(true);
        setIsReady(true);
      } catch (err) {
        console.error('Error calling /auth/check:', err);
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
    return null;
  }

  return <>{children}</>;
}

// --------------------------------------------------------------------
// Sync Stage Item Component
// --------------------------------------------------------------------
interface SyncStageItemProps {
  label: string;
  count: number;
  isCompleted: boolean;
  isActive: boolean;
  isFailed: boolean;
}

function SyncStageItem({
  label,
  count,
  isCompleted,
  isActive,
  isFailed,
}: SyncStageItemProps) {
  const getStatusIcon = () => {
    if (isCompleted) {
      return <Icon source={CheckCircleIcon} tone="success" />;
    }
    if (isActive) {
      return <Icon source={ClockIcon} tone="info" />;
    }
    return <Icon source={ClockIcon} tone="subdued" />;
  };

  const getStatusText = () => {
    if (isFailed) return 'Failed';
    if (isCompleted) return `${count.toLocaleString()} synced`;
    if (isActive) return `${count.toLocaleString()} syncing...`;
    return 'Waiting';
  };

  const getTextTone = (): 'subdued' | 'success' | 'critical' | undefined => {
    if (isFailed) return 'critical';
    if (isCompleted) return 'success';
    if (isActive) return undefined;
    return 'subdued';
  };

  return (
    <InlineStack gap="200" align="start" blockAlign="center">
      {getStatusIcon()}
      <BlockStack gap="050">
        <Text as="p" variant="bodyMd" fontWeight={isActive ? 'semibold' : 'regular'}>
          {label}
        </Text>
        <Text as="p" variant="bodySm" tone={getTextTone()}>
          {getStatusText()}
        </Text>
      </BlockStack>
    </InlineStack>
  );
}

// --------------------------------------------------------------------
// Main AppContent
// --------------------------------------------------------------------
function AppContent() {
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [chartData, setChartData] = useState<ChartData[]>([]);
  const [ordersSummary, setOrdersSummary] = useState<OrdersSummary | null>(null);
  const [shop, setShop] = useState<string | null>(null);

  const [showBanner, setShowBanner] = useState(false);
  const prevStatusRef = useRef<SyncStatus['status'] | null>(null);

  const [customerData, setCustomerData] =
    useState<CustomerLeaderboardResponse | null>(null);
  const [sortedColumn, setSortedColumn] = useState<number | null>(null);
  const [sortDirection, setSortDirection] =
    useState<SortDirection>('descending');

  const [selectedTab, setSelectedTab] = useState(0);

  const [comparisonVisibility, setComparisonVisibility] = useState<
    Record<string, boolean>
  >({});

  const [subscriptionStatus, setSubscriptionStatus] = useState<string | null>(null);

  const API_URL =
    import.meta.env.VITE_API_URL || 'https://api.lodestaranalytics.io';

  const isSubscriptionActive =
    !subscriptionStatus || subscriptionStatus === 'ACTIVE';

  // --------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------

  async function fetchOrdersSummary() {
    try {
      const data = await authenticatedFetch<OrdersSummary>('/api/orders/summary');
      setOrdersSummary(data);
    } catch (e) {
      console.error('Failed to fetch orders summary:', e);
    }
  }

  const fetchCustomerLeaderboard = async () => {
    try {
      console.log('🔍 Fetching customer leaderboard');
      const data = await authenticatedFetch<CustomerLeaderboardResponse>(
        '/api/customers/leaderboard?limit=50'
      );
      console.log(
        '✅ Customer leaderboard loaded:',
        data.customers?.length || 0,
        'customers'
      );
      setCustomerData(data);
    } catch (error) {
      console.error('💥 Failed to fetch customer leaderboard:', error);
      setCustomerData(null);
    }
  };

  const fetchChartData = async () => {
    try {
      console.log('🔍 Fetching charts');
      const data = await authenticatedFetch<ChartsResponse>('/api/charts');
      console.log('✅ Charts loaded:', data.charts?.length || 0, 'charts');
      setChartData(data.charts || []);
    } catch (error) {
      console.error('💥 Failed to fetch chart data:', error);
      setChartData([]);
    }
  };

  const fetchSubscriptionStatus = async () => {
    try {
      const data = await authenticatedFetch<{ subscription_status: string }>(
        '/api/billing/subscription-status'
      );
      setSubscriptionStatus(data.subscription_status);
    } catch (error) {
      console.error('💥 Failed to fetch subscription status:', error);
      // Fail-open: leave subscriptionStatus = null so features stay usable
      setSubscriptionStatus(null);
    }
  };

  const handleUpgradeClick = async () => {
    try {
      const data = await authenticatedFetch<{ pricing_url: string }>(
        '/api/billing/pricing-url'
      );
      const url = data.pricing_url;
      if (!url) {
        throw new Error('Missing pricing_url in response');
      }

      if (window.top) {
        window.top.location.href = url;
      } else {
        window.location.href = url;
      }
    } catch (error) {
      console.error('💥 Failed to get pricing URL:', error);
      alert(
        'Unable to open the subscription page right now. Please try again, or contact support if this continues.'
      );
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

      const filename = `${(chart.key || 'chart')}_${new Date()
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
    try {
      const url = '/api/charts/export/top_customers';
      console.log('Attempting to download customer leaderboard from:', url);

      const blob = await authenticatedBlobFetch(url);
      console.log('Blob created, size:', blob.size, 'type:', blob.type);

      const filename = `customer_leaderboard_${new Date()
        .toISOString()
        .slice(0, 10)}.xlsx`;

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

  const toggleComparison = (chartKey: string) => {
    setComparisonVisibility((prev) => ({
      ...prev,
      [chartKey]: !prev[chartKey],
    }));
  };

  const toneFromAlertLevel = (
    level: ChartAlert['level']
  ): 'info' | 'success' | 'warning' | 'critical' => {
    if (level === 'warning') return 'warning';
    if (level === 'positive') return 'success';
    return 'info';
  };

  const renderSubscriptionBanner = () => (
    <Banner tone="warning">
      <BlockStack gap="300">
        <Text as="p" variant="bodyMd" fontWeight="semibold">
          Subscribe to unlock SKU Analytics, What If, Forecasts, and Attribution
        </Text>
        <Text as="p" variant="bodySm">
          Upgrade your Lodestar plan to access advanced SKU-level analytics, scenario
          planning, predictive forecasts, and marketing attribution.
        </Text>
        <Button variant="primary" onClick={handleUpgradeClick}>
          View subscription options
        </Button>
      </BlockStack>
    </Banner>
  );

  const renderPaywalledTabContent = () => (
    <BlockStack gap="400">
      {renderSubscriptionBanner()}
    </BlockStack>
  );

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

    const waitForAppBridge = () => {
      return new Promise<void>((resolve) => {
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

        const response = await fetch(
          `${API_URL}/auth/sync-status/${encodeURIComponent(shopParam)}`,
          {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' },
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

        // Fetch data when sync completes
        if (data.status === 'completed' && was !== 'completed') {
          console.log('✅ Sync completed - fetching charts, orders, and customers');

          if (!isCancelled) {
            try {
              await Promise.all([
                fetchChartData(),
                fetchOrdersSummary(),
                fetchCustomerLeaderboard(),
                fetchSubscriptionStatus(),
              ]);
              console.log('✅ All data fetched successfully');
            } catch (error) {
              console.error('💥 Error fetching data:', error);
            }
          }
        }

        // Continue polling if still in progress
        if (data.status === 'pending' || data.status === 'in_progress') {
          window.setTimeout(checkSyncStatus, 2000); // Poll every 2 seconds for better UX
        }
      } catch (error) {
        console.error('💥 Failed to fetch sync status:', error);
        if (!isCancelled) {
          setIsLoading(false);
        }
      }
    };

    // Fetch subscription status early (even before sync completes)
    fetchSubscriptionStatus().catch((err) =>
      console.error('Failed initial subscription status fetch:', err)
    );

    waitForAppBridge().then(() => {
      if (!isCancelled) {
        checkSyncStatus();
      }
    });

    return () => {
      isCancelled = true;
      if (checkInterval) clearInterval(checkInterval);
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, [API_URL]);

  // --------------------------------------------------------------------
  // Effect: optimized order-count refresh
  // --------------------------------------------------------------------

  useEffect(() => {
    if (!shop) return;

    const refresh = () => {
      fetchOrdersSummary();
      fetchCustomerLeaderboard();
    };

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
  // Tab change handler
  // --------------------------------------------------------------------

  const handleTabChange = useCallback(
    (selectedTabIndex: number) => setSelectedTab(selectedTabIndex),
    []
  );

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
              <Text as="p">Your store data is being prepared for import...</Text>
            </BlockStack>
          </Banner>
        );

      case 'in_progress': {
        const progress = syncStatus.progress_percent || 0;
        const currentStage = syncStatus.current_stage;

        const stageLabels: Record<string, string> = {
          customers: 'Customers',
          products: 'Products',
          orders: 'Orders',
          line_items: 'Order Details',
        };

        const currentStageLabel = currentStage
          ? stageLabels[currentStage] || currentStage
          : '';

        return (
          <Banner tone="info">
            <BlockStack gap="400">
              <BlockStack gap="200">
                <Text as="p" variant="headingMd">
                  Importing your store data...
                </Text>
                <Text as="p" variant="bodySm" tone="subdued">
                  {currentStageLabel && `Currently syncing: ${currentStageLabel}`}
                </Text>
              </BlockStack>

              <BlockStack gap="100">
                <InlineStack align="space-between">
                  <Text as="p" variant="bodySm" tone="subdued">
                    Overall progress
                  </Text>
                  <Text as="p" variant="bodySm" tone="subdued">
                    {Math.round(progress)}%
                  </Text>
                </InlineStack>
                <ProgressBar progress={progress} size="small" />
              </BlockStack>

              <InlineStack gap="600" wrap>
                <SyncStageItem
                  label="Customers"
                  count={syncStatus.customers_synced}
                  isCompleted={syncStatus.customers_completed}
                  isActive={currentStage === 'customers'}
                  isFailed={false}
                />
                <SyncStageItem
                  label="Products"
                  count={syncStatus.products_synced}
                  isCompleted={syncStatus.products_completed}
                  isActive={currentStage === 'products'}
                  isFailed={false}
                />
                <SyncStageItem
                  label="Orders"
                  count={syncStatus.orders_synced}
                  isCompleted={syncStatus.orders_completed}
                  isActive={currentStage === 'orders'}
                  isFailed={false}
                />
                <SyncStageItem
                  label="Order Details"
                  count={syncStatus.line_items_synced}
                  isCompleted={syncStatus.line_items_completed}
                  isActive={currentStage === 'line_items'}
                  isFailed={false}
                />
              </InlineStack>

              <Text as="p" variant="bodySm" tone="subdued">
                This may take a few minutes depending on your store size. You can use the app
                while this completes.
              </Text>
            </BlockStack>
          </Banner>
        );
      }

      case 'completed': {
        return (
          <Banner tone="success" onDismiss={() => setShowBanner(false)}>
            <BlockStack gap="200">
              <Text as="p" fontWeight="semibold">
                ✅ Successfully imported your store data!
              </Text>
              <Text as="p" variant="bodySm">
                {syncStatus.customers_synced.toLocaleString()} customers •{' '}
                {syncStatus.products_synced.toLocaleString()} products •{' '}
                {syncStatus.orders_synced.toLocaleString()} orders •{' '}
                {syncStatus.line_items_synced.toLocaleString()} line items
              </Text>
            </BlockStack>
          </Banner>
        );
      }

      case 'failed':
        return (
          <Banner tone="critical" onDismiss={() => setShowBanner(false)}>
            <BlockStack gap="200">
              <Text as="p" fontWeight="semibold">
                Failed to import store data
              </Text>

              {syncStatus.error && (
                <Text as="p" variant="bodySm">
                  Error: {syncStatus.error}
                </Text>
              )}

              {syncStatus.current_stage && (
                <Text as="p" variant="bodySm">
                  Failed during: {syncStatus.current_stage}
                </Text>
              )}

              <Text as="p" variant="bodySm">
                Don't worry — new orders will still be tracked. Contact support if this
                persists.
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

    const getSortedCustomers = () => {
      if (sortedColumn === null || sortDirection === 'none') {
        return customers;
      }

      const sorted = [...customers];

      sorted.sort((a, b) => {
        let aVal: number | string | null;
        let bVal: number | string | null;

        switch (sortedColumn) {
          case 0:
            aVal = a.customer_name;
            bVal = b.customer_name;
            break;
          case 1:
            aVal = a.total_orders;
            bVal = b.total_orders;
            break;
          case 2:
            aVal = a.total_revenue;
            bVal = b.total_revenue;
            break;
          case 3:
            aVal = a.total_profit ?? 0;
            bVal = b.total_profit ?? 0;
            break;
          case 4:
            aVal = a.avg_order_value;
            bVal = b.avg_order_value;
            break;
          case 5:
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

    const rows = sortedCustomers.map((customer) => {
      const revenueDisplay =
        customer.formatted?.total_revenue ??
        `$${customer.total_revenue.toLocaleString('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}`;

      const profitDisplay =
        customer.total_profit !== null
          ? customer.formatted?.total_profit ??
            `$${customer.total_profit.toLocaleString('en-US', {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            })}`
          : null;

      const aovDisplay =
        customer.formatted?.avg_order_value ??
        `$${customer.avg_order_value.toLocaleString('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}`;

      return [
        <div key={`name-${customer.customer_id}`}>
          <Text as="p" variant="bodyMd" fontWeight="semibold">
            {customer.customer_name}
          </Text>
          <Text as="p" variant="bodySm" tone="subdued">
            {customer.customer_email}
          </Text>
        </div>,
        customer.total_orders.toLocaleString(),
        revenueDisplay,
        customer.total_profit !== null ? (
          <div key={`profit-${customer.customer_id}`}>
            <Text as="p" variant="bodyMd">
              {profitDisplay}
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
        aovDisplay,
        customer.last_order_date
          ? new Date(customer.last_order_date).toLocaleDateString('en-US', {
              year: 'numeric',
              month: 'short',
              day: 'numeric',
            })
          : 'Never',
      ];
    });

    const handleSort = (index: number, direction: SortDirection) => {
      setSortedColumn(index);
      setSortDirection(direction);
    };

    const totalRevenueDisplay =
      summary.formatted?.total_revenue ??
      `$${summary.total_revenue.toLocaleString('en-US', {
        minimumFractionDigits: 2,
      })}`;
    const totalProfitDisplay =
      summary.profit_data_available &&
      (summary.formatted?.total_profit ??
        `$${(summary.total_profit ?? 0).toLocaleString('en-US', {
          minimumFractionDigits: 2,
        })}`);
    const avgRevenueDisplay =
      summary.formatted?.avg_revenue_per_customer ??
      `$${summary.avg_revenue_per_customer.toLocaleString('en-US', {
        minimumFractionDigits: 2,
      })}`;

    return (
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
            <Button onClick={downloadCustomerLeaderboard}>Download Excel</Button>
          </InlineStack>

          {summary.insights && summary.insights.length > 0 && (
            <BlockStack gap="100">
              {summary.insights.map((insight, idx) => (
                <Text
                  as="p"
                  key={`leaderboard-insight-${idx}`}
                  variant="bodySm"
                  tone="subdued"
                >
                  • {insight}
                </Text>
              ))}
            </BlockStack>
          )}

          <InlineStack gap="400" wrap={false}>
            <Card>
              <BlockStack gap="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  Total Revenue
                </Text>
                <Text as="p" variant="headingMd">
                  {totalRevenueDisplay}
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
                    {totalProfitDisplay}
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
                  {avgRevenueDisplay}
                </Text>
              </BlockStack>
            </Card>
          </InlineStack>

          <DataTable
            columnContentTypes={[
              'text',
              'numeric',
              'numeric',
              'numeric',
              'numeric',
              'text',
            ]}
            headings={headings}
            rows={rows}
            sortable={[true, true, true, true, true, true]}
            defaultSortDirection="descending"
            initialSortColumnIndex={2}
            onSort={handleSort}
          />
        </BlockStack>
      </Card>
    );
  };

  const renderCharts = () => {
    if (syncStatus?.status !== 'completed' || chartData.length === 0) return null;

    return (
      <Layout>
        {chartData.map((chart, index) => {
          const titleText =
            typeof chart.layout.title === 'string'
              ? chart.layout.title
              : chart.layout.title?.text || '';

          const chartKey = chart.key || `chart-${index}`;
          const isComparisonVisible = !!comparisonVisibility[chartKey];

          const traces: Plotly.Data[] = [...chart.data];
          const prevWindow = chart.comparison?.previous_30d;
          if (prevWindow && isComparisonVisible) {
            const comparisonTrace = {
              x: prevWindow.x,
              y: prevWindow.y,
              type: 'scatter',
              mode: 'lines',
              name: prevWindow.label || 'Previous 30 Days',
              line: { dash: 'dot' },
            } as Plotly.Data;
            traces.push(comparisonTrace);
          }

          return (
            <Layout.Section key={chartKey} variant="oneHalf">
              <Card>
                <BlockStack gap="300">
                  <div style={{ padding: '16px 16px 0 16px' }}>
                    <InlineStack align="space-between" blockAlign="center" gap="200">
                      <Text as="h2" variant="headingMd">
                        {titleText}
                      </Text>

                      <InlineStack gap="200" blockAlign="center">
                        {chart.comparison?.previous_30d && (
                          <Button
                            size="slim"
                            variant={isComparisonVisible ? 'primary' : 'secondary'}
                            onClick={() => toggleComparison(chartKey)}
                          >
                            {isComparisonVisible
                              ? 'Hide comparison'
                              : 'Compare to previous 30 days'}
                          </Button>
                        )}

                        {chart.export_url && (
                          <Button size="slim" onClick={() => downloadChart(chart)}>
                            Download
                          </Button>
                        )}
                      </InlineStack>
                    </InlineStack>
                  </div>

                  <Plot
                    data={traces}
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

                  {chart.alerts && chart.alerts.length > 0 && (
                    <div style={{ padding: '0 16px 16px 16px' }}>
                      <BlockStack gap="200">
                        {chart.alerts.map((alert, idx) => (
                          <Banner
                            key={`chart-alert-${chartKey}-${idx}`}
                            tone={toneFromAlertLevel(alert.level)}
                          >
                            <Text as="p" variant="bodySm">
                              {alert.message}
                            </Text>
                          </Banner>
                        ))}
                      </BlockStack>
                    </div>
                  )}

                  {chart.insights && chart.insights.length > 0 && (
                    <div style={{ padding: '0 16px 16px 16px' }}>
                      <BlockStack gap="100">
                        {chart.insights.map((insight, idx) => (
                          <Text
                            as="p"
                            key={`chart-insight-${chartKey}-${idx}`}
                            variant="bodySm"
                            tone="subdued"
                          >
                            • {insight}
                          </Text>
                        ))}
                      </BlockStack>
                    </div>
                  )}
                </BlockStack>
              </Card>
            </Layout.Section>
          );
        })}
      </Layout>
    );
  };

  const renderOrdersOverview = () => {
    if (!ordersSummary) return null;

    const totalOrders = ordersSummary.total_orders ?? 0;
    const formatted = ordersSummary.formatted;
    const trend = ordersSummary.trend || {};

    const revenueDelta = trend.revenue_30d;
    const ordersDelta = trend.orders_30d;

    const formatDeltaLabel = (delta?: TrendDelta, label?: string) => {
      if (!delta || delta.delta_percent == null) return null;
      const direction =
        delta.direction === 'up' ? '▲' : delta.direction === 'down' ? '▼' : '•';
      return `${label || ''} ${direction} ${Math.abs(delta.delta_percent).toFixed(
        1
      )}% vs prior 30 days`;
    };

    return (
      <Card>
        <BlockStack gap="300">
          <Text as="h2" variant="headingMd">
            Store overview
          </Text>

          <InlineStack gap="400" wrap={false}>
            <Card>
              <BlockStack gap="100">
                <Text as="p" variant="bodySm" tone="subdued">
                  Total orders
                </Text>
                <Text as="p" variant="headingMd">
                  {totalOrders.toLocaleString()}
                </Text>
              </BlockStack>
            </Card>

            <Card>
              <BlockStack gap="100">
                <Text as="p" variant="bodySm" tone="subdued">
                  Total revenue (lifetime)
                </Text>
                <Text as="p" variant="headingMd">
                  {formatted?.total_revenue ?? '--'}
                </Text>
              </BlockStack>
            </Card>

            <Card>
              <BlockStack gap="100">
                <Text as="p" variant="bodySm" tone="subdued">
                  Avg order value
                </Text>
                <Text as="p" variant="headingMd">
                  {formatted?.avg_order_value ?? '--'}
                </Text>
              </BlockStack>
            </Card>
          </InlineStack>

          <BlockStack gap="100">
            {revenueDelta && (
              <Text as="p" variant="bodySm" tone="subdued">
                {formatDeltaLabel(revenueDelta, 'Revenue')} (
                {formatted?.revenue_30d || '—'} vs{' '}
                {formatted?.revenue_prev_30d || '—'})
              </Text>
            )}
            {ordersDelta && (
              <Text as="p" variant="bodySm" tone="subdued">
                {formatDeltaLabel(ordersDelta, 'Orders')} (
                {formatted?.orders_30d || '—'} vs{' '}
                {formatted?.orders_prev_30d || '—'})
              </Text>
            )}
          </BlockStack>
        </BlockStack>
      </Card>
    );
  };

  // --------------------------------------------------------------------
  // Tab content rendering
  // --------------------------------------------------------------------

  const renderAnalyticsTab = () => {
    return (
      <BlockStack gap="400">
        {renderOrdersOverview()}
        {renderCharts()}
      </BlockStack>
    );
  };

  const renderCOGSTab = () => {
    if (!shop) return null;
    return <COGSManagement shopDomain={shop} />;
  };

  const renderForecastsTab = () => {
    if (!shop) return null;
    if (!isSubscriptionActive) {
      return renderPaywalledTabContent();
    }
    return <ForecastsPage />;
  };

  const renderCustomersTab = () => {
    return renderCustomerLeaderboard();
  };

  const renderAttributionTab = () => {
    if (!shop) return null;
    if (!isSubscriptionActive) {
      return renderPaywalledTabContent();
    }
    return <AttributionPage shopDomain={shop} />;
  };

  const renderSKUAnalyticsTab = () => {
    if (!isSubscriptionActive) {
      return renderPaywalledTabContent();
    }
    return <SKUAnalyticsPage />;
  };

  const renderWhatIfTab = () => {
    if (!isSubscriptionActive) {
      return renderPaywalledTabContent();
    }
    return <WhatIfScenariosPage />;
  };

  // --------------------------------------------------------------------
  // Main return
  // --------------------------------------------------------------------

  const tabs = [
    { id: 'analytics', content: 'Analytics', panelID: 'analytics-panel' },
    { id: 'sku', content: 'SKU Analytics', panelID: 'sku-panel' },
    { id: 'what-if', content: 'What If', panelID: 'what-if-panel' },
    { id: 'cogs', content: 'COGS & Profit', panelID: 'cogs-panel' },
    { id: 'forecasts', content: 'Forecasts', panelID: 'forecasts-panel' },
    { id: 'customers', content: 'Customers', panelID: 'customers-panel' },
    { id: 'attribution', content: 'Attribution', panelID: 'attribution-panel' },
  ];

  return (
    <AppProvider i18n={enTranslations}>
      <div style={{ padding: '20px' }}>
        {renderSyncBanner()}

        <div style={{ marginTop: '30px' }}>
          <Card>
            <BlockStack gap="400">
              <InlineStack align="space-between" blockAlign="center">
                <Text as="h1" variant="headingLg">
                  Lodestar Analytics
                </Text>
              </InlineStack>
            </BlockStack>
          </Card>

          {/* Global subscription banner directly under header */}
          {subscriptionStatus && subscriptionStatus !== 'ACTIVE' && (
            <div style={{ marginTop: '20px' }}>{renderSubscriptionBanner()}</div>
          )}

          {syncStatus?.status === 'completed' && (
            <div style={{ marginTop: '20px' }}>
              <Tabs tabs={tabs} selected={selectedTab} onSelect={handleTabChange}>
                <div style={{ marginTop: '20px' }}>
                  {selectedTab === 0 && renderAnalyticsTab()}
                  {selectedTab === 1 && renderSKUAnalyticsTab()}
                  {selectedTab === 2 && renderWhatIfTab()}
                  {selectedTab === 3 && renderCOGSTab()}
                  {selectedTab === 4 && renderForecastsTab()}
                  {selectedTab === 5 && renderCustomersTab()}
                  {selectedTab === 6 && renderAttributionTab()}
                </div>
              </Tabs>
            </div>
          )}
        </div>
      </div>
    </AppProvider>
  );
}

// --------------------------------------------------------------------
// Root App
// --------------------------------------------------------------------
export default function App() {
  return (
    <AuthGate>
      <AppContent />
    </AuthGate>
  );
}
