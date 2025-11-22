import { useState, useEffect } from 'react';
import Plot from 'react-plotly.js';
import type { Data, Layout } from 'plotly.js';
import { Card, Layout as PolarisLayout, Page, Text, BlockStack, InlineGrid, Select, Badge, DataTable, Banner } from '@shopify/polaris';
import { authenticatedFetch } from '../lib/api';

// Type definitions
interface ForecastsPageProps {
  shopDomain?: string;
}

interface HistoricalDataPoint {
  date: string;
  revenue?: number;
  orders?: number;
}

interface ForecastDataPoint {
  date: string;
  forecast_revenue?: number;
  forecast_orders?: number;
  lower_bound?: number;
  upper_bound?: number;
  confidence?: string;
}

interface RevenueForecast {
  historical: HistoricalDataPoint[];
  forecast: ForecastDataPoint[];
  metrics: {
    avg_daily_revenue: number;
    daily_trend: number;
    historical_total_30d: number;
    forecast_total: number;
  };
}

interface OrderForecast {
  historical: HistoricalDataPoint[];
  forecast: ForecastDataPoint[];
  metrics: {
    avg_daily_orders: number;
    daily_trend: number;
    std_deviation: number;
    historical_total_30d: number;
    forecast_total: number;
  };
}

interface InventoryProduct {
  product_id: number;
  variant_id: number;
  product_title: string;
  variant_title: string;
  sku: string;
  current_inventory: number;
  units_sold_30d: number;
  daily_velocity: number;
  days_until_stockout: number | null;
  projected_stockout_date: string | null;
  risk_level: 'critical' | 'high' | 'medium' | 'low';
}

interface InventoryDepletion {
  products: InventoryProduct[];
  summary: {
    total_products_tracked: number;
    critical_risk: number;
    high_risk: number;
    medium_risk: number;
  };
}

interface Customer {
  customer_id: number;
  email: string;
  name: string;
  segment: 'new' | 'returning' | 'vip';
  total_orders: number;
  total_spent: number;
  avg_order_value: number;
  first_order_date: string | null;
  last_order_date: string | null;
  customer_lifespan_days: number;
  monthly_order_frequency: number;
  predicted_clv: number;
  churn_risk: 'high' | 'medium' | 'low';
  days_since_last_order: number;
}

interface CustomerCLV {
  customers: Customer[];
  summary: {
    total_customers: number;
    avg_customer_lifetime_value: number;
    total_predicted_value: number;
    high_churn_risk: number;
    segment_breakdown: {
      [key: string]: {
        count: number;
        avg_clv: number;
        total_value: number;
      };
    };
  };
}

export function ForecastsPage({ shopDomain: shopDomainProp }: ForecastsPageProps = {}) {
  const [revenueForecast, setRevenueForecast] = useState<RevenueForecast | null>(null);
  const [orderForecast, setOrderForecast] = useState<OrderForecast | null>(null);
  const [inventoryDepletion, setInventoryDepletion] = useState<InventoryDepletion | null>(null);
  const [customerCLV, setCustomerCLV] = useState<CustomerCLV | null>(null);
  const [loading, setLoading] = useState(true);
  const [forecastDays, setForecastDays] = useState('30');
  const [clvSegment, setClvSegment] = useState('all');

  const shopDomain = shopDomainProp || new URLSearchParams(window.location.search).get('shop');

  useEffect(() => {
    if (shopDomain) {
      loadForecasts();
    }
  }, [shopDomain, forecastDays, clvSegment]);

  const loadForecasts = async () => {
    setLoading(true);
    try {
      // Load all forecasts in parallel with authenticated fetch
      // Note: shop_domain is extracted from session token on backend, not sent in URL
      const [revenue, orders, inventory, clv] = await Promise.all([
        authenticatedFetch<RevenueForecast>(`/api/forecasts/revenue?days=${forecastDays}`),
        authenticatedFetch<OrderForecast>(`/api/forecasts/orders?days=${forecastDays}`),
        authenticatedFetch<InventoryDepletion>(`/api/forecasts/inventory-depletion`),
        authenticatedFetch<CustomerCLV>(`/api/forecasts/customer-lifetime-value${clvSegment !== 'all' ? `?segment=${clvSegment}` : ''}`)
      ]);

      setRevenueForecast(revenue);
      setOrderForecast(orders);
      setInventoryDepletion(inventory);
      setCustomerCLV(clv);
    } catch (error) {
      console.error('Error loading forecasts:', error);
    } finally {
      setLoading(false);
    }
  };

  const revenueForecastChart = revenueForecast ? {
    data: [
      {
        x: revenueForecast.historical?.map(d => d.date) || [],
        y: revenueForecast.historical?.map(d => d.revenue || 0) || [],
        type: 'scatter' as const,
        mode: 'lines' as const,
        name: 'Historical Revenue',
        line: { color: '#008060', width: 2 }
      },
      {
        x: revenueForecast.forecast?.map(d => d.date) || [],
        y: revenueForecast.forecast?.map(d => d.forecast_revenue || 0) || [],
        type: 'scatter' as const,
        mode: 'lines' as const,
        name: 'Forecast',
        line: { color: '#5C6AC4', dash: 'dash', width: 2 }
      }
    ] as Data[],
    layout: {
      title: { text: 'Revenue Forecast' },
      xaxis: { title: { text: 'Date' } },
      yaxis: { title: { text: 'Revenue ($)' } },
      hovermode: 'x unified' as const,
      showlegend: true,
      margin: { t: 40, r: 20, b: 40, l: 60 }
    } as Partial<Layout>
  } : null;

  const orderForecastChart = orderForecast ? {
    data: [
      {
        x: orderForecast.historical?.map(d => d.date) || [],
        y: orderForecast.historical?.map(d => d.orders || 0) || [],
        type: 'scatter' as const,
        mode: 'lines' as const,
        name: 'Historical Orders',
        line: { color: '#008060', width: 2 }
      },
      {
        x: orderForecast.forecast?.map(d => d.date) || [],
        y: orderForecast.forecast?.map(d => d.forecast_orders || 0) || [],
        type: 'scatter' as const,
        mode: 'lines' as const,
        name: 'Forecast Orders',
        line: { color: '#5C6AC4', dash: 'dash', width: 2 }
      },
      {
        x: orderForecast.forecast?.map(d => d.date) || [],
        y: orderForecast.forecast?.map(d => d.upper_bound || 0) || [],
        type: 'scatter' as const,
        mode: 'lines' as const,
        name: 'Upper Bound',
        line: { color: '#E3E3E3', width: 1, dash: 'dot' },
        showlegend: false
      },
      {
        x: orderForecast.forecast?.map(d => d.date) || [],
        y: orderForecast.forecast?.map(d => d.lower_bound || 0) || [],
        type: 'scatter' as const,
        mode: 'lines' as const,
        name: 'Lower Bound',
        fill: 'tonexty' as const,
        fillcolor: 'rgba(92, 106, 196, 0.1)',
        line: { color: '#E3E3E3', width: 1, dash: 'dot' },
        showlegend: false
      }
    ] as Data[],
    layout: {
      title: { text: 'Order Volume Forecast' },
      xaxis: { title: { text: 'Date' } },
      yaxis: { title: { text: 'Number of Orders' } },
      hovermode: 'x unified' as const,
      showlegend: true,
      margin: { t: 40, r: 20, b: 40, l: 60 }
    } as Partial<Layout>
  } : null;

  // Inventory depletion table rows
  const inventoryRows = inventoryDepletion?.products
    .filter(p => p.days_until_stockout !== null && p.days_until_stockout <= 180)
    .slice(0, 10)
    .map(product => [
      product.product_title,
      product.variant_title || 'Default',
      product.current_inventory.toString(),
      product.units_sold_30d.toString(),
      product.days_until_stockout ? Math.round(product.days_until_stockout).toString() : 'N/A',
      <Badge tone={
        product.risk_level === 'critical' ? 'critical' :
        product.risk_level === 'high' ? 'warning' :
        product.risk_level === 'medium' ? 'attention' : 'info'
      }>
        {product.risk_level.toUpperCase()}
      </Badge>
    ]) || [];

  // Customer CLV table rows
  const clvRows = customerCLV?.customers.slice(0, 10).map(customer => [
    customer.name,
    customer.email,
    <Badge tone={
      customer.segment === 'vip' ? 'success' :
      customer.segment === 'returning' ? 'info' : 'attention'
    }>
      {customer.segment.toUpperCase()}
    </Badge>,
    customer.total_orders.toString(),
    `$${customer.total_spent.toFixed(2)}`,
    `$${customer.predicted_clv.toFixed(2)}`,
    <Badge tone={
      customer.churn_risk === 'high' ? 'critical' :
      customer.churn_risk === 'medium' ? 'warning' : 'success'
    }>
      {customer.churn_risk.toUpperCase()}
    </Badge>
  ]) || [];

  return (
    <Page title="Forecasts & Predictions">
      <PolarisLayout>
        <PolarisLayout.Section>
          <BlockStack gap="400">
            {/* Controls */}
            <Card>
              <InlineGrid columns={2} gap="400">
                <Select
                  label="Forecast Period"
                  options={[
                    { label: '7 Days', value: '7' },
                    { label: '14 Days', value: '14' },
                    { label: '30 Days', value: '30' },
                    { label: '60 Days', value: '60' },
                    { label: '90 Days', value: '90' }
                  ]}
                  value={forecastDays}
                  onChange={setForecastDays}
                />
                <Select
                  label="Customer Segment"
                  options={[
                    { label: 'All Customers', value: 'all' },
                    { label: 'New Customers', value: 'new' },
                    { label: 'Returning Customers', value: 'returning' },
                    { label: 'VIP Customers', value: 'vip' }
                  ]}
                  value={clvSegment}
                  onChange={setClvSegment}
                />
              </InlineGrid>
            </Card>

            {loading ? (
              <Card>
                <Text as="p">Loading forecasts...</Text>
              </Card>
            ) : (
              <>
                {/* Revenue Forecast */}
                {revenueForecast && (
                  <Card>
                    <BlockStack gap="400">
                      <Text as="h2" variant="headingMd">Revenue Forecast</Text>
                      {revenueForecastChart && (
                        <Plot
                          data={revenueForecastChart.data}
                          layout={revenueForecastChart.layout}
                          config={{ responsive: true }}
                          style={{ width: '100%', height: '400px' }}
                        />
                      )}
                      <InlineGrid columns={5} gap="400">
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Avg Daily Revenue</Text>
                            <Text as="p" variant="headingLg">
                              ${revenueForecast.metrics.avg_daily_revenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Daily Trend</Text>
                            <Text as="p" variant="headingLg" tone={revenueForecast.metrics.daily_trend >= 0 ? 'success' : 'critical'}>
                              {revenueForecast.metrics.daily_trend >= 0 ? '+' : ''}${revenueForecast.metrics.daily_trend.toFixed(2)}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Historical Total (30d)</Text>
                            <Text as="p" variant="headingLg">
                              ${revenueForecast.metrics.historical_total_30d.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Forecast Total</Text>
                            <Text as="p" variant="headingLg">
                              ${revenueForecast.metrics.forecast_total.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Combined Total</Text>
                            <Text as="p" variant="headingLg" tone="success">
                              ${(revenueForecast.metrics.historical_total_30d + revenueForecast.metrics.forecast_total).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </Text>
                          </BlockStack>
                        </Card>
                      </InlineGrid>
                    </BlockStack>
                  </Card>
                )}

                {/* Order Forecast */}
                {orderForecast && (
                  <Card>
                    <BlockStack gap="400">
                      <Text as="h2" variant="headingMd">Order Volume Forecast</Text>
                      {orderForecastChart && (
                        <Plot
                          data={orderForecastChart.data}
                          layout={orderForecastChart.layout}
                          config={{ responsive: true }}
                          style={{ width: '100%', height: '400px' }}
                        />
                      )}
                      <InlineGrid columns={5} gap="400">
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Avg Daily Orders</Text>
                            <Text as="p" variant="headingLg">
                              {orderForecast.metrics.avg_daily_orders.toFixed(1)}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Daily Trend</Text>
                            <Text as="p" variant="headingLg" tone={orderForecast.metrics.daily_trend >= 0 ? 'success' : 'critical'}>
                              {orderForecast.metrics.daily_trend >= 0 ? '+' : ''}{orderForecast.metrics.daily_trend.toFixed(2)}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Historical Total (30d)</Text>
                            <Text as="p" variant="headingLg">
                              {orderForecast.metrics.historical_total_30d}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Forecast Total</Text>
                            <Text as="p" variant="headingLg">
                              {orderForecast.metrics.forecast_total}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Combined Total</Text>
                            <Text as="p" variant="headingLg" tone="success">
                              {orderForecast.metrics.historical_total_30d + orderForecast.metrics.forecast_total}
                            </Text>
                          </BlockStack>
                        </Card>
                      </InlineGrid>
                    </BlockStack>
                  </Card>
                )}

                {/* Inventory Depletion Alerts */}
                {inventoryDepletion && (
                  <Card>
                    <BlockStack gap="400">
                      <Text as="h2" variant="headingMd">Inventory Depletion Alerts</Text>
                      {inventoryDepletion.products.length === 0 ? (
                        <Banner tone="info">
                          <Text as="p">No products with recent sales activity</Text>
                        </Banner>
                      ) : inventoryRows.length === 0 ? (
                        <Banner tone="success">
                          <Text as="p">All products have sufficient inventory (180+ days supply)</Text>
                        </Banner>
                      ) : (
                        <>
                          {inventoryDepletion.summary.critical_risk > 0 && (
                            <Banner tone="critical">
                              <Text as="p">
                                {inventoryDepletion.summary.critical_risk} product{inventoryDepletion.summary.critical_risk !== 1 ? 's' : ''} at critical risk of stockout within 7 days
                              </Text>
                            </Banner>
                          )}
                          <DataTable
                            columnContentTypes={['text', 'text', 'numeric', 'numeric', 'numeric', 'text']}
                            headings={['Product', 'Variant', 'Stock', '30d Sales', 'Days Left', 'Risk']}
                            rows={inventoryRows}
                          />
                        </>
                      )}
                    </BlockStack>
                  </Card>
                )}

                {/* Customer Lifetime Value */}
                {customerCLV && customerCLV.customers.length > 0 && (
                  <Card>
                    <BlockStack gap="400">
                      <Text as="h2" variant="headingMd">Customer Lifetime Value Predictions</Text>
                      <InlineGrid columns={3} gap="400">
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Avg CLV</Text>
                            <Text as="p" variant="headingLg">
                              ${customerCLV.summary.avg_customer_lifetime_value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">Total Predicted Value</Text>
                            <Text as="p" variant="headingLg">
                              ${customerCLV.summary.total_predicted_value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </Text>
                          </BlockStack>
                        </Card>
                        <Card>
                          <BlockStack gap="200">
                            <Text as="p" variant="bodyMd" tone="subdued">High Churn Risk</Text>
                            <Text as="p" variant="headingLg" tone="critical">
                              {customerCLV.summary.high_churn_risk}
                            </Text>
                          </BlockStack>
                        </Card>
                      </InlineGrid>
                      <DataTable
                        columnContentTypes={['text', 'text', 'text', 'numeric', 'numeric', 'numeric', 'text']}
                        headings={['Customer', 'Email', 'Segment', 'Orders', 'Total Spent', 'Predicted CLV', 'Churn Risk']}
                        rows={clvRows}
                      />
                    </BlockStack>
                  </Card>
                )}
              </>
            )}
          </BlockStack>
        </PolarisLayout.Section>
      </PolarisLayout>
    </Page>
  );
}