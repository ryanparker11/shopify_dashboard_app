import { useState, useEffect } from 'react';
import Plot from 'react-plotly.js';
import type { Data, Layout } from 'plotly.js';
import {
  Card,
  Layout as PolarisLayout,
  Page,
  Text,
  BlockStack,
  InlineGrid,
  Select,
  Badge,
  DataTable,
  Banner
} from '@shopify/polaris';
import { authenticatedFetch } from '../lib/api';

// Type definitions
interface AttributionPageProps {
  shopDomain?: string;
}

interface ChannelMetrics {
  channel: string;
  orders: number;
  revenue: number;
  aov: number;
  new_customers: number;
  repeat_customers: number;
}

interface AttributionOverview {
  channels: ChannelMetrics[];
  date_range: {
    start: string;
    end: string;
    days: number;
  };
}

interface CampaignMetrics {
  campaign: string;
  source: string | null;
  medium: string | null;
  orders: number;
  revenue: number;
  avg_order_value: number;
}

interface CampaignAttribution {
  campaigns: CampaignMetrics[];
  total_campaigns: number;
  date_range: {
    start: string;
    end: string;
    days: number;
  };
}

interface TrendDataPoint {
  date: string;
  orders: number;
  revenue: number;
}

interface ChannelTrendSeries {
  channel: string;
  data: TrendDataPoint[];
}

interface AttributionTrend {
  series: ChannelTrendSeries[];
  group_by: string;
  date_range: {
    start: string;
    end: string;
    days: number;
  };
}

interface CustomerTypeMetrics {
  orders: number;
  revenue: number;
  percentage: number;
}

interface ChannelCustomerSplit {
  channel: string;
  new_customers: CustomerTypeMetrics;
  repeat_customers: CustomerTypeMetrics;
}

interface CustomerSplitData {
  channels: ChannelCustomerSplit[];
  date_range: {
    start: string;
    end: string;
    days: number;
  };
}

// Color palette for channels (consistent across charts)
const CHANNEL_COLORS: { [key: string]: string } = {
  'Google': '#EA4335',
  'Google Ads': '#EA4335',
  'Google Organic': '#34A853',
  'Facebook': '#1877F2',
  'Instagram': '#E4405F',
  'TikTok': '#000000',
  'Email': '#5C6AC4',
  'Twitter': '#1DA1F2',
  'Pinterest': '#E60023',
  'YouTube': '#FF0000',
  'LinkedIn': '#0A66C2',
  'Direct': '#637381',
  'Draft Order': '#919EAB',
  'Point of Sale': '#50B83C',
  'Referral': '#AE62FF'
};

function getChannelColor(channel: string, index: number): string {
  return CHANNEL_COLORS[channel] || `hsl(${(index * 360) / 12}, 70%, 50%)`;
}

export function AttributionPage({ shopDomain: shopDomainProp }: AttributionPageProps = {}) {
  const [overview, setOverview] = useState<AttributionOverview | null>(null);
  const [campaigns, setCampaigns] = useState<CampaignAttribution | null>(null);
  const [trend, setTrend] = useState<AttributionTrend | null>(null);
  const [customerSplit, setCustomerSplit] = useState<CustomerSplitData | null>(null);
  const [loading, setLoading] = useState(true);
  const [days, setDays] = useState('30');
  const [trendGroupBy, setTrendGroupBy] = useState('day');

  const shopDomain = shopDomainProp || new URLSearchParams(window.location.search).get('shop');

  useEffect(() => {
    if (shopDomain) loadAttribution();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [shopDomain, days, trendGroupBy]);

  const loadAttribution = async () => {
    setLoading(true);
    try {
      const [overviewData, campaignsData, trendData, splitData] = await Promise.all([
        authenticatedFetch<AttributionOverview>(`/api/attribution/overview?days=${days}`),
        authenticatedFetch<CampaignAttribution>(`/api/attribution/campaigns?days=${days}&limit=10`),
        authenticatedFetch<AttributionTrend>(`/api/attribution/trend?days=${days}&group_by=${trendGroupBy}`),
        authenticatedFetch<CustomerSplitData>(`/api/attribution/customer-split?days=${days}`)
      ]);

      setOverview(overviewData);
      setCampaigns(campaignsData);
      setTrend(trendData);
      setCustomerSplit(splitData);
    } catch (error) {
      console.error('Error loading attribution:', error);
    } finally {
      setLoading(false);
    }
  };

  // -----------------------
  // Revenue by Channel (Pie Chart)
  // -----------------------
  const revenueByChannelChart = overview ? {
    data: [
      {
        type: 'pie' as const,
        labels: overview.channels.map(c => c.channel),
        values: overview.channels.map(c => c.revenue),
        marker: {
          colors: overview.channels.map((c, i) => getChannelColor(c.channel, i))
        },
        textinfo: 'label+percent' as const,
        hovertemplate: '<b>%{label}</b><br>Revenue: $%{value:,.2f}<br>%{percent}<extra></extra>'
      }
    ] as Data[],
    layout: {
      title: { text: 'Revenue by Channel' },
      showlegend: true,
      margin: { t: 40, r: 20, b: 20, l: 20 }
    } as Partial<Layout>
  } : null;

  // -----------------------
  // Attribution Trend (Line Chart)
  // -----------------------
  const attributionTrendChart = trend && trend.series.length > 0 ? {
    data: trend.series.map((series, index) => ({
      x: series.data.map(d => d.date),
      y: series.data.map(d => d.revenue),
      type: 'scatter' as const,
      mode: 'lines' as const,
      name: series.channel,
      line: { 
        color: getChannelColor(series.channel, index),
        width: 2
      },
      hovertemplate: '<b>%{fullData.name}</b><br>Date: %{x}<br>Revenue: $%{y:,.2f}<extra></extra>'
    })) as Data[],
    layout: {
      title: { text: 'Channel Revenue Trend' },
      xaxis: { title: { text: 'Date' } },
      yaxis: { title: { text: 'Revenue ($)' } },
      hovermode: 'x unified' as const,
      showlegend: true,
      margin: { t: 40, r: 20, b: 60, l: 60 },
      legend: {
        orientation: 'h' as const,
        y: -0.2,
        x: 0.5,
        xanchor: 'center' as const
      }
    } as Partial<Layout>
  } : null;

  // -----------------------
  // New vs Repeat by Channel (Stacked Bar Chart)
  // -----------------------
  const customerSplitChart = customerSplit ? {
    data: [
      {
        x: customerSplit.channels.map(c => c.channel),
        y: customerSplit.channels.map(c => c.new_customers.revenue),
        type: 'bar' as const,
        name: 'New Customers',
        marker: { color: '#50B83C' },
        hovertemplate: '<b>%{x}</b><br>New Customers: $%{y:,.2f}<extra></extra>'
      },
      {
        x: customerSplit.channels.map(c => c.channel),
        y: customerSplit.channels.map(c => c.repeat_customers.revenue),
        type: 'bar' as const,
        name: 'Repeat Customers',
        marker: { color: '#5C6AC4' },
        hovertemplate: '<b>%{x}</b><br>Repeat Customers: $%{y:,.2f}<extra></extra>'
      }
    ] as Data[],
    layout: {
      title: { text: 'New vs Repeat Customer Revenue' },
      xaxis: { title: { text: 'Channel' } },
      yaxis: { title: { text: 'Revenue ($)' } },
      barmode: 'stack' as const,
      hovermode: 'x unified' as const,
      showlegend: true,
      margin: { t: 40, r: 20, b: 80, l: 60 },
      xaxis_tickangle: -45
    } as Partial<Layout>
  } : null;

  // -----------------------
  // Channel Performance Table
  // -----------------------
  const channelRows = overview?.channels.map(channel => {
    const newPercentage = ((channel.new_customers / (channel.new_customers + channel.repeat_customers)) * 100).toFixed(0);
    return [
      channel.channel,
      channel.orders.toString(),
      `$${channel.revenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      `$${channel.aov.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      channel.new_customers.toString(),
      channel.repeat_customers.toString(),
      <Badge tone="info">{`${newPercentage}% New`}</Badge>
    ];
  }) || [];

  // -----------------------
  // Campaign Performance Table
  // -----------------------
  const campaignRows = campaigns?.campaigns.map(campaign => [
    campaign.campaign,
    campaign.source || '-',
    campaign.medium || '-',
    campaign.orders.toString(),
    `$${campaign.revenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
    `$${campaign.avg_order_value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
  ]) || [];

  // -----------------------
  // Summary Stats
  // -----------------------
  const totalRevenue = overview?.channels.reduce((sum, c) => sum + c.revenue, 0) || 0;
  const totalOrders = overview?.channels.reduce((sum, c) => sum + c.orders, 0) || 0;
  const totalNewCustomers = overview?.channels.reduce((sum, c) => sum + c.new_customers, 0) || 0;
  const avgAOV = totalOrders > 0 ? totalRevenue / totalOrders : 0;

  // Top channel
  const topChannel = overview?.channels[0];

  return (
    <Page title="Marketing Attribution">
      <PolarisLayout>
        <PolarisLayout.Section>
          <BlockStack gap="400">
            {/* Info Banner */}
            <Banner tone="info">
              <Text as="p">
                Attribution data is sourced from Shopify's built-in tracking (UTM parameters, source_name, referring_site). 
                For 70-80% of orders, this provides accurate channel attribution without needing ad platform integrations.
              </Text>
            </Banner>

            {/* Controls */}
            <Card>
              <InlineGrid columns={2} gap="400">
                <Select
                  label="Date Range"
                  options={[
                    { label: '7 Days', value: '7' },
                    { label: '14 Days', value: '14' },
                    { label: '30 Days', value: '30' },
                    { label: '60 Days', value: '60' },
                    { label: '90 Days', value: '90' }
                  ]}
                  value={days}
                  onChange={setDays}
                />
                <Select
                  label="Trend View"
                  options={[
                    { label: 'Daily', value: 'day' },
                    { label: 'Weekly', value: 'week' }
                  ]}
                  value={trendGroupBy}
                  onChange={setTrendGroupBy}
                />
              </InlineGrid>
            </Card>

            {loading ? (
              <Card>
                <Text as="p">Loading attribution data...</Text>
              </Card>
            ) : (
              <>
                {/* Summary Stats */}
                {overview && overview.channels.length > 0 ? (
                  <>
                    <Card>
                      <BlockStack gap="400">
                        <Text as="h2" variant="headingMd">Overview</Text>
                        <InlineGrid columns={5} gap="400">
                          <Card>
                            <BlockStack gap="200">
                              <Text as="p" variant="bodyMd" tone="subdued">Total Revenue</Text>
                              <Text as="p" variant="headingLg">
                                ${totalRevenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                              </Text>
                            </BlockStack>
                          </Card>

                          <Card>
                            <BlockStack gap="200">
                              <Text as="p" variant="bodyMd" tone="subdued">Total Orders</Text>
                              <Text as="p" variant="headingLg">
                                {totalOrders.toLocaleString('en-US')}
                              </Text>
                            </BlockStack>
                          </Card>

                          <Card>
                            <BlockStack gap="200">
                              <Text as="p" variant="bodyMd" tone="subdued">Avg Order Value</Text>
                              <Text as="p" variant="headingLg">
                                ${avgAOV.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                              </Text>
                            </BlockStack>
                          </Card>

                          <Card>
                            <BlockStack gap="200">
                              <Text as="p" variant="bodyMd" tone="subdued">New Customers</Text>
                              <Text as="p" variant="headingLg" tone="success">
                                {totalNewCustomers.toLocaleString('en-US')}
                              </Text>
                            </BlockStack>
                          </Card>

                          <Card>
                            <BlockStack gap="200">
                              <Text as="p" variant="bodyMd" tone="subdued">Top Channel</Text>
                              <Text as="p" variant="headingLg">
                                {topChannel?.channel || 'N/A'}
                              </Text>
                            </BlockStack>
                          </Card>
                        </InlineGrid>
                      </BlockStack>
                    </Card>

                    {/* Channel Performance Table */}
                    <Card>
                      <BlockStack gap="400">
                        <Text as="h2" variant="headingMd">Channel Performance</Text>
                        <DataTable
                          columnContentTypes={['text', 'numeric', 'numeric', 'numeric', 'numeric', 'numeric', 'text']}
                          headings={['Channel', 'Orders', 'Revenue', 'AOV', 'New', 'Repeat', 'Split']}
                          rows={channelRows}
                          sortable={[false, true, true, true, true, true, false]}
                        />
                      </BlockStack>
                    </Card>

                    {/* Revenue Distribution Chart */}
                    <Card>
                      <BlockStack gap="400">
                        <Text as="h2" variant="headingMd">Revenue Distribution</Text>
                        {revenueByChannelChart && (
                          <Plot
                            data={revenueByChannelChart.data}
                            layout={revenueByChannelChart.layout}
                            config={{ responsive: true }}
                            style={{ width: '100%', height: '400px' }}
                          />
                        )}
                      </BlockStack>
                    </Card>

                    {/* Attribution Trend */}
                    {trend && trend.series.length > 0 && (
                      <Card>
                        <BlockStack gap="400">
                          <Text as="h2" variant="headingMd">Channel Revenue Trend</Text>
                          {attributionTrendChart && (
                            <Plot
                              data={attributionTrendChart.data}
                              layout={attributionTrendChart.layout}
                              config={{ responsive: true }}
                              style={{ width: '100%', height: '400px' }}
                            />
                          )}
                        </BlockStack>
                      </Card>
                    )}

                    {/* New vs Repeat Customer Split */}
                    {customerSplit && (
                      <Card>
                        <BlockStack gap="400">
                          <Text as="h2" variant="headingMd">Customer Acquisition vs Retention</Text>
                          <Banner tone="info">
                            <Text as="p">
                              Green = New customers (acquisition), Purple = Repeat customers (retention). 
                              High new customer % indicates good acquisition channels. High repeat % indicates retention channels.
                            </Text>
                          </Banner>
                          {customerSplitChart && (
                            <Plot
                              data={customerSplitChart.data}
                              layout={customerSplitChart.layout}
                              config={{ responsive: true }}
                              style={{ width: '100%', height: '400px' }}
                            />
                          )}

                          <DataTable
                            columnContentTypes={['text', 'numeric', 'numeric', 'numeric', 'numeric']}
                            headings={['Channel', 'New Orders', 'New Revenue', 'Repeat Orders', 'Repeat Revenue']}
                            rows={customerSplit.channels.map(c => [
                              c.channel,
                              c.new_customers.orders.toString(),
                              `$${c.new_customers.revenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
                              c.repeat_customers.orders.toString(),
                              `$${c.repeat_customers.revenue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                            ])}
                          />
                        </BlockStack>
                      </Card>
                    )}

                    {/* Campaign Performance */}
                    {campaigns && campaigns.campaigns.length > 0 && (
                      <Card>
                        <BlockStack gap="400">
                          <Text as="h2" variant="headingMd">Top Campaigns</Text>
                          <Banner>
                            <Text as="p">
                              Showing campaigns with UTM tracking. Total campaigns tracked: {campaigns.total_campaigns}
                            </Text>
                          </Banner>
                          <DataTable
                            columnContentTypes={['text', 'text', 'text', 'numeric', 'numeric', 'numeric']}
                            headings={['Campaign', 'Source', 'Medium', 'Orders', 'Revenue', 'AOV']}
                            rows={campaignRows}
                          />
                        </BlockStack>
                      </Card>
                    )}
                  </>
                ) : (
                  <Card>
                    <Banner tone="warning">
                      <BlockStack gap="200">
                        <Text as="p" variant="headingMd">No Attribution Data Available</Text>
                        <Text as="p">
                          We couldn't find any attribution data for the selected time period. This could mean:
                        </Text>
                        <ul>
                          <li>No orders in the selected date range</li>
                          <li>Orders don't have attribution fields (landing_site, source_name, etc.)</li>
                          <li>Try selecting a longer date range</li>
                        </ul>
                      </BlockStack>
                    </Banner>
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