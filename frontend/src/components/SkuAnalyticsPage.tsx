// components/SKUAnalyticsPage.tsx
import {
  Card,
  BlockStack,
  InlineStack,
  Text,
  Button,
  DataTable,
  Badge,
  Select,
  Spinner,
} from '@shopify/polaris';
import { useState, useEffect, useCallback } from 'react';
import { authenticatedFetch, authenticatedBlobFetch } from '../lib/api';

interface SKUData {
  product_id: number;
  variant_id: number | null;
  sku: string | null;
  product_title: string;
  variant_title: string | null;
  total_quantity: number;
  total_revenue: number;
  total_profit: number | null;
  avg_price: number;
  cogs_per_unit: number | null;
  profit_margin: number | null;
  order_count: number;
  last_order_date: string | null;
  has_cogs_data: boolean;
}

interface SKUResponse {
  skus: SKUData[];
  summary: {
    total_skus: number;
    total_revenue: number;
    total_quantity: number;
    total_profit: number | null;
    profit_data_available: boolean;
    skus_with_cogs: number;
    skus_without_cogs: number;
  };
  date_range: {
    start: string;
    end: string;
    days: number;
  };
  sort_by: string;
  limit: number;
}

type SortDirection = 'ascending' | 'descending' | 'none';

export function SKUAnalyticsPage() {
  const [skuData, setSKUData] = useState<SKUResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [sortedColumn, setSortedColumn] = useState<number | null>(2); // Default sort by revenue
  const [sortDirection, setSortDirection] = useState<SortDirection>('descending');
  
  // Filters
  const [days, setDays] = useState('30');
  const [sortBy, setSortBy] = useState('revenue');
  const [limit, setLimit] = useState('50');

  const fetchSKUData = useCallback(async () => {
    try {
      setIsLoading(true);
      console.log('🔍 Fetching SKU analytics');

      const data = await authenticatedFetch<SKUResponse>(
        `/api/sku-analytics/overview?days=${days}&limit=${limit}&sort_by=${sortBy}`
      );

      console.log('✅ SKU analytics loaded:', data.skus?.length || 0, 'SKUs');
      setSKUData(data);
    } catch (error) {
      console.error('💥 Failed to fetch SKU analytics:', error);
      setSKUData(null);
    } finally {
      setIsLoading(false);
    }
  }, [days, limit, sortBy]);

  useEffect(() => {
    fetchSKUData();
  }, [fetchSKUData]);

  const downloadSKUData = async () => {
    try {
      const url = `/api/sku-analytics/export?days=${days}&limit=${limit}&sort_by=${sortBy}`;
      console.log('Attempting to download SKU analytics from:', url);

      const blob = await authenticatedBlobFetch(url);
      console.log('Blob created, size:', blob.size, 'type:', blob.type);

      const filename = `sku_analytics_${new Date().toISOString().slice(0, 10)}.xlsx`;

      const objectUrl = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = objectUrl;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(objectUrl);

      console.log('SKU analytics download completed successfully');
    } catch (err) {
      console.error('Download error:', err);
      alert(
        `Failed to download SKU analytics: ${
          err instanceof Error ? err.message : 'Unknown error'
        }`
      );
    }
  };

  const handleSort = (index: number, direction: SortDirection) => {
    setSortedColumn(index);
    setSortDirection(direction);
  };

  const getSortedSKUs = () => {
    if (!skuData || sortedColumn === null || sortDirection === 'none') {
      return skuData?.skus || [];
    }

    const sorted = [...skuData.skus];

    sorted.sort((a, b) => {
      let aVal: number | string | null;
      let bVal: number | string | null;

      switch (sortedColumn) {
        case 0: // Product/SKU
          aVal = a.product_title;
          bVal = b.product_title;
          break;
        case 1: // Quantity
          aVal = a.total_quantity;
          bVal = b.total_quantity;
          break;
        case 2: // Revenue
          aVal = a.total_revenue;
          bVal = b.total_revenue;
          break;
        case 3: // Profit
          aVal = a.total_profit ?? -999999;
          bVal = b.total_profit ?? -999999;
          break;
        case 4: // Margin %
          aVal = a.profit_margin ?? -999999;
          bVal = b.profit_margin ?? -999999;
          break;
        case 5: // Avg Price
          aVal = a.avg_price;
          bVal = b.avg_price;
          break;
        case 6: // COGS
          aVal = a.cogs_per_unit ?? -999999;
          bVal = b.cogs_per_unit ?? -999999;
          break;
        case 7: // Orders
          aVal = a.order_count;
          bVal = b.order_count;
          break;
        case 8: // Last Order
          aVal = a.last_order_date || '';
          bVal = b.last_order_date || '';
          break;
        default:
          return 0;
      }

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

  const renderSKUTable = () => {
    if (isLoading) {
      return (
        <Card>
          <div style={{ padding: '40px', textAlign: 'center' }}>
            <Spinner size="large" />
            <Text as="p" variant="bodyMd" tone="subdued">
              Loading SKU analytics...
            </Text>
          </div>
        </Card>
      );
    }

    if (!skuData || skuData.skus.length === 0) {
      return (
        <Card>
          <Text as="p" tone="subdued">
            No SKU data available for the selected period.
          </Text>
        </Card>
      );
    }

    const sortedSKUs = getSortedSKUs();

    const headings = [
      'Product / SKU',
      'Qty Sold',
      'Revenue',
      'Profit',
      'Margin %',
      'Avg Price',
      'COGS',
      'Orders',
      'Last Order',
    ];

    const rows = sortedSKUs.map((sku) => {
      // Build product name display
      let productDisplay = sku.product_title;
      if (sku.variant_title) {
        productDisplay += ` - ${sku.variant_title}`;
      }

      return [
        <div key={`product-${sku.product_id}-${sku.variant_id}`}>
          <Text as="p" variant="bodyMd" fontWeight="semibold">
            {productDisplay}
          </Text>
          {sku.sku && (
            <Text as="p" variant="bodySm" tone="subdued">
              SKU: {sku.sku}
            </Text>
          )}
        </div>,
        sku.total_quantity.toLocaleString(),
        `$${sku.total_revenue.toLocaleString('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}`,
        sku.total_profit !== null ? (
          <div key={`profit-${sku.product_id}-${sku.variant_id}`}>
            <Text
              as="p"
              variant="bodyMd"
              tone={sku.total_profit >= 0 ? undefined : 'critical'}
            >
              ${sku.total_profit.toLocaleString('en-US', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
              })}
            </Text>
          </div>
        ) : (
          <Badge tone="info" size="small">
            No COGS
          </Badge>
        ),
        sku.profit_margin !== null ? (
          <Text
            as="p"
            variant="bodyMd"
            tone={sku.profit_margin >= 0 ? undefined : 'critical'}
          >
            {sku.profit_margin.toFixed(1)}%
          </Text>
        ) : (
          <Badge tone="info" size="small">
            N/A
          </Badge>
        ),
        `$${sku.avg_price.toLocaleString('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })}`,
        sku.cogs_per_unit !== null ? (
          `$${sku.cogs_per_unit.toLocaleString('en-US', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}`
        ) : (
          <Badge tone="info" size="small">
            Not set
          </Badge>
        ),
        sku.order_count.toLocaleString(),
        sku.last_order_date
          ? new Date(sku.last_order_date).toLocaleDateString('en-US', {
              year: 'numeric',
              month: 'short',
              day: 'numeric',
            })
          : 'Never',
      ];
    });

    return (
      <Card>
        <BlockStack gap="400">
          <InlineStack align="space-between" blockAlign="center">
            <div>
              <Text as="h2" variant="headingLg">
                SKU Performance
              </Text>
              <Text as="p" variant="bodySm" tone="subdued">
                Showing {sortedSKUs.length} of {skuData.summary.total_skus} total SKUs
              </Text>
            </div>
            <Button onClick={downloadSKUData}>Download Excel</Button>
          </InlineStack>

          {/* Summary Cards */}
          <InlineStack gap="400" wrap={true}>
            <Card>
              <BlockStack gap="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  Total Revenue
                </Text>
                <Text as="p" variant="headingMd">
                  ${skuData.summary.total_revenue.toLocaleString('en-US', {
                    minimumFractionDigits: 2,
                  })}
                </Text>
              </BlockStack>
            </Card>

            <Card>
              <BlockStack gap="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  Total Quantity
                </Text>
                <Text as="p" variant="headingMd">
                  {skuData.summary.total_quantity.toLocaleString()}
                </Text>
              </BlockStack>
            </Card>

            {skuData.summary.profit_data_available && (
              <Card>
                <BlockStack gap="200">
                  <Text as="p" variant="bodySm" tone="subdued">
                    Total Profit
                  </Text>
                  <Text as="p" variant="headingMd">
                    ${(skuData.summary.total_profit ?? 0).toLocaleString('en-US', {
                      minimumFractionDigits: 2,
                    })}
                  </Text>
                </BlockStack>
              </Card>
            )}

            <Card>
              <BlockStack gap="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  SKUs with COGS
                </Text>
                <Text as="p" variant="headingMd">
                  {skuData.summary.skus_with_cogs} / {skuData.summary.total_skus}
                </Text>
              </BlockStack>
            </Card>
          </InlineStack>

          {/* Filters */}
          <InlineStack gap="400" wrap={false}>
            <div style={{ width: '150px' }}>
              <Select
                label="Time Period"
                options={[
                  { label: 'Last 7 days', value: '7' },
                  { label: 'Last 30 days', value: '30' },
                  { label: 'Last 60 days', value: '60' },
                  { label: 'Last 90 days', value: '90' },
                  { label: 'Last 180 days', value: '180' },
                  { label: 'Last 365 days', value: '365' },
                ]}
                value={days}
                onChange={setDays}
              />
            </div>

            <div style={{ width: '150px' }}>
              <Select
                label="Sort By"
                options={[
                  { label: 'Revenue', value: 'revenue' },
                  { label: 'Quantity', value: 'quantity' },
                  { label: 'Profit', value: 'profit' },
                  { label: 'Margin %', value: 'margin' },
                ]}
                value={sortBy}
                onChange={setSortBy}
              />
            </div>

            <div style={{ width: '150px' }}>
              <Select
                label="Show Top"
                options={[
                  { label: 'Top 25', value: '25' },
                  { label: 'Top 50', value: '50' },
                  { label: 'Top 100', value: '100' },
                  { label: 'Top 200', value: '200' },
                ]}
                value={limit}
                onChange={setLimit}
              />
            </div>
          </InlineStack>

          {/* Data Table */}
          <DataTable
            columnContentTypes={[
              'text',
              'numeric',
              'numeric',
              'numeric',
              'numeric',
              'numeric',
              'numeric',
              'numeric',
              'text',
            ]}
            headings={headings}
            rows={rows}
            sortable={[true, true, true, true, true, true, true, true, true]}
            defaultSortDirection="descending"
            initialSortColumnIndex={2}
            onSort={handleSort}
          />
        </BlockStack>
      </Card>
    );
  };

  return <BlockStack gap="400">{renderSKUTable()}</BlockStack>;
}