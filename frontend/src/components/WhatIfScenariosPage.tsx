// components/WhatIfScenariosPage.tsx
import {
  Card,
  BlockStack,
  InlineStack,
  Text,
  Button,
  Spinner,
  RangeSlider,
  Select,
  Badge,
  Banner,
} from '@shopify/polaris';
import { useState, useEffect, useCallback } from 'react';
import Plot from 'react-plotly.js';
import { authenticatedFetch } from '../lib/api';

interface BaselineMetrics {
  period: {
    days: number;
    start_date: string;
    end_date: string;
  };
  totals: {
    revenue: number;
    orders: number;
    cogs: number;
    profit: number;
    profit_margin: number;
  };
  averages: {
    daily_revenue: number;
    daily_orders: number;
    order_value: number;
    daily_cogs: number;
  };
  volatility: {
    revenue_std_dev: number;
    order_std_dev: number;
    aov_std_dev: number;
    revenue_coefficient_of_variation: number;
  };
  trends: {
    revenue_growth_rate: number;
  };
}

interface WhatIfVariables {
  revenue_growth: number;
  aov_change: number;
  order_volume_change: number;
  cogs_change: number;
  conversion_rate_change: number;
}

interface SimulationResults {
  simulation_id: string;
  inputs: {
    base_period_days: number;
    forecast_days: number;
    simulations: number;
    variables: WhatIfVariables;
  };
  baseline: {
    daily_revenue: number;
    daily_orders: number;
    average_order_value: number;
    cogs_rate: number;
  };
  results: {
    revenue: {
      mean: number;
      median: number;
      std_dev: number;
      percentile_5: number;
      percentile_95: number;
      confidence_90: [number, number];
      histogram: {
        bins: number[];
        frequencies: number[];
        bin_centers: number[];
      };
    };
    profit: {
      mean: number;
      median: number;
      std_dev: number;
      percentile_5: number;
      percentile_95: number;
      confidence_90: [number, number];
      probability_positive: number;
      histogram: {
        bins: number[];
        frequencies: number[];
        bin_centers: number[];
      };
    };
    orders: {
      mean: number;
      median: number;
    };
    profit_margin: {
      mean: number;
      median: number;
    };
  };
  sensitivity: {
    [key: string]: number;
  };
  insights: string[];
}

interface Preset {
  name: string;
  description: string;
  icon: string;
  variables: WhatIfVariables;
}

export function WhatIfScenariosPage() {
  const [baseline, setBaseline] = useState<BaselineMetrics | null>(null);
  const [simulation, setSimulation] = useState<SimulationResults | null>(null);
  const [presets, setPresets] = useState<Preset[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isSimulating, setIsSimulating] = useState(false);

  // Simulation parameters
  const [basePeriodDays, setBasePeriodDays] = useState('90');
  const [forecastDays, setForecastDays] = useState('30');
  const [numSimulations, setNumSimulations] = useState('10000');

  // What-if variables (as percentages for display, converted to decimals for API)
  const [revenueGrowth, setRevenueGrowth] = useState(0);
  const [aovChange, setAovChange] = useState(0);
  const [orderVolumeChange, setOrderVolumeChange] = useState(0);
  const [cogsChange, setCogsChange] = useState(0);
  const [conversionRateChange, setConversionRateChange] = useState(0);

  // Fetch baseline metrics
  const fetchBaseline = useCallback(async () => {
    try {
      setIsLoading(true);
      console.log('🔍 Fetching baseline metrics');

      const data = await authenticatedFetch<BaselineMetrics>(
        `/api/what-if/baseline?days=${basePeriodDays}`
      );

      console.log('✅ Baseline loaded successfully');
      setBaseline(data);
    } catch (error) {
      console.error('💥 Failed to fetch baseline:', error);
      setBaseline(null);
    } finally {
      setIsLoading(false);
    }
  }, [basePeriodDays]);

  // Fetch presets
  const fetchPresets = useCallback(async () => {
    try {
      console.log('🔍 Fetching preset scenarios');

      const data = await authenticatedFetch<{ presets: Preset[] }>(
        `/api/what-if/presets`
      );

      console.log('✅ Presets loaded successfully');
      setPresets(data.presets);
    } catch (error) {
      console.error('💥 Failed to fetch presets:', error);
    }
  }, []);

  // Run simulation
  const runSimulation = async () => {
    try {
      setIsSimulating(true);
      console.log('🎲 Running Monte Carlo simulation');

      const data = await authenticatedFetch<SimulationResults>(
        `/api/what-if/simulate`,
        {
          method: 'POST',
          body: JSON.stringify({
            base_period_days: parseInt(basePeriodDays),
            forecast_days: parseInt(forecastDays),
            simulations: parseInt(numSimulations),
            variables: {
              revenue_growth: revenueGrowth / 100,
              aov_change: aovChange / 100,
              order_volume_change: orderVolumeChange / 100,
              cogs_change: cogsChange / 100,
              conversion_rate_change: conversionRateChange / 100,
            },
          }),
        }
      );

      console.log('✅ Simulation completed successfully');
      setSimulation(data);
    } catch (error) {
      console.error('💥 Simulation failed:', error);
      alert(
        `Failed to run simulation: ${
          error instanceof Error ? error.message : 'Unknown error'
        }`
      );
    } finally {
      setIsSimulating(false);
    }
  };

  // Load a preset scenario
  const loadPreset = (preset: Preset) => {
    setRevenueGrowth(preset.variables.revenue_growth * 100);
    setAovChange(preset.variables.aov_change * 100);
    setOrderVolumeChange(preset.variables.order_volume_change * 100);
    setCogsChange(preset.variables.cogs_change * 100);
    setConversionRateChange(preset.variables.conversion_rate_change * 100);
  };

  // Reset to baseline
  const resetToBaseline = () => {
    setRevenueGrowth(0);
    setAovChange(0);
    setOrderVolumeChange(0);
    setCogsChange(0);
    setConversionRateChange(0);
  };

  useEffect(() => {
    fetchBaseline();
    fetchPresets();
  }, [fetchBaseline, fetchPresets]);

  // Render loading state
  if (isLoading) {
    return (
      <Card>
        <div style={{ padding: '40px', textAlign: 'center' }}>
          <Spinner size="large" />
          <Text as="p" variant="bodyMd" tone="subdued">
            Loading baseline metrics...
          </Text>
        </div>
      </Card>
    );
  }

  if (!baseline) {
    return (
      <Card>
        <Text as="p" tone="critical">
          Failed to load baseline data. Please try again.
        </Text>
      </Card>
    );
  }

  return (
    <BlockStack gap="400">
      {/* Header */}
      <Card>
        <BlockStack gap="400">
          <InlineStack align="space-between" blockAlign="center">
            <div>
              <Text as="h2" variant="headingLg">
                What If Scenarios
              </Text>
              <Text as="p" variant="bodySm" tone="subdued">
                Monte Carlo simulation to explore possible futures
              </Text>
            </div>
            <InlineStack gap="200">
              <Button onClick={resetToBaseline}>Reset</Button>
              <Button
                variant="primary"
                onClick={runSimulation}
                loading={isSimulating}
              >
                Run Simulation
              </Button>
            </InlineStack>
          </InlineStack>
        </BlockStack>
      </Card>

      {/* Baseline Metrics */}
      <Card>
        <BlockStack gap="400">
          <Text as="h3" variant="headingMd">
            📊 Current Performance (Last {baseline.period.days} days)
          </Text>

          <InlineStack gap="400" wrap={true}>
            <Card>
              <BlockStack gap="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  Daily Revenue
                </Text>
                <Text as="p" variant="headingMd">
                  ${baseline.averages.daily_revenue.toLocaleString('en-US', {
                    minimumFractionDigits: 2,
                  })}
                </Text>
              </BlockStack>
            </Card>

            <Card>
              <BlockStack gap="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  Daily Orders
                </Text>
                <Text as="p" variant="headingMd">
                  {baseline.averages.daily_orders.toFixed(1)}
                </Text>
              </BlockStack>
            </Card>

            <Card>
              <BlockStack gap="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  Avg Order Value
                </Text>
                <Text as="p" variant="headingMd">
                  ${baseline.averages.order_value.toLocaleString('en-US', {
                    minimumFractionDigits: 2,
                  })}
                </Text>
              </BlockStack>
            </Card>

            <Card>
              <BlockStack gap="200">
                <Text as="p" variant="bodySm" tone="subdued">
                  Profit Margin
                </Text>
                <Text as="p" variant="headingMd">
                  {baseline.totals.profit_margin.toFixed(1)}%
                </Text>
              </BlockStack>
            </Card>
          </InlineStack>
        </BlockStack>
      </Card>

      {/* Simulation Parameters */}
      <Card>
        <BlockStack gap="400">
          <Text as="h3" variant="headingMd">
            ⚙️ Simulation Settings
          </Text>

          <InlineStack gap="400" wrap={false}>
            <div style={{ width: '200px' }}>
              <Select
                label="Base Period"
                options={[
                  { label: 'Last 30 days', value: '30' },
                  { label: 'Last 60 days', value: '60' },
                  { label: 'Last 90 days', value: '90' },
                  { label: 'Last 180 days', value: '180' },
                  { label: 'Last 365 days', value: '365' },
                ]}
                value={basePeriodDays}
                onChange={setBasePeriodDays}
              />
            </div>

            <div style={{ width: '200px' }}>
              <Select
                label="Forecast Period"
                options={[
                  { label: '7 days', value: '7' },
                  { label: '14 days', value: '14' },
                  { label: '30 days', value: '30' },
                  { label: '60 days', value: '60' },
                  { label: '90 days', value: '90' },
                ]}
                value={forecastDays}
                onChange={setForecastDays}
              />
            </div>

            <div style={{ width: '200px' }}>
              <Select
                label="Simulations"
                options={[
                  { label: '1,000 (Fast)', value: '1000' },
                  { label: '5,000', value: '5000' },
                  { label: '10,000 (Recommended)', value: '10000' },
                  { label: '25,000', value: '25000' },
                  { label: '50,000 (Slow)', value: '50000' },
                ]}
                value={numSimulations}
                onChange={setNumSimulations}
              />
            </div>
          </InlineStack>
        </BlockStack>
      </Card>

      {/* Preset Scenarios */}
      <Card>
        <BlockStack gap="400">
          <Text as="h3" variant="headingMd">
            🎯 Quick Scenarios
          </Text>

          <InlineStack gap="300" wrap={true}>
            {presets.map((preset) => (
              <Button key={preset.name} onClick={() => loadPreset(preset)}>
                {preset.icon} {preset.name}
              </Button>
            ))}
          </InlineStack>
        </BlockStack>
      </Card>

      {/* What-If Variables */}
      <Card>
        <BlockStack gap="400">
          <Text as="h3" variant="headingMd">
            🎛️ What If Variables
          </Text>

          <BlockStack gap="400">
            {/* Revenue Growth */}
            <div>
              <Text as="p" variant="bodyMd" fontWeight="semibold">
                Revenue Growth
              </Text>
              <Text as="p" variant="bodySm" tone="subdued">
                Current: {revenueGrowth >= 0 ? '+' : ''}
                {revenueGrowth.toFixed(0)}%
              </Text>
              <RangeSlider
                label=""
                value={revenueGrowth}
                onChange={(value) => setRevenueGrowth(value as number)}
                min={-50}
                max={100}
                step={1}
                output
              />
            </div>

            {/* AOV Change */}
            <div>
              <Text as="p" variant="bodyMd" fontWeight="semibold">
                Average Order Value
              </Text>
              <Text as="p" variant="bodySm" tone="subdued">
                Current: {aovChange >= 0 ? '+' : ''}
                {aovChange.toFixed(0)}%
              </Text>
              <RangeSlider
                label=""
                value={aovChange}
                onChange={(value) => setAovChange(value as number)}
                min={-50}
                max={50}
                step={1}
                output
              />
            </div>

            {/* Order Volume */}
            <div>
              <Text as="p" variant="bodyMd" fontWeight="semibold">
                Order Volume
              </Text>
              <Text as="p" variant="bodySm" tone="subdued">
                Current: {orderVolumeChange >= 0 ? '+' : ''}
                {orderVolumeChange.toFixed(0)}%
              </Text>
              <RangeSlider
                label=""
                value={orderVolumeChange}
                onChange={(value) => setOrderVolumeChange(value as number)}
                min={-50}
                max={100}
                step={1}
                output
              />
            </div>

            {/* COGS Change */}
            <div>
              <Text as="p" variant="bodyMd" fontWeight="semibold">
                Cost of Goods Sold (COGS)
              </Text>
              <Text as="p" variant="bodySm" tone="subdued">
                Current: {cogsChange >= 0 ? '+' : ''}
                {cogsChange.toFixed(0)}%
              </Text>
              <RangeSlider
                label=""
                value={cogsChange}
                onChange={(value) => setCogsChange(value as number)}
                min={-30}
                max={50}
                step={1}
                output
              />
            </div>

            {/* Conversion Rate */}
            <div>
              <Text as="p" variant="bodyMd" fontWeight="semibold">
                Conversion Rate
              </Text>
              <Text as="p" variant="bodySm" tone="subdued">
                Current: {conversionRateChange >= 0 ? '+' : ''}
                {conversionRateChange.toFixed(0)}%
              </Text>
              <RangeSlider
                label=""
                value={conversionRateChange}
                onChange={(value) => setConversionRateChange(value as number)}
                min={-30}
                max={50}
                step={1}
                output
              />
            </div>
          </BlockStack>
        </BlockStack>
      </Card>

      {/* Simulation Results */}
      {simulation && (
        <>
          {/* Summary Results */}
          <Card>
            <BlockStack gap="400">
              <Text as="h3" variant="headingMd">
                📈 Simulation Results ({simulation.inputs.simulations.toLocaleString()}{' '}
                simulations)
              </Text>

              <InlineStack gap="400" wrap={true}>
                <Card>
                  <BlockStack gap="200">
                    <Text as="p" variant="bodySm" tone="subdued">
                      Expected Revenue
                    </Text>
                    <Text as="p" variant="headingLg">
                      ${simulation.results.revenue.median.toLocaleString('en-US', {
                        minimumFractionDigits: 0,
                        maximumFractionDigits: 0,
                      })}
                    </Text>
                    <Text as="p" variant="bodySm" tone="subdued">
                      90% confidence: $
                      {simulation.results.revenue.confidence_90[0].toLocaleString(
                        'en-US',
                        { maximumFractionDigits: 0 }
                      )}{' '}
                      - $
                      {simulation.results.revenue.confidence_90[1].toLocaleString(
                        'en-US',
                        { maximumFractionDigits: 0 }
                      )}
                    </Text>
                  </BlockStack>
                </Card>

                <Card>
                  <BlockStack gap="200">
                    <Text as="p" variant="bodySm" tone="subdued">
                      Expected Profit
                    </Text>
                    <Text as="p" variant="headingLg">
                      ${simulation.results.profit.median.toLocaleString('en-US', {
                        minimumFractionDigits: 0,
                        maximumFractionDigits: 0,
                      })}
                    </Text>
                    <Text as="p" variant="bodySm" tone="subdued">
                      90% confidence: $
                      {simulation.results.profit.confidence_90[0].toLocaleString(
                        'en-US',
                        { maximumFractionDigits: 0 }
                      )}{' '}
                      - $
                      {simulation.results.profit.confidence_90[1].toLocaleString(
                        'en-US',
                        { maximumFractionDigits: 0 }
                      )}
                    </Text>
                  </BlockStack>
                </Card>

                <Card>
                  <BlockStack gap="200">
                    <Text as="p" variant="bodySm" tone="subdued">
                      Success Rate
                    </Text>
                    <Text as="p" variant="headingLg">
                      {simulation.results.profit.probability_positive.toFixed(1)}%
                    </Text>
                    <Text as="p" variant="bodySm" tone="subdued">
                      Probability of profit &gt; $0
                    </Text>
                  </BlockStack>
                </Card>

                <Card>
                  <BlockStack gap="200">
                    <Text as="p" variant="bodySm" tone="subdued">
                      Profit Margin
                    </Text>
                    <Text as="p" variant="headingLg">
                      {simulation.results.profit_margin.median.toFixed(1)}%
                    </Text>
                    <Text as="p" variant="bodySm" tone="subdued">
                      Expected margin
                    </Text>
                  </BlockStack>
                </Card>
              </InlineStack>
            </BlockStack>
          </Card>

          {/* Insights */}
          {simulation.insights.length > 0 && (
            <Banner tone="info">
              <BlockStack gap="200">
                <Text as="p" variant="bodyMd" fontWeight="semibold">
                  🎯 Key Insights
                </Text>
                {simulation.insights.map((insight, index) => (
                  <Text key={index} as="p" variant="bodySm">
                    • {insight}
                  </Text>
                ))}
              </BlockStack>
            </Banner>
          )}

          {/* Revenue Distribution Chart */}
          <Card>
            <BlockStack gap="300">
              <Text as="h3" variant="headingMd">
                📊 Revenue Probability Distribution
              </Text>

              <Plot
                data={[
                  {
                    type: 'bar',
                    x: simulation.results.revenue.histogram.bin_centers,
                    y: simulation.results.revenue.histogram.frequencies,
                    marker: {
                      color: '#5C6AC4',
                    },
                  },
                ]}
                layout={{
                  autosize: true,
                  margin: { t: 20, r: 40, b: 60, l: 60 },
                  xaxis: {
                    title: { text: 'Revenue ($)' },
                    tickformat: '$,.0f',
                  },
                  yaxis: {
                    title: { text: 'Frequency' },
                  },
                  showlegend: false,
                }}
                config={{ responsive: true, displayModeBar: false }}
                style={{ width: '100%', height: '400px' }}
                useResizeHandler
              />

              <InlineStack gap="400" wrap={true}>
                <div>
                  <Text as="p" variant="bodySm" tone="subdued">
                    50th percentile (median)
                  </Text>
                  <Text as="p" variant="bodyMd" fontWeight="semibold">
                    ${simulation.results.revenue.median.toLocaleString('en-US')}
                  </Text>
                </div>
                <div>
                  <Text as="p" variant="bodySm" tone="subdued">
                    Best case (95th percentile)
                  </Text>
                  <Text as="p" variant="bodyMd" fontWeight="semibold">
                    $
                    {simulation.results.revenue.percentile_95.toLocaleString(
                      'en-US'
                    )}
                  </Text>
                </div>
                <div>
                  <Text as="p" variant="bodySm" tone="subdued">
                    Worst case (5th percentile)
                  </Text>
                  <Text as="p" variant="bodyMd" fontWeight="semibold">
                    ${simulation.results.revenue.percentile_5.toLocaleString('en-US')}
                  </Text>
                </div>
              </InlineStack>
            </BlockStack>
          </Card>

          {/* Profit Distribution Chart */}
          <Card>
            <BlockStack gap="300">
              <Text as="h3" variant="headingMd">
                💰 Profit Probability Distribution
              </Text>

              <Plot
                data={[
                  {
                    type: 'bar',
                    x: simulation.results.profit.histogram.bin_centers,
                    y: simulation.results.profit.histogram.frequencies,
                    marker: {
                      color: '#47C1BF',
                    },
                  },
                ]}
                layout={{
                  autosize: true,
                  margin: { t: 20, r: 40, b: 60, l: 60 },
                  xaxis: {
                    title: { text: 'Profit ($)' },
                    tickformat: '$,.0f',
                  },
                  yaxis: {
                    title: { text: 'Frequency' },
                  },
                  showlegend: false,
                  shapes: [
                    {
                      type: 'line',
                      x0: 0,
                      x1: 0,
                      y0: 0,
                      y1: 1,
                      yref: 'paper',
                      line: {
                        color: 'red',
                        width: 2,
                        dash: 'dash',
                      },
                    },
                  ],
                }}
                config={{ responsive: true, displayModeBar: false }}
                style={{ width: '100%', height: '400px' }}
                useResizeHandler
              />

              <Text as="p" variant="bodySm" tone="subdued">
                Red line shows break-even point. Area to the right ={' '}
                {simulation.results.profit.probability_positive.toFixed(1)}% chance of
                positive profit.
              </Text>
            </BlockStack>
          </Card>

          {/* Sensitivity Analysis */}
          {Object.keys(simulation.sensitivity).length > 0 && (
            <Card>
              <BlockStack gap="300">
                <Text as="h3" variant="headingMd">
                  🎚️ Sensitivity Analysis
                </Text>
                <Text as="p" variant="bodySm" tone="subdued">
                  Which variables have the biggest impact on results?
                </Text>

                <BlockStack gap="200">
                  {Object.entries(simulation.sensitivity)
                    .sort(([, a], [, b]) => b - a)
                    .map(([variable, impact]) => {
                      const tone = impact > 30 ? 'critical' : impact > 15 ? 'warning' : 'info';
                      const impactText = `${impact.toFixed(1)}%`;
                      return (
                        <div key={variable}>
                          <InlineStack align="space-between" blockAlign="center">
                            <Text as="p" variant="bodySm">
                              {variable.replace(/_/g, ' ').replace(/\b\w/g, (l) =>
                                l.toUpperCase()
                              )}
                            </Text>
                            <Badge tone={tone}>{impactText}</Badge>
                          </InlineStack>
                          <div
                            style={{
                              width: `${impact}%`,
                              height: '8px',
                              backgroundColor: '#5C6AC4',
                              borderRadius: '4px',
                              marginTop: '4px',
                            }}
                          />
                        </div>
                      );
                    })}
                </BlockStack>
              </BlockStack>
            </Card>
          )}
        </>
      )}
    </BlockStack>
  );
}