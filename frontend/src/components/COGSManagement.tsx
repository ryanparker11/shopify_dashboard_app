import React, { useState, useRef, useEffect, useCallback } from 'react';
import { 
    Button, 
    Card, 
    Text, 
    Banner, 
    BlockStack,
    InlineStack,
    ProgressBar,
    List,
    Divider
} from '@shopify/polaris';
import { authenticatedFetch } from '../lib/api';

interface UploadResult {
    success: boolean;
    updated_count: number;
    skipped_count: number;
    total_rows: number;
    errors?: string[];
    message: string;
}

interface ProfitMetrics {
    period_days: number;
    total_revenue: number;
    total_cogs: number;
    gross_profit: number;
    profit_margin_percentage: number;
    order_count: number;
    items_without_cogs: number;
    has_complete_data: boolean;
}

interface COGSManagementProps {
    shopDomain: string;
}

export function COGSManagement({ shopDomain }: COGSManagementProps) {
    const [downloadingTemplate, setDownloadingTemplate] = useState(false);
    const [uploadingFile, setUploadingFile] = useState(false);
    const [uploadResult, setUploadResult] = useState<UploadResult | null>(null);
    const [uploadError, setUploadError] = useState<string | null>(null);
    const [profitMetrics, setProfitMetrics] = useState<ProfitMetrics | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);

    // Load profit metrics on component mount and after successful upload
    const fetchProfitMetrics = useCallback(async () => {
        try {
            const data = await authenticatedFetch<ProfitMetrics>(
                `/api/cogs/profit-analysis?shop_domain=${encodeURIComponent(shopDomain)}&days=30`
            );
            setProfitMetrics(data);
        } catch (err) {
            console.error('Error fetching profit metrics:', err);
        }
    }, [shopDomain]);

    useEffect(() => {
        fetchProfitMetrics();
    }, [fetchProfitMetrics]);

    const downloadTemplate = async () => {
        setDownloadingTemplate(true);
        try {
            // For blob downloads, we need to make a raw fetch request
            const params = new URLSearchParams(window.location.search);
            const urlToken = params.get('id_token');
            
            if (!urlToken) {
                throw new Error('No session token available');
            }

            const response = await fetch(
                `${import.meta.env.VITE_API_BASE}/api/cogs/download-template?shop_domain=${encodeURIComponent(shopDomain)}`,
                {
                    headers: {
                        'Authorization': `Bearer ${urlToken}`,
                    },
                }
            );

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to download template');
            }

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'cogs_upload_template.xlsx';
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (err) {
            console.error('Error downloading template:', err);
            alert('Failed to download COGS template. Please try again.');
        } finally {
            setDownloadingTemplate(false);
        }
    };

    const handleFileSelect = () => {
        fileInputRef.current?.click();
    };

    const handleFileChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;

        // Reset previous results
        setUploadResult(null);
        setUploadError(null);
        setUploadingFile(true);

        try {
            const params = new URLSearchParams(window.location.search);
            const urlToken = params.get('id_token');
            
            if (!urlToken) {
                throw new Error('No session token available');
            }

            const formData = new FormData();
            formData.append('file', file);

            const response = await fetch(
                `${import.meta.env.VITE_API_BASE}/api/cogs/upload-template?shop_domain=${encodeURIComponent(shopDomain)}`,
                {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${urlToken}`,
                    },
                    body: formData,
                }
            );

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.detail || 'Failed to upload file');
            }

            setUploadResult(data);
            // Refresh profit metrics after successful upload
            await fetchProfitMetrics();
        } catch (err) {
            console.error('Error uploading file:', err);
            setUploadError(err instanceof Error ? err.message : 'Failed to upload file. Please try again.');
        } finally {
            setUploadingFile(false);
            // Reset file input
            if (fileInputRef.current) {
                fileInputRef.current.value = '';
            }
        }
    };

    const formatCurrency = (amount: number) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
        }).format(amount);
    };

    return (
        <BlockStack gap="400">
            {/* COGS Upload Card */}
            <Card>
                <BlockStack gap="400">
                    <Text as="h2" variant="headingMd">
                        COGS Management
                    </Text>

                    <Text as="p" tone="subdued">
                        Download a pre-filled template with all your products, add COGS values, and upload to update your profit calculations.
                    </Text>

                    <InlineStack gap="300">
                        <Button
                            onClick={downloadTemplate}
                            loading={downloadingTemplate}
                        >
                            {downloadingTemplate ? 'Generating...' : '📥 Download Template'}
                        </Button>

                        <Button
                            onClick={handleFileSelect}
                            loading={uploadingFile}
                            variant="primary"
                        >
                            {uploadingFile ? 'Uploading...' : '📤 Upload Completed Template'}
                        </Button>
                    </InlineStack>

                    <input
                        ref={fileInputRef}
                        type="file"
                        accept=".xlsx,.xls"
                        onChange={handleFileChange}
                        style={{ display: 'none' }}
                    />

                    {uploadingFile && (
                        <Banner tone="info">
                            <BlockStack gap="200">
                                <Text as="p">Processing your COGS data...</Text>
                                <ProgressBar progress={50} size="small" />
                            </BlockStack>
                        </Banner>
                    )}

                    {uploadResult && (
                        <Banner
                            tone={uploadResult.success ? 'success' : 'warning'}
                            onDismiss={() => setUploadResult(null)}
                        >
                            <BlockStack gap="200">
                                <Text as="p" fontWeight="semibold">
                                    {uploadResult.message}
                                </Text>
                                <Text as="p" variant="bodySm">
                                    Updated: {uploadResult.updated_count} | Skipped: {uploadResult.skipped_count} | Total: {uploadResult.total_rows}
                                </Text>
                                {uploadResult.errors && uploadResult.errors.length > 0 && (
                                    <BlockStack gap="100">
                                        <Text as="p" variant="bodySm" tone="subdued">
                                            Errors:
                                        </Text>
                                        <List type="bullet">
                                            {uploadResult.errors.slice(0, 5).map((error, idx) => (
                                                <List.Item key={idx}>{error}</List.Item>
                                            ))}
                                        </List>
                                    </BlockStack>
                                )}
                            </BlockStack>
                        </Banner>
                    )}

                    {uploadError && (
                        <Banner tone="critical" onDismiss={() => setUploadError(null)}>
                            <Text as="p">{uploadError}</Text>
                        </Banner>
                    )}
                </BlockStack>
            </Card>

            {/* Profit Metrics Card */}
            {profitMetrics && (
                <Card>
                    <BlockStack gap="400">
                        <Text as="h2" variant="headingMd">
                            Profit Analysis (Last 30 Days)
                        </Text>

                        {!profitMetrics.has_complete_data && (
                            <Banner tone="warning">
                                <Text as="p">
                                    {profitMetrics.items_without_cogs} items are missing COGS data. 
                                    Upload a completed template for accurate profit calculations.
                                </Text>
                            </Banner>
                        )}

                        <BlockStack gap="300">
                            <InlineStack gap="400" wrap={false}>
                                <div style={{ flex: 1 }}>
                                    <BlockStack gap="100">
                                        <Text as="p" tone="subdued" variant="bodySm">
                                            Total Revenue
                                        </Text>
                                        <Text as="p" variant="headingLg">
                                            {formatCurrency(profitMetrics.total_revenue)}
                                        </Text>
                                    </BlockStack>
                                </div>
                                <div style={{ flex: 1 }}>
                                    <BlockStack gap="100">
                                        <Text as="p" tone="subdued" variant="bodySm">
                                            Total COGS
                                        </Text>
                                        <Text as="p" variant="headingLg">
                                            {formatCurrency(profitMetrics.total_cogs)}
                                        </Text>
                                    </BlockStack>
                                </div>
                            </InlineStack>

                            <Divider />

                            <InlineStack gap="400" wrap={false}>
                                <div style={{ flex: 1 }}>
                                    <BlockStack gap="100">
                                        <Text as="p" tone="subdued" variant="bodySm">
                                            Gross Profit
                                        </Text>
                                        <Text as="p" variant="headingLg" tone={profitMetrics.gross_profit >= 0 ? 'success' : 'critical'}>
                                            {formatCurrency(profitMetrics.gross_profit)}
                                        </Text>
                                    </BlockStack>
                                </div>
                                <div style={{ flex: 1 }}>
                                    <BlockStack gap="100">
                                        <Text as="p" tone="subdued" variant="bodySm">
                                            Profit Margin
                                        </Text>
                                        <Text as="p" variant="headingLg" tone={profitMetrics.profit_margin_percentage >= 0 ? 'success' : 'critical'}>
                                            {profitMetrics.profit_margin_percentage.toFixed(2)}%
                                        </Text>
                                    </BlockStack>
                                </div>
                            </InlineStack>

                            <Text as="p" tone="subdued" variant="bodySm">
                                Based on {profitMetrics.order_count} paid orders
                            </Text>
                        </BlockStack>
                    </BlockStack>
                </Card>
            )}
        </BlockStack>
    );
}