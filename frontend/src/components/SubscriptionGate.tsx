// components/SubscriptionGate.tsx
import { EmptyState, Button, BlockStack, Spinner } from '@shopify/polaris';
import { useState, useEffect } from 'react';
import { authenticatedFetch } from '../lib/api';

interface SubscriptionGateProps {
  children: React.ReactNode;
  hasActiveSubscription: boolean;
  feature?: string; // Optional: customize message per feature
}

interface SubscribeUrlResponse {
  subscribe_url: string;
  plan_name: string;
  price: number;
  currency: string;
  trial_days: number;
}

export function SubscriptionGate({
  children,
  hasActiveSubscription,
  feature = "This feature"
}: SubscriptionGateProps) {
  const [subscribeData, setSubscribeData] = useState<SubscribeUrlResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string>('');

  useEffect(() => {
    if (!hasActiveSubscription) {
      fetchSubscribeUrl();
    }
  }, [hasActiveSubscription]);

  const fetchSubscribeUrl = async () => {
    try {
      const data = await authenticatedFetch<SubscribeUrlResponse>('/api/billing/subscribe-url');
      setSubscribeData(data);
    } catch (err) {
      console.error('Error fetching subscription URL:', err);
      setError('Unable to load subscription details');
    }
  };

  const handleSubscribe = () => {
    if (!subscribeData?.subscribe_url) {
      setError('Subscription URL not available');
      return;
    }
    
    setIsLoading(true);
    // Use _top to break out of Shopify admin iframe
    window.open(subscribeData.subscribe_url, '_top');
    
    // Reset loading state after a delay (in case user comes back without subscribing)
    setTimeout(() => setIsLoading(false), 3000);
  };

  if (!hasActiveSubscription) {
    return (
      <EmptyState
        heading="Upgrade to Access This Feature"
        image="https://cdn.shopify.com/s/files/1/0262/4071/2726/files/emptystate-files.png"
      >
        <BlockStack gap="400">
          <p>
            {feature} is available with a Lodestar Analytics subscription. 
            Start your 14-day free trial to unlock advanced analytics, forecasting, and profitability insights.
          </p>
          
          {subscribeData && (
            <p style={{ fontSize: '0.9em', color: '#6d7175' }}>
              Only ${subscribeData.price}/{subscribeData.currency === 'USD' ? 'month' : subscribeData.currency} after your {subscribeData.trial_days}-day trial
            </p>
          )}
          
          {error && (
            <p style={{ color: '#d82c0d', fontSize: '0.9em' }}>
              {error}
            </p>
          )}
          
          {!subscribeData && !error ? (
            <Spinner size="small" />
          ) : (
            <Button
              variant="primary"
              loading={isLoading}
              onClick={handleSubscribe}
              disabled={!subscribeData?.subscribe_url || isLoading}
            >
              {isLoading 
                ? 'Redirecting to Shopify...' 
                : `Start ${subscribeData?.trial_days}-Day Free Trial`
              }
            </Button>
          )}
        </BlockStack>
      </EmptyState>
    );
  }

  return <>{children}</>;
}