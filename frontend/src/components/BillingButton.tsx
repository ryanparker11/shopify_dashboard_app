// === BILLING ADDITIONS START: components/BillingButton.tsx ====================
import { Button } from '@shopify/polaris';
import { useState } from 'react';

type Props = {
  shopDomain: string;
  planName?: string;
  price?: number;
};

export default function BillingButton({ shopDomain, planName = 'Lodestar Pro', price = 25 }: Props) {
  const [loading, setLoading] = useState(false);

  const onUpgrade = async () => {
    setLoading(true);
    try {
      const r = await fetch('/api/billing/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ shop_domain: shopDomain, plan_name: planName, price })
      });
      const { confirmationUrl } = await r.json();
      window.top!.location.href = confirmationUrl;
    } finally {
      setLoading(false);
    }
  };

  return (
    // CHANGED: primary -> variant="primary"
    <Button variant="primary" loading={loading} onClick={onUpgrade}>
      Upgrade to Pro
    </Button>
  );
}
// === BILLING ADDITIONS END: components/BillingButton.tsx ======================

