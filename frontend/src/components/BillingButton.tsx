// === BILLING ADDITIONS START: components/BillingButton.tsx ====================
// components/BillingButton.tsx
import { Button } from '@shopify/polaris';
import { useState } from 'react';

type Props = {
  shopDomain: string;
  planName?: string;
  price?: number;
  apiUrl: string; // <— NEW: pass API base, e.g. https://api.lodestaranalytics.io
};

export default function BillingButton({
  shopDomain,
  planName = 'Lodestar Pro',
  price = 25,
  apiUrl,
}: Props) {
  const [loading, setLoading] = useState(false);

  const onUpgrade = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${apiUrl}/api/billing/start`, {
        method: 'POST',
        credentials: 'include', // <— IMPORTANT if your session/cookies are on the API domain
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ shop_domain: shopDomain, plan_name: planName, price }),
      });

      if (!res.ok) {
        const text = await res.text();
        console.error('Billing start failed:', res.status, text);
        alert('Unable to start billing. Check console for details.');
        return;
      }

      const json = await res.json();
      const confirmationUrl = json?.confirmationUrl;
      if (!confirmationUrl) {
        console.error('No confirmationUrl in response:', json);
        alert('Billing response missing confirmation URL.');
        return;
      }

      // Embedded-safe redirect (works even inside Shopify iframe)
      if (window.top) {
        // Some browsers block assignment if undefined; we validated above.
        (window.top as Window).location.href = confirmationUrl;
      } else {
        // Fallback for non-embedded/local preview
        window.open(confirmationUrl, '_top');
      }
    } catch (e) {
      console.error('Upgrade error:', e);
      alert('Failed to start upgrade. See console for details.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Button variant="primary" loading={loading} onClick={onUpgrade}>
      Upgrade to Pro
    </Button>
  );
}

// === BILLING ADDITIONS END: components/BillingButton.tsx ======================

