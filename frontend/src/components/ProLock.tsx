// === BILLING ADDITIONS START: components/ProLock.tsx ==========================
// FIXED: type-only import for ReactNode
import type { ReactNode } from 'react';
import { Tooltip } from '@shopify/polaris';

type Props = {
  locked: boolean;
  tooltip?: string;
  children: ReactNode;
};

export default function ProLock({
  locked,
  tooltip = 'This is a Pro feature — upgrade to unlock.',
  children,
}: Props) {
  if (!locked) return <>{children}</>;

  // Tooltip won’t show on disabled buttons; wrap with a span and style it
  return (
    <Tooltip content={tooltip} preferredPosition="above">
      <span
        style={{
          display: 'inline-block',
          filter: 'grayscale(1)',
          opacity: 0.5,
          cursor: 'not-allowed',
        }}
      >
        {children}
      </span>
    </Tooltip>
  );
}
// === BILLING ADDITIONS END: components/ProLock.tsx ============================

