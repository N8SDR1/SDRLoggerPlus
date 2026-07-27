import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { QslBadge } from './QrzProfilePlugin';

// Issue #39: QRZ's lotw/eqsl/mqsl fields are a genuine three-state — a badge
// must read as "accepts" / "does not accept" / (nothing at all), never a third
// "unknown" pill that would bury the channels QRZ actually answered.
describe('QslBadge', () => {
  it('renders nothing when the channel is unknown', () => {
    const { container } = render(<QslBadge label="LOTW" accepts={undefined} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('shows the label when the operator accepts the channel', () => {
    render(<QslBadge label="LOTW" accepts={true} />);
    expect(screen.getByText('LOTW')).toBeInTheDocument();
    expect(screen.getByTitle(/accepts this confirmation channel/)).toBeInTheDocument();
  });

  it('still renders — dimmed — when the operator does not accept the channel', () => {
    // false is real information ("QRZ says no"), distinct from absent (undefined).
    render(<QslBadge label="eQSL" accepts={false} />);
    expect(screen.getByText('eQSL')).toBeInTheDocument();
    expect(screen.getByTitle(/does not accept this confirmation channel/)).toBeInTheDocument();
  });
});
