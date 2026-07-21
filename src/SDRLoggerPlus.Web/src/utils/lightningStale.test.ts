import { describe, it, expect } from 'vitest';
import { isLightningDataDelayed, LIGHTNING_STALE_MS } from './lightningStale';

const NOW = Date.parse('2026-07-20T12:00:00Z');
const ago = (ms: number) => new Date(NOW - ms).toISOString();

describe('isLightningDataDelayed', () => {
  it('is fresh right after a poll', () => {
    expect(isLightningDataDelayed(ago(5_000), NOW)).toBe(false);
  });

  // Worst-case HEALTHY age: status restamped every 90 s + banner polling
  // every 60 s. The threshold must not cry wolf inside that window.
  it('is fresh at the worst-case healthy age (~2.5 min)', () => {
    expect(isLightningDataDelayed(ago(150_000), NOW)).toBe(false);
  });

  it('is fresh exactly at the threshold', () => {
    expect(isLightningDataDelayed(ago(LIGHTNING_STALE_MS), NOW)).toBe(false);
  });

  it('is delayed just past the threshold', () => {
    expect(isLightningDataDelayed(ago(LIGHTNING_STALE_MS + 1), NOW)).toBe(true);
  });

  // The backend's outage holdover serves a held status for up to 5 min —
  // squarely inside the window this hint exists for.
  it('is delayed during a mid-holdover stall (4 min)', () => {
    expect(isLightningDataDelayed(ago(4 * 60_000), NOW)).toBe(true);
  });

  it('treats a missing timestamp as not stale', () => {
    expect(isLightningDataDelayed(null, NOW)).toBe(false);
    expect(isLightningDataDelayed(undefined, NOW)).toBe(false);
    expect(isLightningDataDelayed('', NOW)).toBe(false);
  });

  it('treats an unparseable timestamp as not stale', () => {
    expect(isLightningDataDelayed('not-a-date', NOW)).toBe(false);
  });

  it('parses the backend Z-designated form', () => {
    expect(isLightningDataDelayed('2026-07-20T11:50:00.0000000Z', NOW)).toBe(true);
    expect(isLightningDataDelayed('2026-07-20T11:59:00.0000000Z', NOW)).toBe(false);
  });
});
