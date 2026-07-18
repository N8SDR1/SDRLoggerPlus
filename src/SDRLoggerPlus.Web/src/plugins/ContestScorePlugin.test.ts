import { describe, it, expect } from 'vitest';
import { formatElapsed, averageRate } from './ContestScorePlugin';

describe('ContestScorePlugin formatElapsed', () => {
  it('formats under an hour as M:SS', () => {
    expect(formatElapsed(0)).toBe('0:00');
    expect(formatElapsed(65_000)).toBe('1:05');
    expect(formatElapsed(59 * 60_000 + 59_000)).toBe('59:59');
  });

  it('formats an hour or more as H:MM:SS', () => {
    expect(formatElapsed(3_600_000)).toBe('1:00:00');
    expect(formatElapsed(3_661_000)).toBe('1:01:01');
  });

  it('clamps negative / non-finite input to zero', () => {
    expect(formatElapsed(-5000)).toBe('0:00');
    expect(formatElapsed(NaN)).toBe('0:00');
  });
});

describe('ContestScorePlugin averageRate', () => {
  const start = '2026-02-21T00:00:00Z';

  it('returns 0 in the first 30 seconds to avoid a spike', () => {
    const now = Date.parse(start) + 10_000;
    expect(averageRate(1, start, now)).toBe(0);
  });

  it('computes QSOs per hour over the elapsed session', () => {
    const now = Date.parse(start) + 3_600_000; // 1 hour
    expect(averageRate(120, start, now)).toBe(120);
    const half = Date.parse(start) + 1_800_000; // 30 min
    expect(averageRate(60, start, half)).toBe(120); // 60 in 0.5h -> 120/hr
  });

  it('returns 0 for an unparseable start time', () => {
    expect(averageRate(50, 'not-a-date', Date.now())).toBe(0);
  });
});
