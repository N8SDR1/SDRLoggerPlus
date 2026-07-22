import { describe, it, expect } from 'vitest';
import { parseTimestamp, formatAge, formatDupeWarning } from './dupeWarning';
import type { QsoResponse } from '../api/client';

const NOW = Date.parse('2026-07-20T12:00:00Z');

const qso = (over: Partial<QsoResponse> = {}): QsoResponse => ({
  id: 'abc123',
  callsign: 'K1ABC',
  qsoDate: '2026-07-20',
  timeOn: '1142',
  band: '20m',
  mode: 'FT8',
  createdAt: new Date(NOW - 12 * 60_000).toISOString(),
  ...over,
});

describe('parseTimestamp', () => {
  it('parses a Z-designated UTC stamp', () => {
    expect(parseTimestamp('2026-07-20T12:00:00.0000000Z')).toBe(NOW);
  });

  it('honours an explicit offset', () => {
    expect(parseTimestamp('2026-07-20T07:00:00-05:00')).toBe(NOW);
  });

  // A stamp that lost its designator must be read as UTC, not local time.
  it('treats a designator-less stamp as UTC', () => {
    expect(parseTimestamp('2026-07-20T12:00:00')).toBe(NOW);
  });
});

describe('formatAge', () => {
  it('says "just now" under a minute', () => {
    expect(formatAge(new Date(NOW - 20_000).toISOString(), NOW)).toBe('just now');
  });

  it('uses singular for one minute', () => {
    expect(formatAge(new Date(NOW - 90_000).toISOString(), NOW)).toBe('1 min ago');
  });

  it('reports whole minutes', () => {
    expect(formatAge(new Date(NOW - 12 * 60_000).toISOString(), NOW)).toBe('12 min ago');
  });

  it('degrades to "recently" for an unparseable stamp', () => {
    expect(formatAge('not-a-date', NOW)).toBe('recently');
  });
});

describe('formatDupeWarning', () => {
  it('names the call, age, band and mode', () => {
    expect(formatDupeWarning(qso(), NOW)).toBe('K1ABC already logged 12 min ago on 20m FT8');
  });
});
