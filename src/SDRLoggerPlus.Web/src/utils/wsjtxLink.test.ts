import { describe, it, expect } from 'vitest';
import { summarizeWsjtxLink, parseUtc, HEARTBEAT_STALE_MS } from './wsjtxLink';
import type { WsjtxStatus } from '../api/client';

const NOW = Date.parse('2026-07-20T12:00:00Z');

const status = (over: Partial<WsjtxStatus> = {}): WsjtxStatus => ({
  source: 1,
  listening: true,
  port: 2237,
  multicastAddress: null,
  error: null,
  clients: [],
  lastQsoCall: null,
  lastQsoAtUtc: null,
  ...over,
});

const heardAgo = (ms: number, id = 'WSJT-X') => ({
  id,
  version: '2.7.0',
  lastHeardUtc: new Date(NOW - ms).toISOString(),
});

describe('parseUtc', () => {
  it('parses a .NET UTC stamp with the Z designator', () => {
    expect(parseUtc('2026-07-20T12:00:00.0000000Z')).toBe(NOW);
  });

  // Without this, a stamp that lost its designator would be read as local
  // time and shift freshness by the machine's UTC offset.
  it('treats a designator-less stamp as UTC, not local', () => {
    expect(parseUtc('2026-07-20T12:00:00')).toBe(NOW);
  });

  it('honours an explicit offset', () => {
    expect(parseUtc('2026-07-20T14:00:00+02:00')).toBe(NOW);
  });
});

describe('summarizeWsjtxLink', () => {
  it('is off when nothing is listening', () => {
    expect(summarizeWsjtxLink([status({ listening: false })], NOW).state).toBe('off');
  });

  it('is off when the status fetch failed entirely', () => {
    expect(summarizeWsjtxLink(null, NOW).state).toBe('off');
  });

  it('is listening when bound but no decoder has ever connected', () => {
    const link = summarizeWsjtxLink([status()], NOW);
    expect(link.state).toBe('listening');
    expect(link.title).toContain('2237');
  });

  it('is alive on a fresh heartbeat', () => {
    const link = summarizeWsjtxLink([status({ clients: [heardAgo(5_000)] })], NOW);
    expect(link.state).toBe('alive');
    expect(link.title).toContain('WSJT-X');
  });

  it('is alive exactly at the staleness threshold', () => {
    const link = summarizeWsjtxLink([status({ clients: [heardAgo(HEARTBEAT_STALE_MS)] })], NOW);
    expect(link.state).toBe('alive');
  });

  it('is stale one millisecond past the threshold', () => {
    const link = summarizeWsjtxLink([status({ clients: [heardAgo(HEARTBEAT_STALE_MS + 1)] })], NOW);
    expect(link.state).toBe('stale');
  });

  it('reports how long it has been quiet when stale', () => {
    const link = summarizeWsjtxLink([status({ clients: [heardAgo(90_000)] })], NOW);
    expect(link.title).toContain('90s');
  });

  it('surfaces a bind error even when the other source is healthy', () => {
    const link = summarizeWsjtxLink(
      [
        status({ source: 1, clients: [heardAgo(1_000)] }),
        status({ source: 2, port: 2333, listening: false, error: 'Address already in use' }),
      ],
      NOW,
    );
    expect(link.state).toBe('error');
    expect(link.title).toContain('2333');
    expect(link.title).toContain('Address already in use');
  });

  it('takes the newest heartbeat across both sources', () => {
    const link = summarizeWsjtxLink(
      [
        status({ source: 1, clients: [heardAgo(120_000, 'JTDX')] }),
        status({ source: 2, port: 2333, clients: [heardAgo(2_000, 'WSJT-X')] }),
      ],
      NOW,
    );
    expect(link.state).toBe('alive');
  });

  it('counts listening sources in the label', () => {
    const link = summarizeWsjtxLink(
      [status({ source: 1 }), status({ source: 2, port: 2333 })],
      NOW,
    );
    expect(link.label).toBe('FT8 ×2');
  });

  it('uses the plain label for a single source', () => {
    expect(summarizeWsjtxLink([status()], NOW).label).toBe('FT8');
  });

  it('lists each connected decoder once', () => {
    const link = summarizeWsjtxLink(
      [status({ clients: [heardAgo(1_000, 'WSJT-X'), heardAgo(2_000, 'WSJT-X')] })],
      NOW,
    );
    expect(link.title.match(/WSJT-X/g)).toHaveLength(1);
  });

  it('ignores an unparseable timestamp rather than reporting a bogus age', () => {
    const link = summarizeWsjtxLink(
      [status({ clients: [{ id: 'X', version: null, lastHeardUtc: 'not-a-date' }] })],
      NOW,
    );
    expect(link.state).toBe('listening');
  });
});
