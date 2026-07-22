import { describe, it, expect } from 'vitest';
import { qslSyncBadges, qslSyncSortKey } from './qslSyncBadges';
import type { QslServiceSync } from '../api/client';

const synced = (over: Partial<QslServiceSync> = {}): QslServiceSync => ({
  status: 'Synced',
  syncedAt: '2026-07-21T14:30:00Z',
  failureKind: 'None',
  attempts: 0,
  retryable: false,
  ...over,
});

const failed = (over: Partial<QslServiceSync> = {}): QslServiceSync => ({
  status: 'NotSynced',
  failureKind: 'Temporary',
  lastError: 'server error',
  attempts: 1,
  retryable: true,
  ...over,
});

describe('qslSyncBadges', () => {
  it('returns nothing for a QSO logged before tracking existed', () => {
    // Undefined must not be rendered as "not sent" — we genuinely do not know.
    expect(qslSyncBadges(undefined)).toEqual([]);
  });

  it('returns nothing when no service has reported', () => {
    expect(qslSyncBadges({})).toEqual([]);
  });

  it('omits services that never reported rather than showing them as failed', () => {
    const badges = qslSyncBadges({ clubLog: synced() });

    expect(badges).toHaveLength(1);
    expect(badges[0].letter).toBe('C');
  });

  it('marks a successful upload as synced and names the time', () => {
    const [badge] = qslSyncBadges({ eqsl: synced() });

    expect(badge.state).toBe('synced');
    expect(badge.title).toContain('eQSL: uploaded');
    expect(badge.title).toContain('2026-07-21 14:30');
  });

  it('marks a transient failure as retryable', () => {
    const [badge] = qslSyncBadges({ clubLog: failed() });

    expect(badge.state).toBe('retryable');
    expect(badge.title).toContain('will retry');
    expect(badge.title).toContain('server error');
  });

  it('marks an auth failure as blocked and tells the operator to check credentials', () => {
    const [badge] = qslSyncBadges({
      clubLog: failed({ failureKind: 'Auth', retryable: false, lastError: '403' }),
    });

    expect(badge.state).toBe('blocked');
    expect(badge.title).toContain('check credentials');
    expect(badge.title).not.toContain('will retry');
  });

  it('marks a rejected QSO as needing attention, not retry', () => {
    const [badge] = qslSyncBadges({
      eqsl: failed({ failureKind: 'Rejected', retryable: false, lastError: 'ERROR: bad record' }),
    });

    expect(badge.state).toBe('blocked');
    expect(badge.title).toContain('needs attention');
  });

  it('mentions the attempt count only when it has tried more than once', () => {
    expect(qslSyncBadges({ clubLog: failed({ attempts: 1 }) })[0].title).not.toContain('attempts');
    expect(qslSyncBadges({ clubLog: failed({ attempts: 4 }) })[0].title).toContain('after 4 attempts');
  });

  it('reports each service independently', () => {
    const badges = qslSyncBadges({
      clubLog: synced(),
      eqsl: failed({ failureKind: 'Auth', retryable: false }),
    });

    expect(badges.map((b) => [b.letter, b.state])).toEqual([
      ['C', 'synced'],
      ['E', 'blocked'],
    ]);
  });

  it('survives a timestamp with no zone designator', () => {
    // LiteDB round-trips have historically returned local-kind timestamps.
    const [badge] = qslSyncBadges({ clubLog: synced({ syncedAt: '2026-07-21T14:30:00' }) });

    expect(badge.title).toContain('2026-07-21 14:30');
  });

  it('omits the time rather than printing Invalid Date', () => {
    const [badge] = qslSyncBadges({ clubLog: synced({ syncedAt: 'not-a-date' }) });

    expect(badge.title).toBe('Club Log: uploaded');
  });
});

describe('qslSyncSortKey', () => {
  it('sorts unresolved failures above successes and untracked QSOs', () => {
    const blocked = qslSyncSortKey({ clubLog: failed({ failureKind: 'Auth', retryable: false }) });
    const retryable = qslSyncSortKey({ clubLog: failed() });
    const ok = qslSyncSortKey({ clubLog: synced() });
    const untracked = qslSyncSortKey(undefined);

    expect(blocked).toBeGreaterThan(retryable);
    expect(retryable).toBeGreaterThan(ok);
    expect(ok).toBeGreaterThan(untracked);
  });

  it('ranks a QSO by its worst service', () => {
    const key = qslSyncSortKey({
      clubLog: synced(),
      eqsl: failed({ failureKind: 'Auth', retryable: false }),
    });

    expect(key).toBe(qslSyncSortKey({ eqsl: failed({ failureKind: 'Auth', retryable: false }) }));
  });
});
