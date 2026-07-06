import { describe, it, expect } from 'vitest';
import { StrikeStore, type Strike } from './lightningStrikes';

const iso = (ms: number) => new Date(ms).toISOString();

describe('StrikeStore', () => {
  it('merges and dedupes by lat/lon/time', () => {
    const store = new StrikeStore();
    const s: Strike = { lat: 44.8, lon: -91.6, timestampUtc: iso(1_000_000), local: true };
    store.merge([s]);
    store.merge([s]);
    expect(store.active(1_000_000)).toHaveLength(1);
  });

  it('prunes local after 5 min, global after 10 min', () => {
    const store = new StrikeStore();
    const t = 1_000_000;
    store.merge([
      { lat: 44.8, lon: -91.6, timestampUtc: iso(t), local: true },
      { lat: 0, lon: 100, timestampUtc: iso(t), local: false },
    ]);
    const at7min = store.active(t + 7 * 60_000);
    expect(at7min.filter(s => s.local)).toHaveLength(0);   // local pruned
    expect(at7min.filter(s => !s.local)).toHaveLength(1);  // global still shown
  });

  it('records live-arrival time keyed by strike, surviving a later dedup merge', () => {
    const store = new StrikeStore();
    const t = 1_000_000;
    // Two distinct objects, same key: first a live push (records arrival), then a
    // backfill merge that dedupes to the existing key without arrival.
    const push: Strike = { lat: 44.8, lon: -91.6, timestampUtc: iso(t), local: true };
    const backfill: Strike = { lat: 44.8, lon: -91.6, timestampUtc: iso(t), local: true };
    store.merge([push], 5_000);
    store.merge([backfill]); // no arrivedMs — must not clear the recorded arrival
    // arrivedAt resolves by key, so it works for whichever object is retained.
    expect(store.arrivedAt(push)).toBe(5_000);
    expect(store.arrivedAt(backfill)).toBe(5_000);
  });

  it('backfill-only strikes have no arrival time (no bolt)', () => {
    const store = new StrikeStore();
    const s: Strike = { lat: 1, lon: 2, timestampUtc: iso(1_000_000), local: false };
    store.merge([s]); // backfill, no arrivedMs
    expect(store.arrivedAt(s)).toBeUndefined();
  });
});
