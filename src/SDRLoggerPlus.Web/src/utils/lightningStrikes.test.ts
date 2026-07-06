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
});
