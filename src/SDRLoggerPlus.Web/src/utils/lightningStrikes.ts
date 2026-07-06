export interface Strike {
  lat: number;
  lon: number;
  timestampUtc: string;
  local: boolean;
}

const LOCAL_WINDOW_MS = 5 * 60_000;
const GLOBAL_WINDOW_MS = 10 * 60_000;

const key = (s: Strike) => `${s.lat.toFixed(4)}|${s.lon.toFixed(4)}|${s.timestampUtc}`;

/** Client-side rolling set of strikes for the globe. Dedupes and prunes by age. */
export class StrikeStore {
  private byKey = new Map<string, Strike>();
  private arrivedByKey = new Map<string, number>();

  /**
   * Merge strikes into the rolling set (dedupe by key). When `arrivedMs` is
   * given — a live SignalR push — record the first-arrival time keyed by strike
   * key. Keying arrival on the key (not the Strike object) makes it survive both
   * dedupe dropping a duplicate object and the effect being torn down/recreated,
   * so a strike's bolt window is stable. Backfill omits `arrivedMs` (no bolt).
   */
  merge(strikes: Strike[], arrivedMs?: number): void {
    for (const s of strikes) {
      const k = key(s);
      if (!this.byKey.has(k)) this.byKey.set(k, s);
      if (arrivedMs !== undefined && !this.arrivedByKey.has(k)) this.arrivedByKey.set(k, arrivedMs);
    }
  }

  /** Local receipt time for a strike, if it arrived via a live push (else undefined). */
  arrivedAt(s: Strike): number | undefined {
    return this.arrivedByKey.get(key(s));
  }

  /** Non-expired strikes as of nowMs (also drops expired from the store). */
  active(nowMs: number): Strike[] {
    const out: Strike[] = [];
    for (const [k, s] of this.byKey) {
      const age = nowMs - Date.parse(s.timestampUtc);
      const window = s.local ? LOCAL_WINDOW_MS : GLOBAL_WINDOW_MS;
      if (age > window) { this.byKey.delete(k); this.arrivedByKey.delete(k); }
      else out.push(s);
    }
    return out;
  }
}
