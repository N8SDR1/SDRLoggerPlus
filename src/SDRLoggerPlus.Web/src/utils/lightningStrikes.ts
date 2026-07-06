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

  merge(strikes: Strike[]): void {
    for (const s of strikes) {
      const k = key(s);
      if (!this.byKey.has(k)) this.byKey.set(k, s);
    }
  }

  /** Non-expired strikes as of nowMs (also drops expired from the store). */
  active(nowMs: number): Strike[] {
    const out: Strike[] = [];
    for (const [k, s] of this.byKey) {
      const age = nowMs - Date.parse(s.timestampUtc);
      const window = s.local ? LOCAL_WINDOW_MS : GLOBAL_WINDOW_MS;
      if (age > window) this.byKey.delete(k);
      else out.push(s);
    }
    return out;
  }
}
