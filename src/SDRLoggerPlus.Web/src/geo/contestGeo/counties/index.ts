import { PREFIX_BY_NAME } from '../entityPrefixes';

// Per-state county reference + boundary data. County codes are 3-letter
// abbreviations derived from the county name (first letters, collisions
// resolved) — this matches the convention most state QSO parties use for their
// county multipliers (e.g. Ohio ADA/ALL/ASH), though a few parties use their
// own scheme. Boundaries come from US Census cartographic data, simplified.

export interface County {
  fips: string;
  name: string;
  code: string;
}

let dataCache: Record<string, County[]> | null = null;

/** Load the (bundled-lazy) name/code/FIPS table for every US state, keyed by USPS abbr. */
export async function loadCountyData(): Promise<Record<string, County[]>> {
  if (!dataCache) {
    const mod = await import('./countyData.json');
    dataCache = (mod.default ?? mod) as unknown as Record<string, County[]>;
  }
  return dataCache;
}

/** Lazy-load one state's county GeoJSON (each state is its own chunk). */
export async function loadCountyGeo(abbr: string): Promise<unknown> {
  const mod = await import(`./states/${abbr.toLowerCase()}.geo.json`);
  return mod.default ?? mod;
}

// US state names → abbr, longest first so "West Virginia" beats "Virginia" and
// "New Mexico" isn't shadowed. Provinces/MX states are filtered out by the
// caller (they have no county file).
const STATE_NAMES: [string, string][] = Object.entries(PREFIX_BY_NAME)
  .sort((a, b) => b[0].length - a[0].length);

/**
 * Resolve a state QSO party's definition name (e.g. "Ohio QSO Party",
 * "Washington Salmon Run") to the USPS abbr of the state whose counties it
 * uses, or null for multi-state / non-US events. `hasCounties` gates on an
 * actual county file existing.
 */
export function stateAbbrForContest(
  defName: string | undefined,
  hasCounties: (abbr: string) => boolean,
): string | null {
  if (!defName) return null;
  for (const [name, abbr] of STATE_NAMES) {
    if (defName.startsWith(name) && hasCounties(abbr)) return abbr;
  }
  return null;
}
