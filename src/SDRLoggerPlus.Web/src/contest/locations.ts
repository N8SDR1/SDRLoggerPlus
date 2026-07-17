// Location reference data + validators for contest exchange fields.
//
// Received S/P/C fields accept one of: a US state / DC, a Canadian province, "DX",
// or (for QSO parties) a county code of the contest's home state. States and
// provinces are validated strictly against the authoritative 2-letter lists;
// county codes come from countyData.json (generic 3-letter codes, e.g. Brown→BRO)
// and are used for autocomplete + a soft "unknown code" warning, never to block.

import countyDataRaw from './countyData.json';

export interface County {
  fips: string;
  name: string;
  code: string;
}

const countyData = countyDataRaw as Record<string, County[]>;

// 50 US states + DC.
export const US_STATES = new Set([
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL',
  'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT',
  'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
  'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
]);

// Canadian provinces / territories (2-letter).
export const CA_PROVINCES = new Set([
  'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT',
]);

/** A valid 2-letter state / province, or the literal "DX". */
export function isValidStateProv(value: string): boolean {
  const v = value.trim().toUpperCase();
  return v === 'DX' || US_STATES.has(v) || CA_PROVINCES.has(v);
}

/** Merged county list for the given home state code(s) (QSO-party home area). */
export function countiesFor(states: string[]): County[] {
  const out: County[] = [];
  for (const st of states) {
    const list = countyData[st.toUpperCase()];
    if (list) out.push(...list);
  }
  return out;
}

/** Whether a code is a known county code for any of the home states. */
export function isKnownCounty(code: string, states: string[]): boolean {
  const c = code.trim().toUpperCase();
  return countiesFor(states).some((x) => x.code.toUpperCase() === c);
}

/**
 * Autocomplete matches for a partial county entry, by code prefix or name
 * substring. Returns up to `limit` counties from the home state(s).
 */
export function matchCounties(prefix: string, states: string[], limit = 8): County[] {
  const q = prefix.trim().toUpperCase();
  if (!q) return [];
  const all = countiesFor(states);
  const byCode = all.filter((c) => c.code.toUpperCase().startsWith(q));
  const byName = all.filter(
    (c) => !c.code.toUpperCase().startsWith(q) && c.name.toUpperCase().includes(q)
  );
  return [...byCode, ...byName].slice(0, limit);
}
