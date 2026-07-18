// Location reference data + validators for contest exchange fields.
//
// Received S/P/C fields accept one of: a US state / DC, a Canadian province, "DX",
// or (for QSO parties) a county code of the contest's home area. States/provinces
// are validated strictly against the authoritative 2-letter lists. County codes
// come from countyData.json — the OFFICIAL per-party abbreviation tables gathered
// from each sponsor's rules (built by scripts/build-county-data.mjs, keyed by
// contest definition id), with a generic first-3-letter fallback for any party
// whose official list wasn't reachable. When a party's table is official, unknown
// county codes are treated as invalid; for a generic-fallback party they only warn.

import countyDataRaw from './countyData.json';

export interface County {
  name: string;
  code: string;
}

interface PartyCounties {
  official: boolean;
  source: string;
  counties: County[];
}

const countyData = countyDataRaw as Record<string, PartyCounties>;

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

// The full W/VE state+province universe (US states/DC alpha, then Canadian
// provinces alpha), for a "needed" multiplier display where unworked entries read
// as gaps. Used by the Multipliers panel the way CQ zones show a fixed 1–40 grid.
export const STATE_PROV_UNIVERSE: string[] = [
  ...[...US_STATES].sort(),
  ...[...CA_PROVINCES].sort(),
];

/** The county table for a contest (by definition id), or null when it has none. */
function partyCounties(defId: string | undefined): PartyCounties | null {
  return (defId && countyData[defId]) || null;
}

/** County list for a contest's home area (empty for non-QSO-party contests). */
export function countiesForContest(defId: string | undefined): County[] {
  return partyCounties(defId)?.counties ?? [];
}

/** Whether this contest's county codes come from the official sponsor table. */
export function hasOfficialCounties(defId: string | undefined): boolean {
  return partyCounties(defId)?.official ?? false;
}

/** Whether a code is a known county code for the contest. */
export function isKnownCounty(code: string, defId: string | undefined): boolean {
  const c = code.trim().toUpperCase();
  return countiesForContest(defId).some((x) => x.code.toUpperCase() === c);
}

/**
 * Autocomplete matches for a partial county entry, by code prefix or name
 * substring, from the contest's county table.
 */
export function matchCounties(prefix: string, defId: string | undefined, limit = 8): County[] {
  const q = prefix.trim().toUpperCase();
  if (!q) return [];
  const all = countiesForContest(defId);
  const byCode = all.filter((c) => c.code.toUpperCase().startsWith(q));
  const byName = all.filter(
    (c) => !c.code.toUpperCase().startsWith(q) && c.name.toUpperCase().includes(q)
  );
  return [...byCode, ...byName].slice(0, limit);
}
