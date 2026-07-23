// Location reference data + validators for contest exchange fields.
//
// Received S/P/C fields accept a US state / DC, a Canadian province, or "DX",
// validated strictly against the authoritative 2-letter lists.
//
// County-code data and validation used to live here too, keyed by QSO-party
// definition id. The built-in catalog no longer ships any QSO parties, so those
// tables were unreachable and have been pruned; the researched per-party
// abbreviation tables are preserved under
// docs/design/contest-research/counties/ if that catalog ever returns.

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

// ARRL/RAC Sections — the multiplier set for Sweepstakes, ARRL 160, Field Day and
// HPM (71 US + 12 RAC). The ARRL revises this list occasionally; verify against the
// current ARRL Contest Multipliers List each season. Used for the Multipliers-Needed
// "sections" universe and a soft (non-blocking) validity hint on section fields.
export const ARRL_SECTIONS = new Set([
  // US
  'CT', 'EMA', 'ME', 'NH', 'RI', 'VT', 'WMA',
  'ENY', 'NLI', 'NNJ', 'NNY', 'SNJ', 'WNY',
  'DE', 'EPA', 'MDC', 'WPA',
  'AL', 'GA', 'KY', 'NC', 'NFL', 'SC', 'SFL', 'TN', 'VA', 'WCF', 'PR', 'VI',
  'AR', 'LA', 'MS', 'NM', 'NTX', 'OK', 'STX', 'WTX',
  'EB', 'LAX', 'ORG', 'SB', 'SCV', 'SDG', 'SF', 'SJV', 'SV', 'PAC',
  'AZ', 'EWA', 'ID', 'MT', 'NV', 'OR', 'UT', 'WWA', 'WY',
  'AK', 'MI', 'OH', 'WV',
  'IL', 'IN', 'WI',
  'CO', 'IA', 'KS', 'MN', 'MO', 'ND', 'NE', 'SD',
  // RAC (Canada)
  'MAR', 'NL', 'QC', 'ONE', 'ONN', 'ONS', 'GTA', 'MB', 'SK', 'AB', 'BC', 'NT',
]);

/** True if the value is a known ARRL/RAC section (case-insensitive). */
export function isValidSection(value: string): boolean {
  return ARRL_SECTIONS.has(value.trim().toUpperCase());
}

/** All sections alphabetically, for a "needed sections" universe display. */
export const SECTION_UNIVERSE: string[] = [...ARRL_SECTIONS].sort();

