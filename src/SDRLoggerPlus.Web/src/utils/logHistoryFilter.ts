import { utcDatePart } from './qsoDateTime';
import { ALL_BANDS } from './spotBands';
import { LOG_MODES } from './qsoFieldOptions';

/**
 * Client-side filtering for the Log History panel.
 *
 * The panel loads the whole log and filters it in the browser, so the quick
 * filter row (callsign / name / band / mode / date range) is applied here
 * rather than by the backend. AG Grid's per-column filters then narrow
 * whatever this leaves.
 *
 * Band and mode are matched case-insensitively on purpose: a real log stores
 * both "40m" and "40M" (over half of the 24k reference log uses the upper-case
 * spelling), and an exact match would silently hide half the QSOs on a band.
 */

/** The QSO shape this module needs — a structural subset of QsoResponse. */
export interface FilterableQso {
  callsign?: string | null;
  band?: string | null;
  mode?: string | null;
  qsoDate?: string | null;
  name?: string | null;
  station?: { name?: string | null } | null;
}

export interface QuickFilterCriteria {
  callsign?: string;
  name?: string;
  band?: string;
  mode?: string;
  /** UTC yyyy-MM-dd, inclusive. */
  fromDate?: string;
  /** UTC yyyy-MM-dd, inclusive. */
  toDate?: string;
}

/** The name shown in the grid: the looked-up station name wins over the logged one. */
function displayName(qso: FilterableQso): string {
  return (qso.station?.name || qso.name || '').toLowerCase();
}

export function matchesQuickFilter(qso: FilterableQso, criteria: QuickFilterCriteria): boolean {
  const callsign = criteria.callsign?.trim().toUpperCase();
  if (callsign && !(qso.callsign || '').toUpperCase().includes(callsign)) return false;

  const name = criteria.name?.trim().toLowerCase();
  if (name && !displayName(qso).includes(name)) return false;

  const band = criteria.band?.trim().toLowerCase();
  if (band && (qso.band || '').trim().toLowerCase() !== band) return false;

  const mode = criteria.mode?.trim().toUpperCase();
  if (mode && (qso.mode || '').trim().toUpperCase() !== mode) return false;

  if (criteria.fromDate || criteria.toDate) {
    // A QSO with an unparseable date can't satisfy a date range, so it drops
    // out rather than leaking through as an empty string that compares low.
    const date = qso.qsoDate ? utcDatePart(qso.qsoDate) : '';
    if (!date) return false;
    if (criteria.fromDate && date < criteria.fromDate) return false;
    if (criteria.toDate && date > criteria.toDate) return false;
  }

  return true;
}

export function filterQsos<T extends FilterableQso>(
  qsos: readonly T[],
  criteria: QuickFilterCriteria,
): T[] {
  if (!hasQuickFilter(criteria)) return qsos as T[];
  return qsos.filter((q) => matchesQuickFilter(q, criteria));
}

export function hasQuickFilter(criteria: QuickFilterCriteria): boolean {
  return Boolean(
    criteria.callsign?.trim() ||
    criteria.name?.trim() ||
    criteria.band?.trim() ||
    criteria.mode?.trim() ||
    criteria.fromDate ||
    criteria.toDate,
  );
}

/** Order values by a canonical list first, then alphabetically for anything unknown. */
function orderedByReference(values: Iterable<string>, reference: readonly string[]): string[] {
  const rank = new Map(reference.map((v, i) => [v.toLowerCase(), i]));
  return [...values].sort((a, b) => {
    const ra = rank.get(a.toLowerCase()) ?? Number.MAX_SAFE_INTEGER;
    const rb = rank.get(b.toLowerCase()) ?? Number.MAX_SAFE_INTEGER;
    return ra !== rb ? ra - rb : a.localeCompare(b);
  });
}

/**
 * The bands actually present in the log, longest wavelength first.
 *
 * Derived from the data rather than hard-coded: the old fixed list stopped at
 * 2m and had no 60m, so a VHF or 60m QSO could be in the log and impossible to
 * filter for.
 */
export function deriveBandOptions(qsos: readonly FilterableQso[]): string[] {
  const seen = new Set<string>();
  for (const qso of qsos) {
    const band = (qso.band || '').trim().toLowerCase();
    if (band) seen.add(band);
  }
  return orderedByReference(seen, ALL_BANDS);
}

/**
 * The modes actually present in the log. The old fixed list had no MSK144,
 * Q65 or JS8, so meteor-scatter and weak-signal QSOs were unfilterable.
 */
export function deriveModeOptions(qsos: readonly FilterableQso[]): string[] {
  const seen = new Set<string>();
  for (const qso of qsos) {
    const mode = (qso.mode || '').trim().toUpperCase();
    if (mode) seen.add(mode);
  }
  return orderedByReference(seen, LOG_MODES);
}
