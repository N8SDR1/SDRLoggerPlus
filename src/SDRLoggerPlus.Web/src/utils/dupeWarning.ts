/**
 * Display logic for the Log Entry probable-duplicate warning (advisory only —
 * logging is never blocked). The backend flags a QSO as a probable dupe when
 * the same callsign + band + mode was entered within its 30-minute window.
 */

import type { QsoResponse } from '../api/client';

/** Only bother checking once a plausible callsign is present. */
export const MIN_CALLSIGN_LENGTH = 3;

/**
 * Parse a .NET timestamp defensively: a stamp without a zone designator would
 * otherwise be read as local time, skewing the "N min ago" text by the UTC
 * offset. (LiteDB reads come back Kind-shifted; the offset form is normal.)
 */
export function parseTimestamp(iso: string): number {
  const hasZone = /[Zz]$|[+-]\d{2}:?\d{2}$/.test(iso);
  return Date.parse(hasZone ? iso : `${iso}Z`);
}

/** "just now", "1 min ago", "12 min ago" — the window is ≤30 min by design. */
export function formatAge(createdAtIso: string, nowMs: number): string {
  const t = parseTimestamp(createdAtIso);
  if (Number.isNaN(t)) return 'recently';
  const mins = Math.floor((nowMs - t) / 60_000);
  if (mins <= 0) return 'just now';
  return mins === 1 ? '1 min ago' : `${mins} min ago`;
}

/** One-line warning, e.g. "K1ABC already logged 12 min ago on 20m FT8". */
export function formatDupeWarning(dupe: QsoResponse, nowMs: number): string {
  return `${dupe.callsign} already logged ${formatAge(dupe.createdAt, nowMs)} on ${dupe.band} ${dupe.mode}`;
}
