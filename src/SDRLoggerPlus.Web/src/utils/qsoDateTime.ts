/**
 * QSO date/time helpers.
 *
 * A QSO is logged in UTC — TimeOn is UTC by ADIF convention, and the Edit form
 * labels its time field "Time (UTC)". But the API serialises QsoDate with the
 * server's local offset (LiteDB returns DateTimes as Kind=Local, so
 * 2026-06-27T04:52Z comes back as 2026-06-26T23:52-05:00 — the same instant,
 * rendered locally). Reading the date off that string gives the LOCAL date,
 * which for evening QSOs is a day behind the UTC date the contact was actually
 * made on.
 *
 * These helpers keep the UI in one frame: show the UTC date, and send back a
 * full UTC instant rather than a bare date that would discard the time.
 */

/** The UTC calendar date (yyyy-MM-dd) of an instant, whatever offset it arrives in. */
export function utcDatePart(isoDate: string): string {
  const parsed = new Date(isoDate);
  if (Number.isNaN(parsed.getTime())) return '';
  return parsed.toISOString().slice(0, 10);
}

/**
 * Combine a UTC date (yyyy-MM-dd) and a UTC time (HHMM, HHMMSS, or colon-separated)
 * into an ISO instant.
 *
 * Sending the date alone would deserialise to midnight server-side and silently
 * drop the time of day, so the two are always submitted together.
 */
export function toUtcInstant(utcDate: string, timeOn: string): string {
  if (!utcDate) return '';
  const digits = (timeOn ?? '').replace(/\D/g, '').padEnd(6, '0').slice(0, 6);
  const hh = digits.slice(0, 2);
  const mm = digits.slice(2, 4);
  const ss = digits.slice(4, 6);
  const instant = new Date(`${utcDate}T${hh}:${mm}:${ss}Z`);
  if (Number.isNaN(instant.getTime())) return '';
  return instant.toISOString();
}
