// App-wide distance formatting. Internally everything computes distances in
// kilometres; this converts to the user's chosen display unit.

export type DistanceUnit = 'km' | 'mi';

const KM_TO_MI = 0.621371;

/** Convert km → the display unit (rounded number only). */
export function toDisplayDistance(km: number, unit: DistanceUnit): number {
  return Math.round(unit === 'mi' ? km * KM_TO_MI : km);
}

/**
 * Format a km distance for display in the chosen unit, e.g. "4,218 km" or
 * "2,621 mi". Returns '' for null/NaN so callers can drop it cleanly.
 */
export function formatDistance(km: number | null | undefined, unit: DistanceUnit): string {
  if (km == null || Number.isNaN(km)) return '';
  return `${toDisplayDistance(km, unit).toLocaleString()} ${unit}`;
}
