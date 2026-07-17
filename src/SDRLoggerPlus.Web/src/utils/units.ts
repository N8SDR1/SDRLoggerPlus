// App-wide unit formatting. A single master preference — the "unit system"
// (imperial or metric) — drives every physical readout: distance, satellite
// range/altitude/footprint, lightning proximity, wind speed and temperature.
// Individual features (e.g. the weather wind switch) may override it, but their
// default is 'auto', meaning "follow the master".
//
// Everything is stored/computed in canonical units internally (distance in km,
// speed in mph, temperature in °F) and converted here for display.

export type UnitSystem = 'imperial' | 'metric';
export type DistanceUnit = 'km' | 'mi';
export type SpeedUnit = 'mph' | 'kph';
/** A per-feature speed override: an explicit unit, or 'auto' to follow the master. */
export type SpeedUnitPref = 'auto' | SpeedUnit;

const KM_TO_MI = 0.621371;
const MPH_TO_KPH = 1.609344;

/** The distance unit implied by the master system. */
export function distanceUnitFor(system: UnitSystem): DistanceUnit {
  return system === 'imperial' ? 'mi' : 'km';
}

/** The speed unit implied by the master system. */
export function speedUnitFor(system: UnitSystem): SpeedUnit {
  return system === 'imperial' ? 'mph' : 'kph';
}

/** Resolve a per-feature speed override ('auto' follows the master) to a concrete unit. */
export function resolveSpeedUnit(pref: SpeedUnitPref, system: UnitSystem): SpeedUnit {
  return pref === 'auto' ? speedUnitFor(system) : pref;
}

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

/**
 * Format a wind/speed value supplied in mph into the given unit, e.g. "51 mph"
 * or "82 kph". Returns '' for null/NaN.
 */
export function formatSpeed(mph: number | null | undefined, unit: SpeedUnit): string {
  if (mph == null || Number.isNaN(mph)) return '';
  const v = unit === 'kph' ? mph * MPH_TO_KPH : mph;
  return `${Math.round(v)} ${unit}`;
}

/**
 * Format a temperature supplied in °F into the chosen system, e.g. "72°F" or
 * "22°C". Returns '' for null/NaN.
 */
export function formatTemperature(fahrenheit: number | null | undefined, system: UnitSystem): string {
  if (fahrenheit == null || Number.isNaN(fahrenheit)) return '';
  return system === 'imperial'
    ? `${Math.round(fahrenheit)}°F`
    : `${Math.round((fahrenheit - 32) * 5 / 9)}°C`;
}
