/**
 * Distance/bearing from the operator's station to a DX spot.
 *
 * Both inputs are resolved most-precise-first: the station prefers explicit
 * lat/lon over its grid square (same precedence as the map and globe panels),
 * and a spot prefers its reported grid over the cty.dat country centroid —
 * a centroid can be a thousand km off for large entities, so it is only a
 * fallback and is flagged as approximate.
 */
import { gridToLatLon, calculateDistance, calculateBearing } from './maidenhead';

export interface LatLon {
  lat: number;
  lon: number;
}

export interface SpotGeo {
  distanceKm: number;
  /** Great-circle azimuth from the station, degrees true (0 = North). */
  bearingDeg: number;
  /** True when the spot position came from a country centroid, not a grid. */
  approximate: boolean;
}

export interface StationLike {
  latitude?: number | null;
  longitude?: number | null;
  gridSquare?: string | null;
}

export interface SpotLike {
  dxStation?: {
    grid?: string;
    lat?: number;
    lon?: number;
  };
}

/** Operator position: explicit lat/lon wins, then the grid square. */
export function stationCoords(station: StationLike | null | undefined): LatLon | null {
  if (!station) return null;
  if (station.latitude != null && station.longitude != null) {
    return { lat: station.latitude, lon: station.longitude };
  }
  if (station.gridSquare) return gridToLatLon(station.gridSquare);
  return null;
}

/** Spot position: reported grid wins, then the coarse country centroid. */
export function spotCoords(spot: SpotLike | null | undefined): { coords: LatLon; approximate: boolean } | null {
  const dx = spot?.dxStation;
  if (!dx) return null;
  if (dx.grid) {
    const fromGrid = gridToLatLon(dx.grid);
    if (fromGrid) return { coords: fromGrid, approximate: false };
  }
  if (dx.lat != null && dx.lon != null) {
    return { coords: { lat: dx.lat, lon: dx.lon }, approximate: true };
  }
  return null;
}

/**
 * Great-circle distance (km) and bearing (°) from station to spot.
 * Returns null when either end has no usable position.
 */
export function computeSpotGeo(
  station: StationLike | null | undefined,
  spot: SpotLike | null | undefined,
): SpotGeo | null {
  const from = stationCoords(station);
  const to = spotCoords(spot);
  if (!from || !to) return null;
  return {
    distanceKm: calculateDistance(from.lat, from.lon, to.coords.lat, to.coords.lon),
    bearingDeg: calculateBearing(from.lat, from.lon, to.coords.lat, to.coords.lon),
    approximate: to.approximate,
  };
}

/** Distance for the grid column: whole km, thousands-separated, "~" when approximate. */
export function formatDistance(geo: SpotGeo | null, unit: 'km' | 'mi' = 'km'): string {
  if (!geo) return '-';
  const value = unit === 'mi' ? geo.distanceKm * 0.621371 : geo.distanceKm;
  return `${geo.approximate ? '~' : ''}${Math.round(value).toLocaleString()}`;
}

/** Bearing for the grid column: zero-padded degrees, e.g. "047°". */
export function formatBearing(geo: SpotGeo | null): string {
  if (!geo) return '-';
  return `${Math.round(geo.bearingDeg).toString().padStart(3, '0')}°`;
}
