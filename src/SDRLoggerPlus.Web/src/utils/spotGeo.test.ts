import { describe, it, expect } from 'vitest';
import {
  stationCoords,
  spotCoords,
  computeSpotGeo,
  formatDistance,
  formatBearing,
} from './spotGeo';

describe('stationCoords', () => {
  it('prefers explicit lat/lon over the grid square', () => {
    const coords = stationCoords({ latitude: 41.7, longitude: -72.7, gridSquare: 'JO20cx' });
    expect(coords).toEqual({ lat: 41.7, lon: -72.7 });
  });

  it('falls back to the grid square when lat/lon are absent', () => {
    const coords = stationCoords({ gridSquare: 'IO63' });
    expect(coords!.lat).toBeCloseTo(53.5, 1);
    expect(coords!.lon).toBeCloseTo(-7.0, 1);
  });

  it('treats latitude 0 as a real position, not missing', () => {
    expect(stationCoords({ latitude: 0, longitude: 0 })).toEqual({ lat: 0, lon: 0 });
  });

  it('returns null with no position at all', () => {
    expect(stationCoords({})).toBeNull();
    expect(stationCoords(null)).toBeNull();
  });

  it('returns null for an unparseable grid', () => {
    expect(stationCoords({ gridSquare: 'nope' })).toBeNull();
  });
});

describe('spotCoords', () => {
  it('prefers the reported grid and marks it exact', () => {
    const result = spotCoords({ dxStation: { grid: 'IO63', lat: 10, lon: 10 } });
    expect(result!.approximate).toBe(false);
    expect(result!.coords.lat).toBeCloseTo(53.5, 1);
  });

  it('falls back to the country centroid and marks it approximate', () => {
    const result = spotCoords({ dxStation: { lat: 10, lon: 20 } });
    expect(result).toEqual({ coords: { lat: 10, lon: 20 }, approximate: true });
  });

  it('falls back to the centroid when the grid is unparseable', () => {
    const result = spotCoords({ dxStation: { grid: 'X', lat: 10, lon: 20 } });
    expect(result!.approximate).toBe(true);
  });

  it('returns null with no dxStation or no coordinates', () => {
    expect(spotCoords({})).toBeNull();
    expect(spotCoords({ dxStation: {} })).toBeNull();
  });
});

describe('computeSpotGeo', () => {
  it('measures a quarter of the globe due east', () => {
    const geo = computeSpotGeo(
      { latitude: 0, longitude: 0 },
      { dxStation: { lat: 0, lon: 90 } },
    );
    expect(geo!.distanceKm).toBeCloseTo(10007.5, 0);
    expect(geo!.bearingDeg).toBeCloseTo(90, 5);
    expect(geo!.approximate).toBe(true);
  });

  it('reports due north as bearing 0', () => {
    const geo = computeSpotGeo(
      { latitude: 0, longitude: 0 },
      { dxStation: { lat: 10, lon: 0 } },
    );
    expect(geo!.bearingDeg).toBeCloseTo(0, 5);
  });

  it('returns null when either end lacks a position', () => {
    expect(computeSpotGeo({}, { dxStation: { lat: 0, lon: 0 } })).toBeNull();
    expect(computeSpotGeo({ latitude: 0, longitude: 0 }, { dxStation: {} })).toBeNull();
  });
});

describe('formatting', () => {
  const geo = { distanceKm: 7409.6, bearingDeg: 46.7, approximate: false };

  it('rounds distance and adds thousands separators', () => {
    expect(formatDistance(geo)).toBe((7410).toLocaleString());
  });

  it('converts to miles on request', () => {
    expect(formatDistance(geo, 'mi')).toBe((4604).toLocaleString());
  });

  it('prefixes approximate distances with ~', () => {
    expect(formatDistance({ ...geo, approximate: true })).toMatch(/^~/);
  });

  it('zero-pads bearing to three digits', () => {
    expect(formatBearing(geo)).toBe('047°');
    expect(formatBearing({ ...geo, bearingDeg: 5 })).toBe('005°');
  });

  it('renders a dash when there is no geometry', () => {
    expect(formatDistance(null)).toBe('-');
    expect(formatBearing(null)).toBe('-');
  });
});
