import { describe, it, expect } from 'vitest';
import {
  formatDistance,
  toDisplayDistance,
  distanceUnitFor,
  speedUnitFor,
  resolveSpeedUnit,
  formatSpeed,
  formatTemperature,
} from './units';

describe('formatDistance', () => {
  it('formats km unchanged', () => {
    expect(formatDistance(4218, 'km')).toBe('4,218 km');
  });

  it('converts to miles', () => {
    expect(formatDistance(4218, 'mi')).toBe('2,621 mi'); // 4218 * 0.621371 ≈ 2621
  });

  it('rounds and thousands-separates', () => {
    expect(formatDistance(16697, 'km')).toBe('16,697 km');
    expect(toDisplayDistance(1609.34, 'mi')).toBe(1000);
  });

  it('returns empty for null / NaN', () => {
    expect(formatDistance(null, 'km')).toBe('');
    expect(formatDistance(undefined, 'mi')).toBe('');
    expect(formatDistance(NaN, 'km')).toBe('');
  });
});

describe('unit system helpers', () => {
  it('maps the master system to concrete units', () => {
    expect(distanceUnitFor('imperial')).toBe('mi');
    expect(distanceUnitFor('metric')).toBe('km');
    expect(speedUnitFor('imperial')).toBe('mph');
    expect(speedUnitFor('metric')).toBe('kph');
  });

  it("resolves an 'auto' override to the master, else the explicit unit", () => {
    expect(resolveSpeedUnit('auto', 'imperial')).toBe('mph');
    expect(resolveSpeedUnit('auto', 'metric')).toBe('kph');
    expect(resolveSpeedUnit('kph', 'imperial')).toBe('kph'); // explicit wins
    expect(resolveSpeedUnit('mph', 'metric')).toBe('mph');
  });
});

describe('formatSpeed', () => {
  it('keeps mph, converts to kph', () => {
    expect(formatSpeed(51, 'mph')).toBe('51 mph');
    expect(formatSpeed(51, 'kph')).toBe('82 kph'); // 51 * 1.609344 ≈ 82
  });

  it('returns empty for null / NaN', () => {
    expect(formatSpeed(null, 'mph')).toBe('');
    expect(formatSpeed(NaN, 'kph')).toBe('');
  });
});

describe('formatTemperature', () => {
  it('keeps °F, converts to °C', () => {
    expect(formatTemperature(72, 'imperial')).toBe('72°F');
    expect(formatTemperature(72, 'metric')).toBe('22°C'); // (72-32)*5/9 ≈ 22
    expect(formatTemperature(32, 'metric')).toBe('0°C');
  });

  it('returns empty for null / NaN', () => {
    expect(formatTemperature(null, 'imperial')).toBe('');
    expect(formatTemperature(undefined, 'metric')).toBe('');
  });
});
