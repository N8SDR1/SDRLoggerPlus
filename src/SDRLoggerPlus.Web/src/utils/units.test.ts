import { describe, it, expect } from 'vitest';
import { formatDistance, toDisplayDistance } from './units';

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
