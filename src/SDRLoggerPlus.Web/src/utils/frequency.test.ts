import { describe, it, expect } from 'vitest';
import { spotKhzToMhzString, spotKhzToHz } from './frequency';

describe('spotKhzToMhzString', () => {
  it('converts a kHz spot to the MHz form string (6 dp)', () => {
    // Regression: a 14.250 MHz spot arrives as 14250 kHz and must display
    // "14.250000" — NOT "0.014250" (the old ÷1e6 kHz-as-Hz bug).
    expect(spotKhzToMhzString(14250)).toBe('14.250000');
    expect(spotKhzToMhzString(14250)).not.toBe('0.014250');
  });

  it('handles other bands', () => {
    expect(spotKhzToMhzString(7040)).toBe('7.040000');
    expect(spotKhzToMhzString(1810)).toBe('1.810000');
    expect(spotKhzToMhzString(0)).toBe('0.000000');
  });
});

describe('spotKhzToHz', () => {
  it('converts a kHz spot to Hz for band lookups', () => {
    expect(spotKhzToHz(14250)).toBe(14250000);
    expect(spotKhzToHz(7040)).toBe(7040000);
  });
});
