import { describe, expect, it } from 'vitest';
import { dbmToArcFraction, formatFrequencyHz } from './RoundMeter';

describe('RoundMeter helpers', () => {
  it('maps and clamps dBm values across the configured arc range', () => {
    expect(dbmToArcFraction(-130, -130, -10)).toBe(0);
    expect(dbmToArcFraction(-70, -130, -10)).toBe(0.5);
    expect(dbmToArcFraction(-10, -130, -10)).toBe(1);
    expect(dbmToArcFraction(-150, -130, -10)).toBe(0);
    expect(dbmToArcFraction(5, -130, -10)).toBe(1);
    expect(dbmToArcFraction(-70, -10, -10)).toBe(0);
  });

  it('formats radio frequency as dot-separated Hz groups', () => {
    expect(formatFrequencyHz(14_074_000)).toBe('14.074.000');
    expect(formatFrequencyHz(7_040_000)).toBe('7.040.000');
  });
});
