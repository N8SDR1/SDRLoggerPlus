import { describe, it, expect } from 'vitest';
import { isValidStateProv, isKnownCounty, matchCounties, countiesFor } from './locations';

describe('contest location validators', () => {
  it('accepts US states, DC, Canadian provinces, and DX', () => {
    expect(isValidStateProv('WI')).toBe(true);
    expect(isValidStateProv('tx')).toBe(true); // case-insensitive
    expect(isValidStateProv('DC')).toBe(true);
    expect(isValidStateProv('ON')).toBe(true);
    expect(isValidStateProv('DX')).toBe(true);
  });

  it('rejects invalid 2-letter codes', () => {
    expect(isValidStateProv('ZZ')).toBe(false);
    expect(isValidStateProv('XY')).toBe(false);
  });

  it('knows Wisconsin has Brown county coded BRO', () => {
    expect(isKnownCounty('BRO', ['WI'])).toBe(true);
    const wi = countiesFor(['WI']);
    expect(wi.find((c) => c.name === 'Brown')?.code).toBe('BRO');
    expect(wi).toHaveLength(72);
  });

  it('flags an unknown county code', () => {
    expect(isKnownCounty('ZZZ', ['WI'])).toBe(false);
  });

  it('autocompletes counties by code prefix and name', () => {
    const byCode = matchCounties('BR', ['WI']);
    expect(byCode.some((c) => c.code === 'BRO' && c.name === 'Brown')).toBe(true);
    const byName = matchCounties('Milwauk', ['WI']);
    expect(byName.some((c) => c.name === 'Milwaukee')).toBe(true);
  });

  it('merges counties across multi-state home areas (regionals)', () => {
    const ne = countiesFor(['CT', 'ME', 'MA', 'NH', 'RI', 'VT']);
    expect(ne.length).toBeGreaterThan(60);
  });
});
