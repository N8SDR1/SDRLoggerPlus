import { describe, it, expect } from 'vitest';
import { isValidStateProv, STATE_PROV_UNIVERSE } from './locations';

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

  it('exposes the full W/VE universe: 51 US + DC, then 13 CA provinces', () => {
    expect(STATE_PROV_UNIVERSE).toHaveLength(64);
    expect(STATE_PROV_UNIVERSE.every((c) => isValidStateProv(c))).toBe(true);
    // US block sorted first, Canadian provinces after.
    expect(STATE_PROV_UNIVERSE[0]).toBe('AK');
    expect(STATE_PROV_UNIVERSE.at(-1)).toBe('YT');
  });
});
