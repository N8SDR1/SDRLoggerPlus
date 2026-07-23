import { describe, it, expect } from 'vitest';
import { isValidStateProv, STATE_PROV_UNIVERSE, isValidSection, SECTION_UNIVERSE } from './locations';

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

  it('validates ARRL/RAC sections case-insensitively', () => {
    expect(isValidSection('WI')).toBe(true);   // Wisconsin
    expect(isValidSection('stx')).toBe(true);  // South Texas, case-insensitive
    expect(isValidSection('GTA')).toBe(true);  // Greater Toronto Area (RAC)
    expect(isValidSection('ONS')).toBe(true);  // Ontario South (RAC)
  });

  it('rejects non-sections (incl. 2-letter states that are not sections)', () => {
    expect(isValidSection('ZZ')).toBe(false);
    expect(isValidSection('CA')).toBe(false);  // "CA" is a state, not a section abbrev
    expect(isValidSection('')).toBe(false);
  });

  it('exposes a sorted section universe with US + RAC entries', () => {
    // 71 US + 12 RAC — a plausible-size roster, sorted and self-consistent.
    expect(SECTION_UNIVERSE).toHaveLength(83);
    expect(SECTION_UNIVERSE.every((s) => isValidSection(s))).toBe(true);
    expect([...SECTION_UNIVERSE]).toEqual([...SECTION_UNIVERSE].sort());
  });
});
