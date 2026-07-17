import { describe, it, expect } from 'vitest';
import {
  isValidStateProv, isKnownCounty, matchCounties, countiesForContest, hasOfficialCounties,
} from './locations';

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

  it('uses the official Wisconsin table (Brown = BRO, St Croix = STC)', () => {
    expect(hasOfficialCounties('qp-wisconsin')).toBe(true);
    const wi = countiesForContest('qp-wisconsin');
    expect(wi).toHaveLength(72);
    expect(wi.find((c) => c.name === 'Brown')?.code).toBe('BRO');
    expect(wi.find((c) => c.name === 'St Croix')?.code).toBe('STC');
    expect(isKnownCounty('BRO', 'qp-wisconsin')).toBe(true);
    expect(isKnownCounty('ZZZ', 'qp-wisconsin')).toBe(false);
  });

  it('autocompletes counties by code prefix and name', () => {
    const byCode = matchCounties('BR', 'qp-wisconsin');
    expect(byCode.some((c) => c.code === 'BRO' && c.name === 'Brown')).toBe(true);
    const byName = matchCounties('Milwauk', 'qp-wisconsin');
    expect(byName.some((c) => c.name === 'Milwaukee')).toBe(true);
  });

  it('returns no counties for an unknown / non-QSO-party contest', () => {
    expect(countiesForContest('cq-ww-cw')).toHaveLength(0);
    expect(countiesForContest(undefined)).toHaveLength(0);
    expect(hasOfficialCounties('cq-ww-cw')).toBe(false);
  });
});
