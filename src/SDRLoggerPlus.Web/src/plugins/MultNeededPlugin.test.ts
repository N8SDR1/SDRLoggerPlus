import { describe, it, expect } from 'vitest';
import { parseEntry } from './MultNeededPlugin';

describe('MultNeededPlugin parseEntry', () => {
  it('parses a bare value', () => {
    expect(parseEntry('OH')).toEqual({ value: 'OH', band: null, mode: null });
  });

  it('parses value@band (per-band rule)', () => {
    expect(parseEntry('14@20m')).toEqual({ value: '14', band: '20m', mode: null });
  });

  it('parses value+mode (per-mode rule) without leaking "+CW" into the value', () => {
    expect(parseEntry('OH+CW')).toEqual({ value: 'OH', band: null, mode: 'CW' });
  });

  it('parses value@band+mode (per-band and per-mode)', () => {
    expect(parseEntry('OH@20m+SSB')).toEqual({ value: 'OH', band: '20m', mode: 'SSB' });
  });

  it('lowercases band and uppercases mode', () => {
    const p = parseEntry('5@20M+ry');
    expect(p.band).toBe('20m');
    expect(p.mode).toBe('RY');
  });
});
