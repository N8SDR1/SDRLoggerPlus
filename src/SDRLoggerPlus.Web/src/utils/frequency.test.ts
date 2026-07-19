import { describe, it, expect } from 'vitest';
import {
  spotKhzToMhzString,
  spotKhzToHz,
  formMhzToStoredKhz,
  rigHzToStoredKhz,
  storedKhzToFormMhz,
} from './frequency';

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

describe('formMhzToStoredKhz', () => {
  it('converts the MHz form value to the stored kHz value', () => {
    // Regression: the form submitted its MHz value straight through, so the
    // ADIF exporters divided by 1000 a second time and QRZ received 0.050
    // for a 50.125 MHz QSO.
    expect(formMhzToStoredKhz(50.125)).toBe(50125);
    expect(formMhzToStoredKhz(14.074)).toBe(14074);
    expect(formMhzToStoredKhz(7.268)).toBe(7268);
  });

  it('does not leak binary float noise into storage', () => {
    // 14.074 * 1000 === 14074.000000000002 without rounding.
    expect(formMhzToStoredKhz(14.074)).toBe(14074);
    expect(Number.isInteger(formMhzToStoredKhz(28.4))).toBe(true);
  });

  it('keeps sub-kHz precision', () => {
    expect(formMhzToStoredKhz(14.075287)).toBeCloseTo(14075.287, 6);
  });

  it('handles the 630m case, where kHz is below 1000', () => {
    expect(formMhzToStoredKhz(0.474)).toBe(474);
  });
});

describe('rigHzToStoredKhz', () => {
  it('converts rig/Combo Hz to stored kHz', () => {
    expect(rigHzToStoredKhz(14074000)).toBe(14074);
    expect(rigHzToStoredKhz(50125000)).toBe(50125);
    // NOT 14.074 — that was the bug (Hz treated as if it should become MHz).
    expect(rigHzToStoredKhz(14074000)).not.toBe(14.074);
  });
});

describe('storedKhzToFormMhz', () => {
  it('round-trips with formMhzToStoredKhz', () => {
    for (const mhz of [50.125, 14.074, 7.268, 0.474, 144.174, 14.075287]) {
      expect(storedKhzToFormMhz(formMhzToStoredKhz(mhz))).toBeCloseTo(mhz, 6);
    }
  });

  it('shows stored kHz as MHz in the edit form', () => {
    expect(storedKhzToFormMhz(14074)).toBe(14.074);
    expect(storedKhzToFormMhz(50313)).toBe(50.313);
  });
});
