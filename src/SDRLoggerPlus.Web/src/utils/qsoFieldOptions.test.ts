import { describe, it, expect } from 'vitest';
import { selectOptionsFor, LOG_MODES } from './qsoFieldOptions';

const BANDS = ['160m', '80m', '40m', '20m', '6m'];

describe('selectOptionsFor', () => {
  it('selects the exact option when the value already matches', () => {
    const { value, options } = selectOptionsFor(BANDS, '40m');
    expect(value).toBe('40m');
    expect(options).toEqual(BANDS);
  });

  it('matches case-insensitively and returns the list spelling', () => {
    // The real trigger: over half this log's QSOs store an upper-case band
    // ("40M", "10M", "80M") against a lower-case dropdown, so the select
    // rendered blank and the required field then blocked the whole save.
    const { value, options } = selectOptionsFor(BANDS, '40M');
    expect(value).toBe('40m');
    expect(options).toEqual(BANDS);
  });

  it('keeps a value the list does not contain, rather than dropping it', () => {
    const { value, options } = selectOptionsFor(LOG_MODES, 'VARA HF');
    expect(value).toBe('VARA HF');
    expect(options[0]).toBe('VARA HF');
    expect(options).toHaveLength(LOG_MODES.length + 1);
  });

  it('offers an unknown value first so it reads as the current setting', () => {
    const { options } = selectOptionsFor(BANDS, '2200m');
    expect(options).toEqual(['2200m', ...BANDS]);
  });

  it('treats an empty, missing or blank value as nothing selected', () => {
    for (const empty of ['', '   ', null, undefined]) {
      const { value, options } = selectOptionsFor(BANDS, empty);
      expect(value).toBe('');
      expect(options).toEqual(BANDS);
    }
  });

  it('trims a stored value that carries stray whitespace', () => {
    expect(selectOptionsFor(BANDS, ' 40M ').value).toBe('40m');
    expect(selectOptionsFor(LOG_MODES, ' VARA HF ').value).toBe('VARA HF');
  });

  it('never adds a duplicate option for a value it already knows', () => {
    for (const band of ['40m', '40M', ' 40m ']) {
      expect(selectOptionsFor(BANDS, band).options).toHaveLength(BANDS.length);
    }
  });

  it('does not mutate the caller list', () => {
    const original = [...BANDS];
    selectOptionsFor(BANDS, 'VARA HF');
    expect(BANDS).toEqual(original);
  });

  it('covers the modes this log actually contains', () => {
    // Every mode below appears in the real 24k log. Ones in LOG_MODES must
    // resolve to the canonical spelling; the rest must survive verbatim.
    const inLog = ['FT8', 'SSB', 'FT4', 'CW', 'JT65', 'DATA', 'MFSK', 'PSK31',
      'USB', 'RTTY', 'PKT', 'JT9', 'FM', 'HELL', 'LSB', 'DIGITALVOICE', 'AM',
      'OLIVIA', 'MSK144', 'FreeDV', 'VARA HF', 'PSK'];
    for (const mode of inLog) {
      const { value, options } = selectOptionsFor(LOG_MODES, mode);
      expect(value.toLowerCase()).toBe(mode.toLowerCase());
      expect(options).toContain(value);
    }
  });
});
