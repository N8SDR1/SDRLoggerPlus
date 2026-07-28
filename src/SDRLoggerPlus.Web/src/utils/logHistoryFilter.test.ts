import { describe, it, expect } from 'vitest';
import {
  matchesQuickFilter,
  filterQsos,
  hasQuickFilter,
  deriveBandOptions,
  deriveModeOptions,
  type FilterableQso,
} from './logHistoryFilter';

const qso = (over: Partial<FilterableQso> = {}): FilterableQso => ({
  callsign: 'W1AW',
  band: '20m',
  mode: 'FT8',
  qsoDate: '2026-03-15T14:30:00Z',
  ...over,
});

describe('matchesQuickFilter', () => {
  it('matches a callsign as a case-insensitive substring', () => {
    expect(matchesQuickFilter(qso(), { callsign: 'w1a' })).toBe(true);
    expect(matchesQuickFilter(qso(), { callsign: 'W1AW' })).toBe(true);
    expect(matchesQuickFilter(qso(), { callsign: 'K5' })).toBe(false);
  });

  it('matches band regardless of the spelling stored in the log', () => {
    // Real logs carry both "40m" and "40M"; an exact match would hide half of them.
    for (const stored of ['40m', '40M', ' 40m ']) {
      expect(matchesQuickFilter(qso({ band: stored }), { band: '40m' })).toBe(true);
      expect(matchesQuickFilter(qso({ band: stored }), { band: '40M' })).toBe(true);
    }
    expect(matchesQuickFilter(qso({ band: '40m' }), { band: '20m' })).toBe(false);
  });

  it('matches band exactly, not as a substring', () => {
    // "0m" must not sweep in 20m/40m/80m.
    expect(matchesQuickFilter(qso({ band: '20m' }), { band: '0m' })).toBe(false);
  });

  it('matches mode case-insensitively and exactly', () => {
    expect(matchesQuickFilter(qso({ mode: 'msk144' }), { mode: 'MSK144' })).toBe(true);
    expect(matchesQuickFilter(qso({ mode: 'FT8' }), { mode: 'FT4' })).toBe(false);
  });

  it('searches the station name, falling back to the logged name', () => {
    const withStation = qso({ name: 'logged', station: { name: 'Hiram' } });
    expect(matchesQuickFilter(withStation, { name: 'hir' })).toBe(true);
    // The station name wins, so the stale logged name no longer matches.
    expect(matchesQuickFilter(withStation, { name: 'logged' })).toBe(false);

    const loggedOnly = qso({ name: 'Hiram', station: null });
    expect(matchesQuickFilter(loggedOnly, { name: 'hiram' })).toBe(true);
  });

  it('treats the date range as inclusive on both ends', () => {
    const march15 = qso({ qsoDate: '2026-03-15T14:30:00Z' });
    expect(matchesQuickFilter(march15, { fromDate: '2026-03-15' })).toBe(true);
    expect(matchesQuickFilter(march15, { toDate: '2026-03-15' })).toBe(true);
    expect(matchesQuickFilter(march15, { fromDate: '2026-03-16' })).toBe(false);
    expect(matchesQuickFilter(march15, { toDate: '2026-03-14' })).toBe(false);
  });

  it('uses the UTC date, so a late-evening UTC QSO stays on its own day', () => {
    // 23:50 UTC on the 15th is the 15th, even where local time is already the 16th.
    const lateUtc = qso({ qsoDate: '2026-03-15T23:50:00Z' });
    expect(matchesQuickFilter(lateUtc, { fromDate: '2026-03-15', toDate: '2026-03-15' })).toBe(true);
  });

  it('drops a QSO with an unusable date only when a date range is set', () => {
    const undated = qso({ qsoDate: 'not-a-date' });
    expect(matchesQuickFilter(undated, { fromDate: '2026-01-01' })).toBe(false);
    expect(matchesQuickFilter(undated, { callsign: 'W1AW' })).toBe(true);
  });

  it('requires every supplied criterion to match', () => {
    const target = qso({ band: '6m', mode: 'MSK144' });
    expect(matchesQuickFilter(target, { band: '6m', mode: 'MSK144' })).toBe(true);
    expect(matchesQuickFilter(target, { band: '6m', mode: 'FT8' })).toBe(false);
  });

  it('ignores blank and whitespace-only criteria', () => {
    expect(matchesQuickFilter(qso(), { callsign: '   ', band: '', mode: undefined })).toBe(true);
  });

  it('does not match a missing field against a real filter value', () => {
    expect(matchesQuickFilter(qso({ band: null }), { band: '20m' })).toBe(false);
    expect(matchesQuickFilter(qso({ callsign: null }), { callsign: 'W1AW' })).toBe(false);
  });
});

describe('filterQsos', () => {
  const log = [
    qso({ callsign: 'W1AW', band: '20m', mode: 'FT8' }),
    qso({ callsign: 'K5ZZ', band: '6m', mode: 'MSK144' }),
    qso({ callsign: 'N8SDR', band: '6M', mode: 'msk144' }),
  ];

  it('returns the original list untouched when nothing is filtered', () => {
    expect(filterQsos(log, {})).toBe(log);
  });

  it('narrows to the QSOs matching every criterion, whatever their casing', () => {
    const result = filterQsos(log, { band: '6m', mode: 'MSK144' });
    expect(result.map((q) => q.callsign)).toEqual(['K5ZZ', 'N8SDR']);
  });

  it('can filter down to nothing', () => {
    expect(filterQsos(log, { band: '160m' })).toEqual([]);
  });
});

describe('hasQuickFilter', () => {
  it('is false for empty, missing and whitespace-only criteria', () => {
    expect(hasQuickFilter({})).toBe(false);
    expect(hasQuickFilter({ callsign: '', name: '   ', band: undefined })).toBe(false);
  });

  it('is true as soon as any criterion carries a value', () => {
    expect(hasQuickFilter({ callsign: 'W1' })).toBe(true);
    expect(hasQuickFilter({ fromDate: '2026-01-01' })).toBe(true);
  });
});

describe('deriveBandOptions', () => {
  it('lists only the bands present, longest wavelength first', () => {
    const options = deriveBandOptions([
      qso({ band: '6m' }),
      qso({ band: '160m' }),
      qso({ band: '20m' }),
    ]);
    expect(options).toEqual(['160m', '20m', '6m']);
  });

  it('folds casing and whitespace together into one option', () => {
    const options = deriveBandOptions([
      qso({ band: '40m' }),
      qso({ band: '40M' }),
      qso({ band: ' 40m ' }),
    ]);
    expect(options).toEqual(['40m']);
  });

  it('includes bands the old hard-coded list omitted', () => {
    const options = deriveBandOptions([qso({ band: '60m' }), qso({ band: '70cm' })]);
    expect(options).toEqual(['60m', '70cm']);
  });

  it('keeps an unrecognised band rather than dropping it, sorted last', () => {
    const options = deriveBandOptions([qso({ band: 'satellite' }), qso({ band: '20m' })]);
    expect(options).toEqual(['20m', 'satellite']);
  });

  it('skips blank and missing bands', () => {
    expect(deriveBandOptions([qso({ band: '' }), qso({ band: null }), qso({ band: '  ' })])).toEqual([]);
  });
});

describe('deriveModeOptions', () => {
  it('lists the modes present in canonical order, upper-cased', () => {
    const options = deriveModeOptions([
      qso({ mode: 'msk144' }),
      qso({ mode: 'CW' }),
      qso({ mode: 'FT8' }),
    ]);
    expect(options).toEqual(['CW', 'FT8', 'MSK144']);
  });

  it('surfaces MSK144 — the mode the old fixed dropdown could not filter for', () => {
    expect(deriveModeOptions([qso({ mode: 'MSK144' })])).toContain('MSK144');
  });

  it('keeps a mode the app does not know about, sorted last', () => {
    const options = deriveModeOptions([qso({ mode: 'VARA HF' }), qso({ mode: 'CW' })]);
    expect(options).toEqual(['CW', 'VARA HF']);
  });
});
