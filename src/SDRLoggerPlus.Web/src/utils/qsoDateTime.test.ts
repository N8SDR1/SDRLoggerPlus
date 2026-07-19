import { describe, it, expect } from 'vitest';
import { utcDatePart, toUtcInstant } from './qsoDateTime';

describe('utcDatePart', () => {
  it('takes the UTC date, not the local one, across the UTC-midnight boundary', () => {
    // 23:52 at -05:00 is 04:52 the NEXT day in UTC. Slicing the string directly
    // (the old behaviour) would yield 2026-06-26 and move the QSO back a day.
    expect(utcDatePart('2026-06-26T23:52:00-05:00')).toBe('2026-06-27');
  });

  it('agrees with a plain string slice when the two dates coincide', () => {
    expect(utcDatePart('2026-06-27T14:14:45.868-05:00')).toBe('2026-06-27');
  });

  it('handles a value already expressed in UTC', () => {
    expect(utcDatePart('2026-06-27T04:52:00Z')).toBe('2026-06-27');
  });

  it('handles a positive offset crossing backwards over UTC midnight', () => {
    // 01:30 at +09:00 is 16:30 the PREVIOUS day in UTC.
    expect(utcDatePart('2026-06-27T01:30:00+09:00')).toBe('2026-06-26');
  });
});

describe('toUtcInstant', () => {
  it('combines a UTC date and HHMM time into an instant', () => {
    expect(toUtcInstant('2026-06-27', '0452')).toBe('2026-06-27T04:52:00.000Z');
  });

  it('accepts HHMMSS and keeps the seconds', () => {
    expect(toUtcInstant('2026-06-27', '045233')).toBe('2026-06-27T04:52:33.000Z');
  });

  it('tolerates a colon-separated time', () => {
    expect(toUtcInstant('2026-06-27', '04:52')).toBe('2026-06-27T04:52:00.000Z');
  });

  it('round-trips a QSO through edit without moving it', () => {
    // The exact failure this fixes: open the modal on an evening QSO, change
    // nothing, save. The instant must come back out unchanged.
    const original = '2026-06-26T23:52:00-05:00';
    const shown = utcDatePart(original);          // what the modal displays
    const saved = toUtcInstant(shown, '0452');    // what submit sends
    expect(new Date(saved).getTime()).toBe(new Date(original).getTime());
  });

  it('returns an empty string when the date is missing rather than inventing one', () => {
    expect(toUtcInstant('', '0452')).toBe('');
  });
});
