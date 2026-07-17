import { describe, it, expect } from 'vitest';
import {
  ssnFromSfi,
  midpoint,
  calculateMuf,
  calculateLuf,
  calculateReliability,
  bandOpenState,
  assessPropagation,
} from '../../utils/propagationGate';

describe('ssnFromSfi', () => {
  it('floors at 0 for low SFI', () => {
    expect(ssnFromSfi(67)).toBe(0);
    expect(ssnFromSfi(50)).toBe(0);
  });
  it('scales with SFI (OpenHamClock formula)', () => {
    expect(ssnFromSfi(150)).toBe(Math.round((150 - 67) / 0.97)); // 86
  });
});

describe('midpoint', () => {
  it('returns a point between two coordinates', () => {
    const m = midpoint(40, -80, 50, 0);
    expect(m.lat).toBeGreaterThan(40);
    expect(m.lat).toBeLessThan(60);
    expect(m.lon).toBeGreaterThan(-80);
    expect(m.lon).toBeLessThan(0);
  });
  it('midpoint of a point with itself is itself', () => {
    const m = midpoint(30, 20, 30, 20);
    expect(m.lat).toBeCloseTo(30, 4);
    expect(m.lon).toBeCloseTo(20, 4);
  });
});

describe('calculateReliability', () => {
  it('is 0 above the MUF', () => {
    expect(calculateReliability(30, 20, 5, 2, 1)).toBe(0);
  });
  it('is 0 below the LUF', () => {
    expect(calculateReliability(3, 20, 5, 2, 1)).toBe(0);
  });
  it('peaks near FOT (0.85 * MUF) and stays within 0-99', () => {
    const fot = 0.85 * 20;
    const r = calculateReliability(fot, 20, 5, 0, 1);
    expect(r).toBeGreaterThan(50);
    expect(r).toBeLessThanOrEqual(99);
  });
  it('K-index storms degrade reliability', () => {
    const fot = 0.85 * 20;
    const calm = calculateReliability(fot, 20, 5, 0, 1);
    const storm = calculateReliability(fot, 20, 5, 6, 1);
    expect(storm).toBeLessThan(calm);
  });
});

describe('calculateMuf / calculateLuf ordering', () => {
  it('MUF exceeds LUF for a typical daytime mid-latitude path', () => {
    const ssn = ssnFromSfi(140);
    const muf = calculateMuf(ssn, 3000, 45, 40); // zenith 40 = daytime
    const luf = calculateLuf(ssn, 3000, 40, 2);
    expect(muf).toBeGreaterThan(luf);
  });
  it('night LUF collapses (D-layer gone)', () => {
    const ssn = ssnFromSfi(140);
    const night = calculateLuf(ssn, 3000, 100, 2); // zenith >= 90 = night
    expect(night).toBeLessThan(4);
  });
});

describe('bandOpenState buckets', () => {
  it('maps reliability into open/marginal/closed', () => {
    expect(bandOpenState(80)).toBe('open');
    expect(bandOpenState(50)).toBe('open');
    expect(bandOpenState(35)).toBe('marginal');
    expect(bandOpenState(20)).toBe('marginal');
    expect(bandOpenState(5)).toBe('closed');
  });
});

describe('assessPropagation graceful degradation', () => {
  it('returns unknown with no coordinates or indices', () => {
    const a = assessPropagation({
      freqMHz: 14.2,
      deLat: null,
      deLon: null,
      dxLat: null,
      dxLon: null,
      sfi: null,
      kIndex: null,
    });
    expect(a.open).toBe('unknown');
    expect(a.reliability).toBeNull();
    expect(a.grayLine).toBeNull();
    expect(a.summary).toBe('No propagation data');
  });

  it('computes a band-open score when path + indices are present', () => {
    const a = assessPropagation({
      freqMHz: 14.2,
      deLat: 40,
      deLon: -83,
      dxLat: 52,
      dxLon: 13, // Germany
      sfi: 150,
      kIndex: 2,
      now: new Date('2026-07-13T15:00:00Z'), // fixed instant for determinism
    });
    expect(['open', 'marginal', 'closed']).toContain(a.open);
    expect(a.reliability).not.toBeNull();
    expect(a.reliability!).toBeGreaterThanOrEqual(0);
    expect(a.reliability!).toBeLessThanOrEqual(99);
  });

  it('never fabricates a band-open score without indices', () => {
    const a = assessPropagation({
      freqMHz: 7.1,
      deLat: 40,
      deLon: -83,
      dxLat: 52,
      dxLon: 13,
      sfi: null,
      kIndex: null,
      now: new Date('2026-07-13T15:00:00Z'),
    });
    expect(a.open).toBe('unknown');
    expect(a.reliability).toBeNull();
  });
});
