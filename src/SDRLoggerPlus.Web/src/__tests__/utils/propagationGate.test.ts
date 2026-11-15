import { describe, it, expect } from 'vitest';
import {
  ssnFromSfi,
  midpoint,
  pathMuf,
  hopMFactor,
  calculateLuf,
  calculateReliability,
  bandOpenState,
  assessPropagation,
  estimateFoF2,
  controlPoints,
  calibrationForPath,
  type IonosondeReading,
} from '../../utils/propagationGate';
import { calculateDistance } from '../../utils/maidenhead';

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
  it('is 0 well above the MUF', () => {
    expect(calculateReliability(30, 20, 5, 2, 1)).toBe(0);
  });
  it('tapers above the MUF instead of a cliff (the MUF is a median)', () => {
    const atMuf = calculateReliability(20, 20, 5, 0, 1);
    const above = calculateReliability(22, 20, 5, 0, 1);
    expect(atMuf).toBeGreaterThan(40);
    expect(atMuf).toBeLessThan(55);
    expect(above).toBeGreaterThan(5);
    expect(above).toBeLessThan(atMuf);
  });
  it('matches the server: 28 MHz on a 25.5 MHz MUF, 2 hops, quiet', () => {
    // Same inputs as PropagationServiceTests.Reliability_MatchesClientPort.
    expect(calculateReliability(28, 25.5, 3.9, 0, 2)).toBe(20);
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

describe('pathMuf / calculateLuf', () => {
  it('MUF exceeds LUF for a typical daytime mid-latitude path', () => {
    const ssn = ssnFromSfi(140);
    const noon = new Date('2025-06-21T12:00:00Z');
    const muf = pathMuf(ssn, 51.5, -0.1, 41.9, 12.5, noon); // London → Rome
    const luf = calculateLuf(ssn, 1430, 30, 2);
    expect(muf).toBeGreaterThan(luf);
  });
  it('a multi-hop path is limited by its weakest control point, with no per-hop cut', () => {
    const ssn = ssnFromSfi(110);
    const t = new Date('2026-11-15T13:30:00Z');
    const weakest = Math.min(...controlPoints(39.7, -84.5, 51, 10).map((p) => estimateFoF2(ssn, p.lat, p.lon, t)));
    const hopKm = calculateDistance(39.7, -84.5, 51, 10) / 2;
    expect(pathMuf(ssn, 39.7, -84.5, 51, 10, t)).toBeCloseTo(weakest * hopMFactor(hopKm), 9);
  });
  it('night LUF drops below 160m (D-layer gone)', () => {
    const ssn = ssnFromSfi(140);
    expect(calculateLuf(ssn, 3000, 100, 1)).toBeLessThan(1.8); // zenith >= 90 = night
  });
  it('LUF is continuous through sunset', () => {
    const ssn = ssnFromSfi(120);
    for (let z = 80; z < 100; z += 0.25) {
      expect(Math.abs(calculateLuf(ssn, 3000, z + 0.25, 1) - calculateLuf(ssn, 3000, z, 1))).toBeLessThan(0.25);
    }
  });
  it('a quiet-night 160m path is workable, not 0%', () => {
    const a = assessPropagation({ freqMHz: 1.83, deLat: 40, deLon: -84, dxLat: 47, dxLon: -53, sfi: 120, kIndex: 1, now: new Date('2026-01-15T05:00:00Z') });
    expect(a.reliability!).toBeGreaterThan(20);
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

describe('live ionosonde calibration', () => {
  const now = new Date('2026-11-15T15:00:00Z');
  const ssn = ssnFromSfi(110);
  // Ohio → Germany: ~7,000 km, so two control points.
  const de = { lat: 39.7, lon: -84.5 };
  const dx = { lat: 51, lon: 10 };
  const at = (lat: number, lon: number, mult: number, name = 'S'): IonosondeReading => ({
    name, lat, lon, foF2: mult * estimateFoF2(ssn, lat, lon, now), timeUtc: now.toISOString(),
  });

  it('no readings: factors 1, model only', () => {
    expect(calibrationForPath([], ssn, de.lat, de.lon, dx.lat, dx.lon, now)).toEqual({ factors: [1, 1], source: 'model' });
  });

  it('long paths use two control points 2,000 km in from each end', () => {
    const pts = controlPoints(de.lat, de.lon, dx.lat, dx.lon);
    expect(pts).toHaveLength(2);
    expect(calculateDistance(de.lat, de.lon, pts[0].lat, pts[0].lon)).toBeCloseTo(2000, -1);
    expect(calculateDistance(dx.lat, dx.lon, pts[1].lat, pts[1].lon)).toBeCloseTo(2000, -1);
    expect(controlPoints(39.7, -84.5, 42.4, -71.1)).toHaveLength(1);
  });

  it('ionosondes at the control points reading 2x the model double the factor (measured)', () => {
    const readings = controlPoints(de.lat, de.lon, dx.lat, dx.lon).map((p, i) => at(p.lat, p.lon, 2, `S${i}`));
    const cal = calibrationForPath(readings, ssn, de.lat, de.lon, dx.lat, dx.lon, now);
    expect(cal.source).toBe('measured');
    cal.factors.forEach((f) => expect(f).toBeCloseTo(2, 2));
  });

  it('far-away ionosondes still correct the level, labelled calibrated', () => {
    const readings = [at(-35.3, 149.1, 1.8), at(-19.6, 146.8, 1.8), at(-42.9, 147.3, 1.8)];
    const cal = calibrationForPath(readings, ssn, de.lat, de.lon, dx.lat, dx.lon, now);
    expect(cal.source).toBe('calibrated');
    cal.factors.forEach((f) => expect(f).toBeCloseTo(1.8, 2));
  });

  it('the calibrated forecast opens a 20m path the plain model calls dead', () => {
    const readings = controlPoints(de.lat, de.lon, dx.lat, dx.lon).map((p) => at(p.lat, p.lon, 2));
    const args = { freqMHz: 14.025, deLat: de.lat, deLon: de.lon, dxLat: dx.lat, dxLon: dx.lon, sfi: 110, kIndex: 0, now };
    const plain = assessPropagation(args);
    const live = assessPropagation({ ...args, ionosondes: readings });
    expect(plain.reliability).toBe(0);
    expect(live.reliability!).toBeGreaterThan(plain.reliability!);
    expect(live.mufMHz!).toBeCloseTo(plain.mufMHz! * 2, 1);
    expect(live.source).toBe('measured');
    expect(live.summary).toContain('live ionosondes near this path');
  });
});
