import { describe, it, expect } from 'vitest';
import { dbmToSUnit, S_METER_FLOOR_DBM, S_METER_CEIL_DBM } from './smeter';

// S-meter convention (HF): S9 = -73 dBm, 6 dB per S-unit below S9,
// dB-linear "over S9" above. The fraction maps the meter sweep
// (S0..S9+40) onto 0..1 for needle positioning.
describe('dbmToSUnit', () => {
  it('maps -73 dBm to S9', () => {
    expect(dbmToSUnit(-73).label).toBe('S9');
  });

  it('maps -121 dBm to S1', () => {
    expect(dbmToSUnit(-121).label).toBe('S1');
  });

  it('maps -53 dBm to S9+20', () => {
    expect(dbmToSUnit(-53).label).toBe('S9+20');
  });

  it('clamps below the floor to S0', () => {
    expect(dbmToSUnit(-200).label).toBe('S0');
    expect(dbmToSUnit(-200).fraction).toBe(0);
  });

  it('clamps above the ceiling to S9+40', () => {
    expect(dbmToSUnit(0).label).toBe('S9+40');
    expect(dbmToSUnit(0).fraction).toBe(1);
  });

  it('rounds intermediate readings to the nearest S-unit label', () => {
    // -76 dBm is half an S-unit under S9 -> S8 or S9 acceptable only via
    // explicit rule: we round to nearest, so -76 -> S9 (3 dB away) not S8 (3 dB away).
    // Tie goes up; pin the exact behavior.
    expect(dbmToSUnit(-76).label).toBe('S9');
    expect(dbmToSUnit(-80).label).toBe('S8');
  });

  it('produces a monotonically increasing fraction', () => {
    let prev = -1;
    for (let dbm = S_METER_FLOOR_DBM; dbm <= S_METER_CEIL_DBM; dbm += 1) {
      const f = dbmToSUnit(dbm).fraction;
      expect(f).toBeGreaterThanOrEqual(prev);
      expect(f).toBeGreaterThanOrEqual(0);
      expect(f).toBeLessThanOrEqual(1);
      prev = f;
    }
  });

  it('reports over-nine readings in 10 dB steps', () => {
    expect(dbmToSUnit(-63).label).toBe('S9+10');
    expect(dbmToSUnit(-43).label).toBe('S9+30');
  });

  it('supports an extended over-nine ceiling for round meters', () => {
    expect(dbmToSUnit(-13, 60).label).toBe('S9+60');
    expect(dbmToSUnit(0, 60).fraction).toBe(1);
    expect(dbmToSUnit(0).label).toBe('S9+40');
  });
});
