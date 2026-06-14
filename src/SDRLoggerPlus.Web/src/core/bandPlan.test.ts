import { describe, it, expect } from 'vitest';
import { ITU_BANDS, bandForFrequency, checkOutOfBand } from './bandPlan';

describe('ITU_BANDS', () => {
  it('defines all three regions', () => {
    expect(Object.keys(ITU_BANDS)).toEqual(['1', '2', '3']);
  });

  it('region 2 (Americas) 80m extends to 4.0 MHz; region 1 stops at 3.8', () => {
    expect(ITU_BANDS[2]['80m']).toEqual([3.5, 4.0]);
    expect(ITU_BANDS[1]['80m']).toEqual([3.5, 3.8]);
  });

  it('region 1 2m stops at 146; regions 2/3 extend to 148', () => {
    expect(ITU_BANDS[1]['2m'][1]).toBe(146.0);
    expect(ITU_BANDS[2]['2m'][1]).toBe(148.0);
  });
});

describe('bandForFrequency', () => {
  it('maps frequencies to bands using loose ranges', () => {
    expect(bandForFrequency(14.074)).toBe('20m');
    expect(bandForFrequency(7.2)).toBe('40m');
    expect(bandForFrequency(50.313)).toBe('6m');
    expect(bandForFrequency(146.52)).toBe('2m');
  });

  it('returns null outside any band', () => {
    expect(bandForFrequency(12.0)).toBeNull();
    expect(bandForFrequency(100.0)).toBeNull();
  });

  it('loose ranges catch slightly out-of-band VFO (so OOB can warn)', () => {
    expect(bandForFrequency(7.35)).toBe('40m'); // above region-2 7.300 edge but in loose range
  });
});

describe('checkOutOfBand', () => {
  it('in-band returns null', () => {
    expect(checkOutOfBand(14.2, 2)).toBeNull();
    expect(checkOutOfBand(7.25, 2)).toBeNull(); // legal in region 2
  });

  it('flags out-of-band for the configured region', () => {
    // 7.25 MHz is legal in region 2 but above region 1's 7.200 edge
    const r = checkOutOfBand(7.25, 1);
    expect(r).not.toBeNull();
    expect(r!.band).toBe('40m');
    expect(r!.lo).toBe(7.0);
    expect(r!.hi).toBe(7.2);
  });

  it('flags below the band edge too', () => {
    const r = checkOutOfBand(1.795, 2); // below 160m 1.800 edge, loose range catches it
    expect(r?.band).toBe('160m');
  });

  it('frequency outside any loose band returns null (no band, no warning)', () => {
    expect(checkOutOfBand(12.0, 2)).toBeNull();
  });

  it('unknown region falls back to region 2', () => {
    expect(checkOutOfBand(7.25, 9 as 1 | 2 | 3)).toBeNull(); // legal under region-2 plan
  });
});
