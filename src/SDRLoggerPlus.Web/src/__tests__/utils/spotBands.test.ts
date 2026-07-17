import { describe, it, expect } from 'vitest';
import { bandClassForFrequency, getBandFromFrequency } from '../../utils/spotBands';

describe('bandClassForFrequency', () => {
  it('classifies HF bands (160m–10m)', () => {
    expect(bandClassForFrequency(1830)).toBe('HF'); // 160m
    expect(bandClassForFrequency(7040)).toBe('HF'); // 40m
    expect(bandClassForFrequency(14200)).toBe('HF'); // 20m
    expect(bandClassForFrequency(28400)).toBe('HF'); // 10m
  });

  it('classifies 6m as its own class (HF+6m rigs)', () => {
    expect(bandClassForFrequency(50125)).toBe('6M'); // 6m calling freq
    expect(bandClassForFrequency(50000)).toBe('6M'); // low edge
    expect(bandClassForFrequency(53990)).toBe('6M'); // near top edge
  });

  it('classifies VHF bands above 6m (2m, 1.25m)', () => {
    expect(bandClassForFrequency(144200)).toBe('VHF'); // 2m
    expect(bandClassForFrequency(223500)).toBe('VHF'); // 1.25m
  });

  it('the 4m gap between 6m and 2m is VHF, not 6M', () => {
    expect(bandClassForFrequency(70200)).toBe('VHF'); // 4m (region 1) — not 6m
    expect(bandClassForFrequency(54001)).toBe('VHF'); // just above 6m
  });

  it('classifies UHF bands (70cm, 33cm, 23cm and up)', () => {
    expect(bandClassForFrequency(432100)).toBe('UHF'); // 70cm
    expect(bandClassForFrequency(905000)).toBe('UHF'); // 33cm / 900 MHz
    expect(bandClassForFrequency(1296000)).toBe('UHF'); // 23cm / 1200 MHz
    expect(bandClassForFrequency(2400000)).toBe('UHF'); // 13cm microwave folds into UHF
  });

  it('classifies MF/LF low bands (2200m, 630m)', () => {
    expect(bandClassForFrequency(136)).toBe('MF'); // 2200m
    expect(bandClassForFrequency(475)).toBe('MF'); // 630m
  });

  it('returns null only below the lowest ham allocation', () => {
    expect(bandClassForFrequency(100)).toBeNull();
    expect(bandClassForFrequency(0)).toBeNull();
  });

  it('boundary at 135 kHz is null below, MF at/above', () => {
    expect(bandClassForFrequency(134)).toBeNull();
    expect(bandClassForFrequency(135)).toBe('MF');
  });

  it('boundary at 1.8 MHz is MF below, HF at/above', () => {
    expect(bandClassForFrequency(1799)).toBe('MF');
    expect(bandClassForFrequency(1800)).toBe('HF');
  });

  it('boundary at 30 MHz is HF below, VHF at/above', () => {
    expect(bandClassForFrequency(29999)).toBe('HF');
    expect(bandClassForFrequency(30000)).toBe('VHF');
  });

  it('boundary at 300 MHz is VHF below, UHF at/above', () => {
    expect(bandClassForFrequency(299999)).toBe('VHF');
    expect(bandClassForFrequency(300000)).toBe('UHF');
  });
});

describe('getBandFromFrequency still resolves labels', () => {
  it('maps known bands', () => {
    expect(getBandFromFrequency(14200)).toBe('20m');
    expect(getBandFromFrequency(432100)).toBe('70cm');
  });
  it('returns ? outside known bands', () => {
    expect(getBandFromFrequency(12345)).toBe('?');
  });
});
