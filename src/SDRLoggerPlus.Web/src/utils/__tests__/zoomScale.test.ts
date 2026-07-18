import { describe, it, expect } from 'vitest';
import {
  MIN_PERCENT, MAX_PERCENT, STEP_PERCENT,
  PANEL_MIN_PERCENT, PANEL_STEP_PERCENT,
  clampPercent, clampPanelScale, percentToZoomLevel, zoomLevelToPercent,
} from '../zoomScale';

describe('zoomScale', () => {
  it('exports the agreed clamps and steps', () => {
    expect(MIN_PERCENT).toBe(70);
    expect(MAX_PERCENT).toBe(100);
    expect(STEP_PERCENT).toBe(5);
    expect(PANEL_MIN_PERCENT).toBe(70);
    expect(PANEL_STEP_PERCENT).toBe(10);
  });

  it('maps 100% to zoom level 0 and back', () => {
    expect(percentToZoomLevel(100)).toBeCloseTo(0, 10);
    expect(zoomLevelToPercent(0)).toBe(100);
  });

  it('round-trips every stepper percentage', () => {
    for (let pct = MIN_PERCENT; pct <= MAX_PERCENT; pct += STEP_PERCENT) {
      expect(zoomLevelToPercent(percentToZoomLevel(pct))).toBe(pct);
    }
  });

  it('is monotonic (smaller percent -> more negative level)', () => {
    expect(percentToZoomLevel(70)).toBeLessThan(percentToZoomLevel(85));
    expect(percentToZoomLevel(85)).toBeLessThan(percentToZoomLevel(100));
  });

  it('clampPercent clamps to [70, 100] and defaults bad input to 100', () => {
    expect(clampPercent(120)).toBe(100);
    expect(clampPercent(50)).toBe(70);
    expect(clampPercent(85)).toBe(85);
    expect(clampPercent(Number.NaN)).toBe(100);
  });

  it('clampPanelScale clamps to [70, 100] and defaults bad input to 100', () => {
    expect(clampPanelScale(105)).toBe(100);
    expect(clampPanelScale(10)).toBe(70);
    expect(clampPanelScale(90)).toBe(90);
    expect(clampPanelScale(Number.NaN)).toBe(100);
  });
});
