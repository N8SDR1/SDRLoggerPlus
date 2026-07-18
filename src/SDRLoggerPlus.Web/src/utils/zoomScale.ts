// Percent <-> Electron zoom-level conversion for the global UI Scale, plus the
// per-panel scale clamp. Electron zoom is log-scale: factor = 1.2 ** level.
// Both scales are DOWN-ONLY by design (operator decision): max is 100%.

export const MIN_PERCENT = 70;
export const MAX_PERCENT = 100;
export const STEP_PERCENT = 5;

export const PANEL_MIN_PERCENT = 70;
export const PANEL_STEP_PERCENT = 10;

/** Clamp a global UI-scale percentage to [70, 100]; bad input reads as 100. */
export function clampPercent(pct: number): number {
  if (!Number.isFinite(pct)) return MAX_PERCENT;
  return Math.min(MAX_PERCENT, Math.max(MIN_PERCENT, Math.round(pct)));
}

/** Clamp a per-panel scale percentage to [70, 100]; bad input reads as 100. */
export function clampPanelScale(pct: number): number {
  if (!Number.isFinite(pct)) return 100;
  return Math.min(100, Math.max(PANEL_MIN_PERCENT, Math.round(pct)));
}

/** Percentage (70–100) -> Electron zoom level (negative or 0). */
export function percentToZoomLevel(pct: number): number {
  return Math.log(clampPercent(pct) / 100) / Math.log(1.2);
}

/** Electron zoom level -> nearest whole percentage, clamped to [70, 100]. */
export function zoomLevelToPercent(level: number): number {
  if (!Number.isFinite(level)) return MAX_PERCENT;
  return clampPercent(Math.round(Math.pow(1.2, level) * 100));
}
