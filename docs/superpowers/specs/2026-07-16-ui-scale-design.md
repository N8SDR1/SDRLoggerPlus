# UI Scale (whole-app zoom) — Design

**Date:** 2026-07-16
**Problem:** On a 1920×1080 monitor, panels (Log Entry and most others) need more
vertical room than they get — content is cut off or forces scrolling. The panels
already use small type; the practical fix is letting the operator scale the whole
UI down (~85–90%) so more fits.

**Key finding:** The Electron shell already has complete zoom plumbing — IPC
handlers `get-zoom-level` / `set-zoom-level` (main.js), `saveZoomLevel()` to the
per-machine config, restore-on-launch, and `window.electron.setZoomLevel()` /
`getZoomLevel()` exposed in preload.js — but **nothing calls it**. No menu item,
no shortcut, no UI. Ctrl+wheel fires the `zoom-changed` listener, which only
*saves* the (unchanged) level. This design exposes the existing plumbing; it adds
no new persistence or IPC.

## Scope

- **In:** Electron menu + keyboard zoom, Ctrl+wheel zoom, a UI Scale control in
  Settings → Appearance. Electron only — the control hides in a plain browser.
- **Out:** CSS-based scaling for non-Electron browsers (native browser zoom
  covers that). Per-panel responsive/reflow work. The clipping bugs in
  Contests / Log History / DX Cluster (no scrollbar on overflow) — separate
  follow-up, they're broken at any scale.

## Design

### 1. Electron shell — `src/SDRLoggerPlus.Desktop/main.js`

- **View menu items** (custom items, NOT Electron's `zoomIn/zoomOut/resetZoom`
  roles, because the built-in roles bypass `saveZoomLevel()` and wouldn't
  persist):
  - Zoom In — `CommandOrControl+=`
  - Zoom Out — `CommandOrControl+-`
  - Reset Zoom — `CommandOrControl+0`
  - Each computes the new level, clamps, calls
    `mainWindow.webContents.setZoomLevel(level)` and `saveZoomLevel(level)`.
- **Step:** ±0.5 zoom-level per action (≈ ±9.5%).
- **Clamp:** level −2.0 … +1.5 (≈ 69% … 131%). All entry points clamp
  (menu, wheel, IPC `set-zoom-level`, restore-on-launch).
- **Ctrl+wheel:** the existing `zoom-changed` listener currently only saves;
  change it to *apply* ±0.5 per event (`zoomDirection === 'in' ? +0.5 : −0.5`),
  clamp, set, save.
- **IPC:** unchanged (`get-zoom-level`, `set-zoom-level`) except `set-zoom-level`
  gains the clamp.
- A `zoom-level-changed` notification from main → renderer (via
  `webContents.send`) so the Settings control stays in sync when zoom changes
  from the menu/wheel while Settings is open. Preload exposes
  `onZoomLevelChanged(callback)`.

### 2. Settings → Appearance — `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx`

- New "UI Scale" row in the existing Appearance section:
  - Current percentage readout (e.g. **85%**)
  - **−** / **+** buttons stepping 5 percentage points, clamped 70–130%
  - **Reset** button → 100%
  - Hint text: "Ctrl + = / − / 0 or Ctrl + mouse wheel also work anywhere."
- Reads initial value via `window.electron.getZoomLevel()`; writes via
  `window.electron.setZoomLevel(level)`; subscribes to `onZoomLevelChanged` to
  stay current.
- **Visibility:** rendered only when `window.electron?.setZoomLevel` exists
  (same Electron detection pattern used elsewhere in the web app). In a plain
  browser the row does not render.

### 3. Percent ↔ level conversion — `src/SDRLoggerPlus.Web/src/utils/zoomScale.ts` (new)

Electron zoom level is log-scale: `factor = 1.2^level`.

- `percentToZoomLevel(pct: number): number` → `Math.log(pct / 100) / Math.log(1.2)`
- `zoomLevelToPercent(level: number): number` → `Math.round(Math.pow(1.2, level) * 100)`
- Clamp constants exported: `MIN_PERCENT = 70`, `MAX_PERCENT = 130`,
  `STEP_PERCENT = 5`.
- Pure functions — unit-tested.

### 4. Persistence — no changes

Zoom stays in the Electron per-machine config (existing `saveZoomLevel` /
`getStoredZoomLevel`), NOT the backend settings DB. Rationale: display scale is
a property of this machine's monitor; it must not ride along with settings
import/export to another machine. No Contracts change, no migration.

## Error handling

- All zoom paths clamp to [−2.0, +1.5]; a corrupt stored value is clamped on
  restore.
- Settings control guards on `window.electron` — absent (browser) it renders
  nothing; IPC failures fall back to leaving the readout unchanged.

## Testing

- **Unit (vitest):** `zoomScale.test.ts` — round-trips (100% ↔ 0, 85% → level →
  85%), clamping, monotonicity.
- **Manual in the Electron app:** menu items + accelerators, Ctrl+wheel, the
  Settings stepper, persistence across an app restart, clamp at both ends.
  (Shell behavior isn't reachable by the web test suite or headless browser.)

## Affected files

| File | Change |
|---|---|
| `src/SDRLoggerPlus.Desktop/main.js` | View-menu zoom items, wheel-zoom apply, clamp, change notification |
| `src/SDRLoggerPlus.Desktop/preload.js` | expose `onZoomLevelChanged` |
| `src/SDRLoggerPlus.Web/src/utils/zoomScale.ts` | new — percent↔level conversion + clamps |
| `src/SDRLoggerPlus.Web/src/utils/zoomScale.test.ts` | new — unit tests |
| `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx` | UI Scale row in Appearance (Electron-only) |
