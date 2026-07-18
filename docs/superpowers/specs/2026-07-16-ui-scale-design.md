# UI Scale (global + per-panel) — Design

**Date:** 2026-07-16 (revised after live prototype on the operator's 1080p monitor)
**Problem:** On a 1920×1080 monitor, panels (Log Entry and most others) need more
vertical room than they get — content is cut off or forces scrolling. The panels
already use small type; the practical fix is letting the operator scale the UI
down (~85–90%) so more fits — **globally** (whole app) and **per panel**
(each panel independently), which compose multiplicatively.

**Prototype verdict (operator-tested):** per-panel stepper in the tabset header
approved ("I like it"); canvas panels must be exempt (scaling broke the 2D map's
globe geometry); two standalone fit bugs found and fixed during the demo
(committed separately as `fix(map)`: min-h-[500px] removal + ClampedFlyout).

**Key finding:** The Electron shell already has complete zoom plumbing — IPC
handlers `get-zoom-level` / `set-zoom-level` (main.js), `saveZoomLevel()` to the
per-machine config, restore-on-launch, and `window.electron.setZoomLevel()` /
`getZoomLevel()` exposed in preload.js — but **nothing calls it**. No menu item,
no shortcut, no UI. Ctrl+wheel fires the `zoom-changed` listener, which only
*saves* the (unchanged) level. This design exposes the existing plumbing; it adds
no new persistence or IPC.

## Scope

- **In:**
  - **Global scale:** Electron menu + keyboard zoom and a UI Scale control in
    Settings → Appearance. Electron only — the control hides in a plain browser.
  - **Per-panel scale:** a small − % + stepper in each tabset header (acts on
    the active tab), CSS `zoom` on the panel's content wrapper, persisted in
    the FlexLayout tab config. Works in browser and Electron alike.
- **Out:**
  - **Ctrl+wheel global zoom — deliberately dropped.** The Panadapter and
    Globe already bind Ctrl+wheel to zoom themselves; a global binding would
    fight them. (The existing `zoom-changed` save-only listener stays as-is.)
  - CSS-based **global** scaling for non-Electron browsers (native browser
    zoom covers that).
  - Per-panel responsive/reflow work. The clipping bugs in
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
- **Clamp:** level −2.0 … 0 (≈ 69% … 100% — scaling **down only**; the operator asked for no upscaling). All entry points clamp
  (menu, wheel, IPC `set-zoom-level`, restore-on-launch).
- **Ctrl+wheel:** NOT wired globally (see Scope — conflicts with the
  Panadapter/Globe Ctrl+wheel zoom). The existing save-only `zoom-changed`
  listener is left untouched.
- **IPC:** unchanged (`get-zoom-level`, `set-zoom-level`) except `set-zoom-level`
  gains the clamp.
- A `zoom-level-changed` notification from main → renderer (via
  `webContents.send`) so the Settings control stays in sync when zoom changes
  from the menu/wheel while Settings is open. Preload exposes
  `onZoomLevelChanged(callback)`.

### 2. Settings → Appearance — `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx`

- New "UI Scale" row in the existing Appearance section:
  - Current percentage readout (e.g. **85%**)
  - **−** / **+** buttons stepping 5 percentage points, clamped 70–100%
  - **Reset** button → 100%
  - Hint text: "Ctrl + = / − / 0 also work anywhere."
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
- Clamp constants exported: `MIN_PERCENT = 70`, `MAX_PERCENT = 100`,
  `STEP_PERCENT = 5`.
- Pure functions — unit-tested.

### 4. Per-panel scale — `src/SDRLoggerPlus.Web/src/App.tsx`

Prototype validated live by the operator; the final version replaces the
prototype's React-state map with FlexLayout-config persistence.

- **Plugin registry:** `PluginDef` gains `scalable?: boolean`. `'map'`,
  `'globe-3d'`, and `'panadapter'` set `scalable: false` — canvas/WebGL panels
  measure their containers in real pixels, and CSS zoom breaks their geometry
  (the 2D map's embedded globe circle visibly tore in the prototype). Their
  content renders unwrapped and their tabset shows no stepper.
- **Factory wrapper:** for scalable plugins the factory wraps the component:
  `<div style={{ zoom: scale / 100, height: '100%' }}>` where
  `scale = (node.getConfig()?.scale as number | undefined) ?? 100`.
- **Stepper UI (`onRenderTabSet`):** pushed to `renderValues.buttons` (before
  the sticky "+" add-panel button) when the tabset's selected tab is a
  scalable plugin: `−` button, `NN%` readout (click = reset to 100), `+`
  button. Same look as the prototype: `text-[10px] font-mono text-dark-300`,
  `flexlayout__tab_toolbar_button` class on the buttons.
- **Steps/clamp:** 10-point steps, clamped 70–100% (down-only, matching the
  global scale decision).
- **Persistence:** the scale lives in the tab node's `config`
  (`Actions.updateNodeAttributes(tabId, { config: { ...config, scale } })`),
  which serializes into the layout JSON and rides the existing debounced
  layout save to the backend — survives restart, per layout, no new storage.
- **Composition:** per-panel CSS zoom multiplies with global Electron zoom
  (panel 90% in an app at 90% renders at 81%) — intended behavior, no
  compensation logic.

### 5. Remove the superseded "compact" controls

Per-panel scaling replaces the app's compact-density features; the operator
confirmed both go:

- **Settings → Appearance "Compact Mode" toggle** — a dead switch: nothing
  reads `appearance.compactMode`. Remove the toggle (SettingsPanel.tsx), the
  `compactMode` field from `AppearanceSettings` (settingsStore.ts), the
  `[BsonElement("compactMode")]` property (Contracts `Settings.cs`), and the
  references in `settingsStore.test.ts` / `StartupProviderTests.cs`. Old
  stored values are simply ignored by LiteDB after removal — no migration.
- **Per-panel CompactToggle** — the working density buttons on Log History,
  Contests, DX Cluster, and Rig. Remove `CompactToggle.tsx`,
  `usePanelCompact.ts`, and their uses in the four plugins; compact-branch
  styling (`compact ? … : …`) collapses to the non-compact branch. The
  localStorage `panelCompact:*` keys become orphans — harmless.

### 6. Persistence (global) — no changes

Zoom stays in the Electron per-machine config (existing `saveZoomLevel` /
`getStoredZoomLevel`), NOT the backend settings DB. Rationale: display scale is
a property of this machine's monitor; it must not ride along with settings
import/export to another machine. No Contracts change, no migration.

## Error handling

- All zoom paths clamp to [−2.0, 0]; a corrupt stored value is clamped on
  restore.
- Settings control guards on `window.electron` — absent (browser) it renders
  nothing; IPC failures fall back to leaving the readout unchanged.
- Per-panel: a missing/non-numeric `config.scale` reads as 100; values are
  clamped to [70, 100] wherever read, so a hand-edited layout can't break
  rendering.

## Testing

- **Unit (vitest):** `zoomScale.test.ts` — round-trips (100% ↔ 0, 85% → level →
  85%), clamping, monotonicity. Per-panel: a small test for the clamp/step
  helper if extracted; the FlexLayout wiring is exercised manually.
- **Manual in the Electron app:** menu items + accelerators, the Settings
  stepper, per-panel stepper (scale, reset, exempt panels show none),
  persistence across an app restart (both global zoom and per-panel scales),
  clamp at both ends. (Shell behavior isn't reachable by the web test suite
  or headless browser.)

## Affected files

| File | Change |
|---|---|
| `src/SDRLoggerPlus.Desktop/main.js` | View-menu zoom items, clamp, change notification |
| `src/SDRLoggerPlus.Desktop/preload.js` | expose `onZoomLevelChanged` |
| `src/SDRLoggerPlus.Web/src/utils/zoomScale.ts` | new — percent↔level conversion + clamps |
| `src/SDRLoggerPlus.Web/src/utils/zoomScale.test.ts` | new — unit tests |
| `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx` | UI Scale row in Appearance (Electron-only) |
| `src/SDRLoggerPlus.Web/src/App.tsx` | `scalable` flag, factory zoom wrapper, tabset stepper, config persistence |
| `src/SDRLoggerPlus.Web/src/components/CompactToggle.tsx` | DELETE (superseded by panel scale) |
| `src/SDRLoggerPlus.Web/src/hooks/usePanelCompact.ts` | DELETE (superseded by panel scale) |
| `LogHistoryPlugin / ContestsPlugin / ClusterPlugin / RigPlugin` | remove compact toggle + branches |
| `src/SDRLoggerPlus.Contracts/Models/Settings.cs` | remove dead `CompactMode` |
| `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` | remove dead `compactMode` |

Already committed during prototyping (independent fixes): `MapPlugin.tsx`
min-h-[500px] removal + ClampedFlyout (`5b0dc30`).
