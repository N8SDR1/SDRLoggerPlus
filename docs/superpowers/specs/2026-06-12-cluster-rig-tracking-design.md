# DX Cluster: Follow Rig (Band/Mode Tracking) — Design

**Date:** 2026-06-12
**Status:** Approved

## Goal

An optional "Follow rig" mode for the DX Cluster panel: when enabled, the spot
list filters to the connected rig's current band **and** mode, updating live as
the operator QSYs.

## Approach

Frontend-only filter-layer override in `ClusterPlugin.tsx`. The rig's live
state (frequency, mode) is already in `appStore` (`radioStates` +
`selectedRadioId`); no backend changes. The shared spot stream feeding the
map/globe is not affected.

## Behavior

- **Toggle:** A "Follow rig" button (crosshair icon) in the cluster filter bar,
  styled like the Rig panel's auto-reconnect toggle (active = accent tint).
- **Persistence:** Stored as `trackRig: boolean` in `ClusterSettings`
  (settingsStore → database), so it survives restarts. Default `false`.
- **When ON and a rig is connected:**
  - The effective band filter becomes the rig's current band
    (via existing `getBandFromFrequency`).
  - The effective mode filter becomes the mapped rig mode set (below).
  - The manual Band and Mode dropdowns are disabled and dimmed with a
    "Following rig" tooltip. Manual selections are preserved in state and
    take effect again when tracking is turned off.
  - Search and Status filters continue to apply normally.
- **When ON but no rig connected (or no rig state yet):** tracking is dormant —
  manual filters apply as today; the toggle renders dimmed with a
  "waiting for rig" tooltip.
- **Rig outside any known band** (`getBandFromFrequency` returns `?`):
  skip the band filter rather than showing zero spots.
- **Unrecognized rig mode:** skip the mode filter (band-only tracking).

## Rig mode → spot filter mode mapping

| Rig mode                          | Spot modes matched   |
|-----------------------------------|----------------------|
| USB, LSB, AM, FM                  | SSB                  |
| CW, CW-R                          | CW                   |
| FT8                               | FT8                  |
| FT4                               | FT4                  |
| RTTY                              | RTTY                 |
| DIGU, DIGL, DATA, PKT, PSK        | FT8, FT4, DIGI       |
| anything else                     | (no mode filter)     |

Spot-side matching reuses the existing semantics in `filteredSpots`:
USB/LSB→SSB normalization and `inferModeFromFrequency` for spots without a
reported mode.

## Components

- `ClusterPlugin.tsx`: toggle button; effective-filter computation in the
  `filteredSpots` memo; disabled state on the two dropdowns.
- `settingsStore.ts`: `trackRig` added to `ClusterSettings` (+ default).
- New pure helpers (exported for tests): `rigModeToSpotModes(rigMode)` and
  the effective band/mode resolution given rig state + manual selections.

## Testing

- Unit tests for `rigModeToSpotModes` and effective-filter resolution
  (tracking on/off, no rig, unknown band/mode) alongside existing frontend
  tests.
- Live verification against the real rig (QSY band/mode and watch the spot
  list follow) before calling it done.
