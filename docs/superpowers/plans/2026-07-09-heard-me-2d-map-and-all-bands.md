# Heard-Me on 2D Map + All-Bands + Timeframe — Implementation Plan

**Goal:** Extend the "Heard Me" feature onto the 2D map and add an "All bands" option + timeframe control shared across the 3D globe and 2D map.

**Scope (confirmed with user):**
1. **All bands** option in the band picker (value `'all'` → no band filter), on **both** 3D globe + 2D map. Overrides rig-follow when chosen.
2. **Timeframe** control surfaced for the 2D layers (reuses existing `heardMePskWindowMinutes` / `heardMeRbnWindowMinutes`).
3. **2D PSK Layer** — reuse & upgrade the existing `PskReporterOverlay`: move its toggle into the map's **Overlays** menu as "PSK Layer", add band + all-bands + timeframe, make receiver dots **click → report popup**, and back `showPskOverlay` with a Contracts field (currently frontend-only / silently dropped).
4. **2D Heard-Me RBN Layer** — new overlay using `GET /api/rbn/heardme`, station→skimmer lines + clickable dots, in the Overlays menu as "Heard Me — RBN"; new backend-backed setting.

**Architecture:** The 2D overlays reuse the same backend endpoints as the 3D globe (`/api/pskreporter/reports?callsign=&minutes=`, `/api/rbn/heardme?callsign=&band=&minutes=`) and the same shared settings (`heardMeBand`, `heardMePskWindowMinutes`, `heardMeRbnWindowMinutes`). Band filtering reuses the `heardMe.ts` band helpers.

## Global Constraints
- New settings backend-backed (Contracts `Settings.cs` `[BsonElement]` + store + UI). Fix the existing `showPskOverlay` silent-drop while here.
- Reuse existing components/helpers (`PskReporterOverlay`, `heardMe.ts`, `api.getRbnHeardMe`) — don't duplicate.
- `'all'` band = no filter, and must override the rig-follow band.
- 2D map is Leaflet DOM → **verify live** via preview tools after each visible change.
- Branch `feat/globe-heard-me-layers`. One commit per task.

## Tasks

### Task 1 — "All bands" option (shared, 3D first)
- `heardMe.ts` / band handling: treat `band === 'all'` (or null) as no filter (already null-safe in `buildHeardMeArcs`).
- `useHeardMeReports.ts`: `activeBand = heardMeBand === 'all' ? null : (rigBand || heardMeBand)`.
- `SettingsPanel.tsx` `HEARD_ME_BANDS`: prepend `'all'` rendered as "All bands".
- Commit.

### Task 2 — Settings/Contracts plumbing for 2D layers
- Contracts `MapSettings`: add `ShowPskOverlay` (fix silent-drop) + `Show2dHeardMeRbn` (both `[BsonElement]`).
- Store `MapSettings`: add `show2dHeardMeRbn: boolean` (default false); `showPskOverlay` already exists.
- Commit.

### Task 3 — Upgrade PskReporterOverlay (band + timeframe + clickable popup)
- Props: add `band: string | null`, `minutes: number`.
- Fetch `api.getPskReports(callsign, minutes)`; filter reports by band (skip when `band` null/`'all'`).
- Replace hover tooltip on the receiver `circleMarker` with `bindPopup` (click → report: who heard you, freq/band, mode, SNR, age).
- MapPlugin: pass `band={heardMeBand==='all'?null:heardMeBand}` and `minutes={heardMePskWindowMinutes}`.
- Verify live (enable PSK, N9BC 17m data → arcs + click popup).
- Commit.

### Task 4 — New 2D Heard-Me RBN overlay
- New `components/RbnHeardMeOverlay.tsx`: props `callsign, band, minutes`; poll `api.getRbnHeardMe`; draw station→skimmer polyline + clickable `circleMarker` popup (skimmer, freq/band, mode, SNR, age). Station origin from `settings.station`.
- Render in MapPlugin when `show2dHeardMeRbn`.
- Commit.

### Task 5 — Overlays menu entries + controls
- In the map's **Overlays** menu (next to "RBN Layer"): add **"PSK Layer"** (toggles `showPskOverlay`) and **"Heard Me — RBN"** (toggles `show2dHeardMeRbn`), each with a small band + timeframe control row (writes `heardMeBand` / window settings, `saveSettings()`).
- Remove/leave the old PSK toggle in the Solar-Overlays menu (move it — avoid two).
- Verify live (menu shows PSK + Heard-Me RBN; toggling works; band/timeframe persist).
- Commit.

### Verification
- `npx tsc --noEmit` + `dotnet build` clean; existing heardMe tests green.
- Live 2D: PSK Layer draws N9BC's 17m reports with All-bands + timeframe working, dots click to report popups; Heard-Me RBN toggle present.
- Backend settings round-trip: `showPskOverlay` + `show2dHeardMeRbn` persist.
