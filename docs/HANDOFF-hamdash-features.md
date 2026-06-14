# Handoff — HamDash-inspired features (globe + PSK Reporter + aurora)

**Date:** 2026-06-12
**Status:** Implemented, builds green, tests pass. **Not yet smoke-tested in the running app.**

## Why
We compared hamdash.com against SDRLoggerPlus and found SDRLoggerPlus already covers almost all of
HamDash (space weather, propagation MUF/LUF engine, DX cluster, RBN, greyline, POTA/SOTA,
3D globe, satellites, contests, lightning). The only genuine gaps were **PSK Reporter** and
an **aurora oval**. Separately, the user asked for **globe auto-rotation + clickable DX spots**.

## What was built (4 work items)

### 1. Globe: slow auto-rotation + pause/play  — `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx`
- globe.gl 2.45 uses OrbitControls; enabled `controls().autoRotate` (speed `0.35`), on by default.
- `isRotating` state (+ `isRotatingRef`), `globeReady` state, effect syncs `autoRotate`.
- Pause/Play button overlay top-right of the globe (guarded by `!hideOverlays`).

### 2. Globe: render + click DX cluster spots — same file
- Globe previously rendered only POTA + focused callsign. Now also renders `dxClusterSpots`
  (store) as `type: 'dx'` markers, colored by band (local `GLOBE_BAND_COLORS`/`GLOBE_BAND_RANGES`).
- Gated by the existing `dxClusterMapEnabled` app-store flag (same toggle as the 2D map).
- `onPointClick` → `focusCallsign(dxCall, 'globe-spot')` (same lookup/fly-to as cluster clicks).
  `onGlobeClick` (rotator beam) is unaffected — point clicks don't fire it.

### 3. PSK Reporter map overlay (any-callsign lookup)
- **Backend:** `src/SDRLoggerPlus.Server/Controllers/PskReporterController.cs`
  `GET /api/pskreporter/reports?callsign=` → retrieve.pskreporter.info `senderCallsign=` (who's
  hearing it), last hour, XML parsed. **5-min per-callsign IMemoryCache = rate-limit guard.**
  Parser is `internal static ParseReports(string xml)`.
- **Frontend:** `src/SDRLoggerPlus.Web/src/components/PskReporterOverlay.tsx` — sender→receiver
  polylines colored by band, SNR/mode tooltip, refresh 5 min. Rendered in `MapPlugin` when
  `settings.map.showPskOverlay`. Callsign = `settings.map.pskCallsign || station.callsign`.

### 4. Aurora oval map overlay
- **Backend:** `src/SDRLoggerPlus.Server/Controllers/AuroraController.cs`
  `GET /api/aurora/ovation` → NOAA OVATION grid, drops cells < 3% server-side, 5-min cache.
  Parser is `internal static ParseOvation(string json)`.
- **Frontend:** `src/SDRLoggerPlus.Web/src/components/AuroraOverlay.tsx` — canvas heat layer
  (green→red by probability), refresh 5 min. Rendered in `MapPlugin` when `settings.map.showAuroraOverlay`.

## Other files touched
- `src/SDRLoggerPlus.Web/src/api/client.ts` — `PskReceptionReport`, `AuroraForecast`/`AuroraPoint`
  types + `getPskReports(callsign)`, `getAuroraOvation()`.
- `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` — `MapSettings`: `showPskOverlay`,
  `pskCallsign`, `showAuroraOverlay` (+ defaults). Persistence merge needs no change (spreads `...settings.map`).
- `src/SDRLoggerPlus.Web/src/plugins/MapPlugin.tsx` — overlay imports, render sites inside
  `<MapContainer>`, and a new **"Propagation"** section in the Layers menu (aurora toggle,
  PSK toggle + callsign input).
- Tests: `src/SDRLoggerPlus.Server.Tests/Tests/Controllers/{PskReporterControllerTests,AuroraControllerTests}.cs`.

## Verification done
- Web: `tsc --noEmit` clean, `npm run build` green, `npm run test` → **232/232 pass**.
- Backend: `dotnet build` clean; **9 new parser unit tests pass** (`Category=Unit`).
- Both parsers validated against **live** OVATION JSON + PSK Reporter XML (shapes match).
- **NOT done:** running the app and clicking the 3 new toggles. Recommended next step.

## To smoke-test
Three terminals (Server `dotnet run`, Web `npm run dev`, Desktop `npm run dev:vite`), then:
- Globe panel: confirm it slowly rotates; pause/play button stops/starts it; enable the DX
  cluster map layer and click a spot → it should look up/fly-to the callsign.
- Map panel → Layers menu → "Propagation": toggle **Aurora Oval** (oval appears near poles);
  toggle **PSK Reporter**, leave callsign blank or type e.g. `W1AW` → reception lines appear.

## Open decisions (reversible, not yet done)
- `BAND_COLORS`/`BAND_RANGES` are now duplicated in MapPlugin, GlobePlugin, and
  PskReporterOverlay. Candidate refactor: extract `utils/bands.ts` and import in all three.
- DX-spots-on-globe intentionally gated by `dxClusterMapEnabled` (shared with 2D map). If you
  want an independent globe toggle, add a separate setting.
- No formal spec doc was written (user asked to skip brainstorming's spec step and just build).
