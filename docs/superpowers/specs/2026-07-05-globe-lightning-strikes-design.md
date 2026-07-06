# Globe Lightning Strikes — Design Spec

**Date:** 2026-07-05
**Base:** `v2-alpha` @ `f20ce6c` (synced from origin; work branches fresh off this)
**Status:** Approved design, pending implementation plan
**Supersedes:** the iframe "Lightning Map" panel on branch `feat/lightning-map-panel` (that approach is abandoned — it embedded `maps.blitzortung.org`, which carries ads and can't be made ad-free).

## Summary

Plot live Blitzortung lightning strikes on the existing 3D globe (`GlobePlugin`) as
animated expanding rings, using globe.gl's currently-unused `ringsData` layer. Strikes
are rendered by the app itself (ad-free) from Blitzortung's detection-network data.
Coverage is two-tier: the local region (Americas) refreshes frequently at full detail;
the rest of the world refreshes on a slower cadence. Gated by a toggle, off by default.

This is **Phase 1** of a larger "weather-safety" direction. Deliberately out of scope
here (each its own later spec): weather radar overlay (RainViewer/NOAA), a flat 2D map,
a multi-source selector (Ambient/Met Office), and a day/night grey-line globe toggle.

## Goals

- Show worldwide lightning strikes on the 3D globe, rendered by us (no ads).
- Full-detail, near-real-time strikes for the operator's local region; slower-cadence
  global strikes for worldwide situational awareness.
- Reuse the existing globe and the existing backend Blitzortung fetch; add an isolated
  strike layer without bloating the already-large `GlobePlugin.tsx`.
- User-toggleable, off by default, consistent with the existing POTA / DX-cluster overlays.

## Non-Goals (YAGNI)

- **No** weather radar, flat 2D map, source selector, or day/night toggle (later phases).
- **No** density/heat aggregation — strikes are individual strikes; "summary" for the
  global tier means only a slower refresh cadence, not spatial aggregation.
- **No** Blitzortung realtime websocket — polling their GEOjson feed (the existing
  mechanism) is sufficient and simpler.
- **No** persistence of strikes across restarts — they are ephemeral, rolling-window data.

## Context / Findings

- The globe.gl instance already declares `ringsData` and it is **unused** — `renderBeam`
  only ever calls `.ringsData([])` to keep it empty. It is free for lightning.
- Rick's recent DX-QTH "pulse" (origin `077089a`/`4a97d1d`) is a **CSS/HTML overlay**
  (a `RadioTower` icon with `@keyframes sdrGlobeTowerPulse`) plus an arc alpha heartbeat —
  it does **not** use `ringsData`, so there is no collision. His DX-tower rings are
  **bright yellow**, so lightning must use a different color.
- The backend `BlitzortungClient.GetStrikesAsync` (in
  `src/SDRLoggerPlus.Server/Services/Weather/WeatherClients.cs`) already fetches strikes
  from `map.blitzortung.org/GEOjson/getjson.php?f=s&n={region}` for regions **07/12/13**
  (Americas) and parses `[lon, lat, timestamp, …]`, but currently returns only
  distance/bearing (`StrikeInfo`) and discards the coordinates.
- The existing lightning alert poller (`WeatherAlertService`) runs every **90 s**; there
  is no published hard rate limit on the GEOjson endpoint, and each desktop app polls from
  its own IP, so even all ~14 regions at a 30–60 s cadence stays well under 1 req/s per
  client. The real constraint is render/payload volume, addressed by the two-tier cadence
  and a retention window — not a fetch quota.

## Architecture

### Data tiers

> **FIELD DISCOVERY (2026-07-05, manual verification — supersedes the region-tier
> design below):** live probing proved the GEOjson `n` parameter is a worldwide
> **5-minute time slice** (0 = newest), NOT a geographic region, and the row
> timestamp is a `"yyyy-MM-dd HH:mm:ss.fffffffff"` UTC **string**, not ns-since-epoch.
> "Americas regions 07/12/13" (a premise inherited from the alert code) actually
> meant "worldwide strikes 35–65 minutes old." As built instead:
> - **One poll cadence:** slices `{0,1}` every 60 s (2 req/min — fewer than the
>   two-tier design) cover the buffer's full 10-minute retention window, worldwide.
> - **Local tier = distance:** strikes within **750 km of the station** are tagged
>   Local (bright rings, kept preferentially under the cap); the rest are the dim
>   global tier. User-approved; this also delivers the station-relative behavior
>   the original phase-1 note deferred.
> - Retention windows, cap, and the rest of this spec are unchanged.

> **POST-VERIFICATION RESTYLE (2026-07-06, user-directed):** after seeing it live,
> the user rejected the cyan rings — they didn't read as lightning. Rings now use
> the blitzortung-convention **age ramp: white-hot → electric yellow → amber**
> (fresh → 1 min → 5+ min), local strikes at full alpha, global dimmer. This
> consciously overrides this spec's "not yellow" rule (the DX-tower-pulse clash
> was judged acceptable; the flash core is white). Each strike also gets a
> **center dot** (labels layer, dot-only) for **60 s from live arrival** — keyed
> on SignalR arrival rather than strike time because the feed publishes ~1–2 min
> behind real time, so strike-time dots would never show. Backfilled history
> draws rings only, never dots.

- **Local tier:** strikes within 750 km of the station (see discovery note; formerly
  "regions 07/12/13"). Full-detail rings, brighter color.
- **Global tier:** all other strikes. Dimmer color, longer retention window (10 min
  vs 5 min local).
- Both tiers render every strike as a ring. No aggregation.

### Backend

1. **`BlitzortungClient`** — add a method that returns raw strikes with coordinates:
   `Task<List<LightningStrike>> GetStrikesRawAsync(IEnumerable<int> regions, CancellationToken ct)`
   returning `LightningStrike { double Lat; double Lon; DateTime TimestampUtc; }`. The
   existing distance/bearing `GetStrikesAsync` (used by the alert feature) is left untouched.
2. **`LightningStrikeService`** (new hosted `BackgroundService`) — owns the two timers
   (fast local / slow global), dedupes strikes by coordinate+timestamp against a rolling
   in-memory window, enforces a safety cap (default 2000; trims **farthest-from-station
   first**, always keeping local strikes), and pushes **new** strikes to clients over
   SignalR. Retention windows: local 5 min, global 10 min (tunable constants).
3. **SignalR** — add `OnLightningStrikes(LightningStrikesEvent evt)` to `ILogHubClient`,
   where the event carries a batch of new `LightningStrike` records (each tagged
   `Local: bool`).
4. **REST backfill** — `GET /api/weather/lightning/strikes` returns the current rolling
   set so a freshly-opened globe isn't empty. Add to `WeatherController`.

### Frontend

1. **`lightningStrikes.ts`** (new pure module) — the strike store/logic: add strikes,
   dedupe, age-out by window, local-vs-global cap, expose the current renderable set. No
   React, no globe — pure and unit-testable.
2. **Globe integration** (`GlobePlugin.tsx`) — a small effect subscribes to
   `OnLightningStrikes` (via the SignalR client) and the REST backfill, feeds the pure
   module, and writes the resulting rings to `ringsData` with:
   - `ringColor` as a function of ripple progress `t` (bright→transparent alpha ramp);
     local = electric cyan/white, global = dimmer. **Not yellow.**
   - `ringMaxRadius`, `ringPropagationSpeed`, `ringRepeatPeriod` tuned so each strike
     ripples once over a few seconds.
   - **Remove** the per-frame `.ringsData([])` clear in `renderBeam`; the strike layer now
     owns `ringsData`. When the toggle is off, the strike effect sets `ringsData([])` itself.
   - Add the ring accessor methods (`ringColor`, `ringMaxRadius`, `ringPropagationSpeed`,
     `ringRepeatPeriod`, `ringAltitude`, `ringResolution`) to the local `GlobeInstance`
     TS interface, matching the existing accessor declarations.
3. **Toggle** — `settings.map.showLightning` (default **false**) plus a globe overlay
   button, mirroring the existing `showPotaOverlay` / `dxClusterMapEnabled` pattern. The
   backend `LightningStrikeService` **only polls Blitzortung while the setting is enabled**
   (reading settings the same way `WeatherAlertService` gates on `Weather.Lightning.Enabled`),
   so a client that never turns lightning on generates zero extra Blitzortung traffic. When
   the toggle is off the globe also sets `ringsData([])`.

## Data Flow

```
Blitzortung GEOjson ──poll(fast: local regions / slow: all regions)──► LightningStrikeService
      │ dedupe + rolling window + cap (farthest-trimmed, local kept)
      ├── SignalR OnLightningStrikes (new strikes, batched) ─┐
      └── GET /api/weather/lightning/strikes (backfill) ─────┤
                                                             ▼
                                    lightningStrikes.ts (pure store: add/dedupe/age/cap)
                                                             ▼
                        GlobePlugin effect → globe.ringsData(...) (cyan/white local, dim global)
                                     (gated by settings.map.showLightning)
```

## Error / Edge Handling

- A failed region poll logs at debug and is skipped (matches existing `BlitzortungClient`
  per-region try/catch); other regions still render.
- No station location → local-tier trimming/priority falls back to "keep newest"; the
  globe still shows global strikes.
- Toggle off → globe sets `ringsData([])`; no rings drawn.
- Strikes are best-effort/ephemeral; missing or malformed feed entries are skipped, never
  fatal.

## Testing

- **Backend (unit, `RichardSzalay.MockHttp`):** `GetStrikesRawAsync` parses coordinates +
  timestamp from a sample GEOjson payload and skips malformed rows;
  `LightningStrikeService` dedupe, age-out window, and cap logic (farthest trimmed, local
  strikes preserved) with a mocked clock.
- **Frontend (unit, Vitest):** `lightningStrikes.ts` — add/dedupe, age-out by window,
  local-vs-global cap keeps local, empty state.
- **Manual:** the WebGL globe rendering (ring color/animation, toggle) is verified in the
  running app, not unit-tested.

## Files (indicative — the plan will finalize)

| File | Change |
|---|---|
| `src/SDRLoggerPlus.Contracts/Events/LogEvents.cs` | new `LightningStrike` + `LightningStrikesEvent` records |
| `src/SDRLoggerPlus.Server/Services/Weather/WeatherClients.cs` | `BlitzortungClient.GetStrikesRawAsync` |
| `src/SDRLoggerPlus.Server/Services/Weather/LightningStrikeService.cs` | new hosted service (tiers, dedupe, cap, push) |
| `src/SDRLoggerPlus.Server/Hubs/LogHub.cs` | `OnLightningStrikes` on `ILogHubClient` |
| `src/SDRLoggerPlus.Server/Controllers/WeatherController.cs` | `GET /weather/lightning/strikes` |
| `src/SDRLoggerPlus.Server/Program.cs` | register the hosted service |
| `src/SDRLoggerPlus.Web/src/utils/lightningStrikes.ts` (+ test) | pure strike store |
| `src/SDRLoggerPlus.Web/src/api/signalr.ts`, `client.ts` | wire the event + REST |
| `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx` | ring layer + interface additions; remove per-frame ringsData clear |
| `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` | `map.showLightning` |

## Coordination Note

`GlobePlugin.tsx` is co-owned with Rick and changes frequently. The `ringsData` ownership
change (removing the per-frame clear) and the `GlobeInstance` interface additions are the
two touch points most likely to conflict — implement against `v2-alpha` @ `f20ce6c` and
re-sync before merging.
