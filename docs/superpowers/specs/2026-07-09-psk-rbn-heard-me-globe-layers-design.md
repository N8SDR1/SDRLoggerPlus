# PSK & RBN "Heard Me" Globe Layers — Design

**Date:** 2026-07-09
**Author:** Brent (with Claude)
**Status:** Approved design; ready for implementation plan.

## Goal

Two independent, toggleable layers on the 3D globe that draw animated great-circle arcs
from the operator's station to every station that recently heard them — one layer sourced
from **PSK Reporter** (digital: FT8/FT4/etc.), one from **RBN** (CW/RTTY skimmers).
Clicking a receiving station shows its reception ("RX") report.

## Architecture (Approach A: poll two HTTP endpoints)

Data reaches the globe by polling two backend endpoints. This mirrors the existing 2D-map
PSK overlay pattern, keeps both sources unit-testable, and keeps the change footprint on the
co-owned `GlobePlugin.tsx` small. (RBN "heard me" spots are naturally sparse — you're only
spotted while actively transmitting — so poll latency is a non-issue; a live SignalR push was
considered and rejected as unnecessary plumbing.)

**Tech stack:** .NET 10 backend (existing `RbnController`, `RbnService`, `PskReporterController`),
React/globe.gl frontend (existing `pathsData`/`pathPointAlt` animated-arc infra), zustand
settings store + `Settings.cs` Contracts.

## Global constraints

- **`GlobePlugin.tsx` is co-owned with Rick** — keep edits minimal and contained; reuse the
  existing animated-arc rendering rather than adding parallel machinery.
- **New settings must be backend-backed** (Contracts `Settings.cs` field with `[BsonElement]` +
  TS store type/default + Settings UI). Do NOT repeat the older `showPskOverlay`/`pskCallsign`/
  `showAuroraOverlay` mistake where the frontend fields had no Contracts backing and were
  silently dropped on save.
- **The WebGL globe cannot be verified headless** — all testable logic (band mapping, filtering,
  normalization, arc building) must live in pure functions with unit tests; the visual is
  confirmed live by the operator.
- Branch off `v2-alpha`; never push `local/testing`.

## Data model

Both sources normalize to a single frontend shape the arc-builder consumes:

```ts
interface HeardMeReport {
  source: 'psk' | 'rbn';
  receiverCall: string;   // station that heard me (PSK receiver / RBN skimmer)
  lat: number;            // receiver location
  lon: number;
  freqKhz: number;
  band: string;           // e.g. '20m'
  mode: string;           // e.g. 'FT8', 'CW'
  snr: number;            // dB
  ageMinutes: number;     // how long ago the report was
}
```

The arc origin is always the operator's **station** location
(`settings.station.latitude/longitude`, else `settings.station.gridSquare`), so both layers
share one anchor point (independent of whatever grid PSK Reporter has on file for the sender).

## Backend

### PSK (extend existing endpoint with a window param)
`GET /api/pskreporter/reports?callsign=&minutes=` returns "who heard this callsign" over the
last `minutes` (sender/receiver locators, frequency Hz, mode, SNR, flowStartSeconds). Today the
controller hardcodes `flowStartSeconds=-3600` (1 hour); add a `minutes` query param that drives
`flowStartSeconds`, **clamped to [5, 60]** — PSK Reporter's retrieve API rejects/limits very
large windows and asks clients not to over-fetch, so 60 min stays the ceiling. The 5-min
per-callsign cache becomes keyed by **callsign + minutes** so different windows don't collide.
The frontend converts the receiver grid → lat/lon (existing `gridToLatLon`) and computes band
from frequency.

### RBN (new endpoint on the existing `RbnController`)
`GET /api/rbn/heardme?callsign=&band=&minutes=`:
1. `GetRecentSpots(minutes)` (`minutes` clamped to [5, 120]) → filter to `DxCall == callsign`
   (case-insensitive) and, if `band` is supplied, to spots whose frequency maps to that band.
2. For each surviving spot, resolve the skimmer's location (see **Skimmer location resolution**
   below); drop spots with no resolvable location.
3. Return a DTO list: `{ skimmer, lat, lon, freqKhz, band, mode, snr, ageSeconds }`.

**Skimmer location resolution (must be implemented — currently a stub).**
`RbnService.LookupSkimmerLocationAsync` today always returns null. Implement it as:
QRZ precise → cty.dat fallback, cached once per skimmer in the existing `_skimmerLocations`
dictionary (skimmers are a small, stable set, so this stays cheap):
1. If QRZ is configured, `IQrzService.LookupCallsignAsync(skimmer)` → use its lat/lon when present.
2. Otherwise, or when QRZ has no coordinates, fall back to
   `CtyService.GetCentroidFromCallsign(skimmer)` (offline cty.dat country centroid).
3. Cache the resolved tuple (including a negative/centroid result) so each skimmer is looked
   up at most once per session; QRZ rate limits are respected by the cache.
This requires injecting `IQrzService` into `RbnService` (constructor DI).

The **filter + kHz→band mapping is a pure function** (sits beside/with `BandOpeningLogic`) so it
can be unit-tested independently of the telnet service. The controller endpoint composes:
pure-filter → async location resolve → DTO.

Contracts: add an `RbnHeardMeReport` DTO to `SDRLoggerPlus.Contracts`.

## Frontend

### Fetching
A hook (e.g. `useHeardMeReports`) that, for each enabled layer, polls with
`settings.station.callsign`, the **active band**, and the layer's **window** setting:
- PSK: `api.getPskReports(call, pskWindowMinutes)` every **5 min** (etiquette + backend cache).
- RBN: new `api.getRbnHeardMe(call, band, rbnWindowMinutes)` every **~45 s**.

Changing a window setting refetches on the next tick (window is a dependency of the poller).

**Active band** = the connected rig's band if a rig is connected; otherwise `settings.map.heardMeBand`
(manual fallback). PSK isn't band-filtered server-side (endpoint is fixed to last hour, all bands),
so the frontend filters PSK reports to the active band client-side for consistency with RBN.

### Globe rendering (minimal footprint)
- Normalize both feeds to `HeardMeReport[]`, then build **animated flowing arcs** reusing the
  existing arc style (pathsData polyline + dash animation), band-colored, station → receiver.
- Distinguish RBN from PSK with a **subtly different dash pattern** (both still band-colored) so
  overlapping paths stay readable.
- A clickable marker at each receiver → a **floating RX-report card** showing receiver call,
  band, frequency, mode, SNR, and age. Reuse the globe's existing point-label/tooltip mechanism.
- **Clutter cap:** render at most ~150 arcs per layer, kept by SNR then recency.

### Settings + controls
New `MapSettings` fields (Contracts + store + Settings UI, all persisted):
- `showGlobeHeardMePsk: bool` (default `false`)
- `showGlobeHeardMeRbn: bool` (default `false`)
- `heardMeBand: string` (default `'20m'`) — manual band fallback
- `heardMePskWindowMinutes: int` (default `60`, allowed `[5, 60]`) — PSK look-back window
- `heardMeRbnWindowMinutes: int` (default `30`, allowed `[5, 120]`) — RBN look-back window

The frontend clamps to the allowed range before sending; the backend clamps again (defense in
depth) so a hand-edited settings blob can't push the PSK query past the 60-min ceiling.

UI:
- Settings panel: two checkboxes ("Heard Me — PSK", "Heard Me — RBN"), a band dropdown, and a
  **window control per layer** (PSK 5–60 min, RBN 5–120 min) — a dropdown of sensible presets
  (e.g. 15 / 30 / 60, plus 120 for RBN) or a clamped number input.
- On-globe: when a Heard-Me layer is active, follow the rig's band if connected; otherwise show
  a compact band picker near the globe controls bound to `heardMeBand`.

## Error handling
- Both pollers leave the previous frame on transient fetch errors (as the 2D PSK overlay does).
- No station callsign set → layers render nothing (and the Settings UI hints why).
- Grids/locations that don't resolve are silently skipped (can't draw an arc without an endpoint).

## Testing
- **Backend unit tests:** the RBN heard-me pure filter (DxCall match, band match, kHz→band edges,
  age cutoff) and the controller composed against a fake `IRbnService` (locatable vs unlocatable
  skimmers). Window-clamp tests on both endpoints (PSK → [5,60], RBN → [5,120]: below-min,
  above-max, and in-range values).
- **Frontend unit tests:** the normalizer/arc-builder pure util — grid→lat/lon, band color, the
  ~150 cap + SNR/recency sort, PSK client-side band filter, PSK-vs-RBN dash selection.
- `tsc --noEmit` clean; a settings round-trip confirming the three new fields persist through the
  backend.
- Live visual confirmation by the operator (globe can't render headless).

## Out of scope (this iteration)
- POTA cluster filters (separate batch-3 item).
- Merging PSK+RBN into a single feed (explicitly kept as two layers).
- Historical/track playback; only the current polling window is shown.
