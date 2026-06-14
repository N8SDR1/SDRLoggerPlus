# SDRLoggerPlus TCI Meters — Design Spec

**Date:** 2026-06-12
**Status:** Approved scope (conversation 2026-06-12); style D chosen by Claude per user's
"keep working" instruction after user went to bed — restyling is cheap, component-local.

## Goal

Live radio meters inside SDRLoggerPlus, fed by Thetis's TCI server over the network:
analog S-meter (needle) plus digital TX power / SWR / frequency-mode tiles —
the "combo dashboard" (style D) from the brainstorm mockups.

## Context (verified during recon)

- SDRLoggerPlus **already has** a mature TCI integration: `TciRadioService`
  (`src/SDRLoggerPlus.Server/Services/TciRadioService.cs`, BackgroundService) with UDP
  discovery, connect/reconnect, `SetFrequencyAsync`/`SetModeAsync`/CW, config
  persistence. Its inner `TciRadioConnection` parses `vfo`, `modulation`, `trx`,
  `device`, `ready`.
- QSO auto-fill from rig state (`followRadio` in LogEntryPlugin) and
  tune-to-spot (`LogHub.TuneToFrequency`, TCI-first) **already exist** — they are
  OUT OF SCOPE here, contrary to the original 3-part scope; nothing to build.
- The missing piece is sensors: Thetis TCIServer (verified in
  `Thetis/Project Files/Source/Console/TCIServer.cs`) supports:
  - Client sends `rx_sensors_enable:true,100;` / `tx_sensors_enable:true,100;`
    (bool + optional interval ms, clamped 30–1000 by Thetis).
  - Thetis then emits, on its timer:
    - `rx_sensors:<rx>,<dBm>;`
    - `rx_channel_sensors:<rx>,<ch>,<dBm>;`
    - `rx_channel_sensors_ex:<rx>,<ch>,<dBm>,<avg dBm>,<peak-bin dBm>;`
    - `tx_sensors:<mic dBm>,<watts>,<peak watts>,<swr>;`
- High-rate UI data in SDRLoggerPlus uses a **callback pattern, not the zustand store**
  (see `setSpectrumDataCallback` consumed by PanadapterPlugin) to avoid re-render churn.
- Hub events are typed via `ILogHubClient`; event DTOs live in SDRLoggerPlus.Contracts.

## Design

**Data flow:** Thetis TCI ─ws─> `TciRadioConnection` (parse) ─> `TciRadioService`
─> `IHubContext<LogHub, ILogHubClient>.All.OnTciMeters(evt)` ─SignalR─>
`signalr.ts` `setTciMetersCallback` ─> `MeterPlugin` (canvas needle + tiles).

**Server:**
- `TciRadioConnection`: after receiving `ready;`, send both sensor enables at
  100 ms. Add parse cases for the 4 sensor frames (InvariantCulture float parse,
  ignore malformed frames silently — consistent with existing parser style).
- New contract `TciMetersEvent` (Contracts/Events): `RadioId`, `RxSignalDbm`,
  `RxAvgSignalDbm`, `TxMicDbm`, `TxPowerWatts`, `TxPeakPowerWatts`, `TxSwr`,
  `IsTransmitting` (from existing trx state), `TimestampUtc`.
  RX channel 0 of the selected instance only (matches existing vfo handling).
- Broadcast coalesced: RX and TX sensor arrivals update a state object; a
  100 ms server-side throttle sends at most 10 events/sec regardless of how
  many frames arrive (protects SignalR clients if user configures faster).
- `ILogHubClient`: add `Task OnTciMeters(TciMetersEvent evt);`

**Frontend:**
- `signalr.ts`: `TciMetersEvent` interface + `setTciMetersCallback` (spectrum pattern).
- `MeterPlugin.tsx` (new, registered in `plugins/index.ts` + layout registry the
  same way PanadapterPlugin is): GlassPanel titled "Meters".
  - RX view: analog needle S-meter on canvas — scale S1..S9..+40dB
    (S9 = −73 dBm, 6 dB/S-unit below, dB-linear above), needle with attack/decay
    ballistics (fast attack ~50 ms, decay ~500 ms), peak-hold tick (avg from
    `_ex` frame shown as small secondary marker), digital dBm + S-unit readout.
  - TX view (as built — amended after audit): the needle stays an S-meter;
    during TX the needle/readout switch to red and the readout shows TX watts.
    A dedicated TX power scale with adaptive ceiling and a mic tile were
    descoped (future polish; `txMicDbm` is captured in state for it).
  - Bottom tiles always visible: PWR, SWR, frequency + mode (from existing
    `rigStatus` in appStore). SWR color thresholds: <1.5 green, <2.5 yellow,
    else red.
  - "No TCI radio connected" empty state mirroring Panadapter's NO_DATA_MSG style.
- Stale-data guard: if no meters event for 2 s, gray the needle/tiles.

**Settings:** none new — rides the existing TCI radio config. Sensor enable is
unconditional on connect (cost is negligible; Thetis only sends while enabled).

**Testing:**
- Server (xUnit, existing Server.Tests project): frame-parsing unit tests for the
  4 sensor message types incl. malformed input; throttle test (N rapid updates →
  ≤1 broadcast per 100 ms window); enable-commands-sent-after-ready test against
  the connection's command log (or mock WebSocket if the class supports injection;
  otherwise refactor `TciRadioConnection` send path behind an interface — minimal
  seam, no behavior change).
- Web (vitest): MeterPlugin renders empty state; renders values from a synthetic
  event; S-unit conversion function unit-tested (−73 dBm → S9, −121 → S1, −53 → S9+20).

**Error handling:** sensor parsing failures are logged at Debug and dropped;
disconnect/reconnect re-sends enables (they're keyed to the `ready` handshake);
no sensor support in radio (non-Thetis TCI device) = no frames = empty state — no errors.

## Audit decision record (2026-06-12, 6-agent audit + round-2 arbiter)

Two MUST-FIX findings were found and fixed:
1. **tx_sensors arity** — Thetis and the official ExpertSDR3 TCI spec both send
   `tx_sensors:<trx>,<mic>,<watts>,<peak>,<swr>;` (5 args, trx first, one frame
   per trx). The original 4-arg parser silently shifted every field. Fixed:
   5-arg-only parse + `Trx == _selectedInstance` filter; arbiter ruled NO 4-arg
   fallback (no real radio emits it; corrupted frames must drop).
2. **Non-finite doubles** — `NaN`/`Infinity`/`1e309` parse under
   `NumberStyles.Float` but crash System.Text.Json serialization, and the
   exception killed the receive loop. Fixed: `double.IsFinite` at the parse
   boundary + try/catch around the meters broadcast + client-side normalize.

Accepted SHOULD-FIXes (all applied): 80 ms throttle window (arbiter: below the
100 ms source interval to prevent beat-frequency emission skips); tile stale
dimming re-render poll; guarded callback teardown (`clearTciMetersCallback`);
single-radio follow with stale-switchover in MeterPlugin; deterministic burst
pacing + off-instance poison frames in the pipeline test; e2e mock bound to
127.0.0.1; devicePixelRatio-aware canvas backing scale; exponential needle
ballistics.

**Follow-up filed (arbiter ruling: out of scope for this branch):**
TciRadioService has no automatic reconnect — `TryAutoConnectAsync` runs once at
startup; a dropped TCI connection stays dead until manual reconnect. Affects
CAT control as much as meters, pre-existing. Sketch: connection-state enum on
TciRadioConnection, sweep Closed connections in ExecuteAsync's 10 s loop,
backoff on consecutive failures. (~50–80 lines + tests + product decision.)

Pre-existing issues observed, not touched: 2 LiteDb tests fail near UTC-day
boundaries (local-vs-UTC, fails on main too); DI scope-validation crash under
ASPNETCORE_ENVIRONMENT=Development (Program.cs ~268, scoped ISettingsService
from root); receive loop ignores WebSocket EndOfMessage (fragmentation).

## Out of scope

QSO auto-fill, tune-to-spot (already exist); per-band meter calibration; volts/amps
(needs MultiMeterIO, future); meter styling beyond style D; multi-radio simultaneous meters
(first connected radio only, matching TuneToFrequency's existing first-radio behavior).
