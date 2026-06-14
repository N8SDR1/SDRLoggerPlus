# TCI Meters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Live S-meter / TX power / SWR panel in SDRLoggerPlus fed by Thetis TCI sensors.

**Architecture:** Pure-function `TciSensorParser` + throttling `TciMeterAggregator` (both unit-testable, no network), called from the existing `TciRadioConnection`; broadcast via existing typed-hub pattern; React `MeterPlugin` consuming a high-rate callback (Panadapter pattern). Spec: `docs/superpowers/specs/2026-06-12-tci-meters-design.md`.

**Tech Stack:** .NET 10 / xUnit + Moq + FluentAssertions; React 18 + TS + vitest.

---

### Task 1: TciSensorParser (TDD)

**Files:**
- Create: `src/SDRLoggerPlus.Server/Services/Tci/TciSensorParser.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/TciSensorParserTests.cs`

- [ ] Step 1: failing tests — parse `rx_sensors` args `["0","-97.4"]` → RxReading(0, −97.4); `rx_channel_sensors_ex` args `["0","0","-97.4","-99.1","-85.0"]` → channel reading with avg/peak; `tx_sensors` args `["-12.3","4.8","5.0","1.4"]` → TxReading; malformed (`["x"]`, empty, wrong count) → null; InvariantCulture (`"-97.4"` parses under any culture).
- [ ] Step 2: run, verify fail. `dotnet test --filter TciSensorParser` → FAIL (type missing).
- [ ] Step 3: implement static parser returning nullable record structs `TciRxSensorReading(int Rx, double Dbm)`, `TciRxChannelSensorReading(int Rx, int Channel, double Dbm, double? AvgDbm, double? PeakBinDbm)`, `TciTxSensorReading(double MicDbm, double PowerWatts, double PeakPowerWatts, double Swr)`.
- [ ] Step 4: tests pass. Step 5: commit `feat(server): TCI sensor frame parser`.

### Task 2: TciMeterAggregator (TDD)

**Files:**
- Create: `src/SDRLoggerPlus.Server/Services/Tci/TciMeterAggregator.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/TciMeterAggregatorTests.cs`

- [ ] Step 1: failing tests — UpdateRx then TryGetSnapshot(t0) → emits event with values; immediate second TryGetSnapshot(t0+50ms) → false (throttled, 100 ms); TryGetSnapshot(t0+150ms) with no new data → false (not dirty); UpdateTx then snapshot carries both latest RX and TX; thread-safety smoke (parallel updates don't throw). Time injected as `DateTime` parameter — no clock dependency.
- [ ] Step 2: verify fail. Step 3: implement (lock-guarded state, lastEmit timestamp, dirty flag; snapshot returns `TciMetersEvent`). Step 4: pass. Step 5: commit `feat(server): TCI meter aggregator with 100ms coalescing`.

### Task 3: Contract + hub plumbing

**Files:**
- Modify: `src/SDRLoggerPlus.Contracts/Events/LogEvents.cs` — add positional record:
  `TciMetersEvent(string RadioId, double? RxSignalDbm, double? RxAvgSignalDbm, double? TxMicDbm, double? TxPowerWatts, double? TxPeakPowerWatts, double? TxSwr, bool IsTransmitting, DateTime TimestampUtc)`
- Modify: `src/SDRLoggerPlus.Server/Hubs/LogHub.cs` — `ILogHubClient`: `Task OnTciMeters(TciMetersEvent evt);` + extension `BroadcastTciMeters` next to `BroadcastRadioStateChanged` (line ~1018).

- [ ] Implement, build (`dotnet build`), commit `feat(contracts): TciMetersEvent + hub broadcast`.

### Task 4: Wire into TciRadioConnection

**Files:**
- Modify: `src/SDRLoggerPlus.Server/Services/TciRadioService.cs` (`TciRadioConnection`):
  - field `private readonly TciMeterAggregator _meters = new();`
  - in `ProcessMessageAsync` switch add cases `rx_sensors`, `rx_channel_sensors`, `rx_channel_sensors_ex`, `tx_sensors` → parse via TciSensorParser, feed aggregator (RX: only `rx == _selectedInstance`, channel 0 for the channel variants), then after the foreach: `if (_meters.TryGetSnapshot(DateTime.UtcNow, _device.Id, _isTransmitting, out var metersEvt)) await _hubContext.BroadcastTciMeters(metersEvt);`
  - in `ready` case, after `start;`: send `rx_sensors_enable:true,100;` and `tx_sensors_enable:true,100;`

- [ ] Implement, build, run full server test suite, commit `feat(server): subscribe + rebroadcast TCI sensors`.

### Task 5: Frontend signalr + MeterPlugin

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/api/signalr.ts` — `TciMetersEvent` interface (camelCase fields), `setTciMetersCallback`, `connection.on('OnTciMeters', …)` next to OnSpectrumData (line ~997).
- Create: `src/SDRLoggerPlus.Web/src/plugins/MeterPlugin.tsx` — style D: canvas analog S-meter (needle, S1–S9 ticks at 6 dB, +20/+40 over; S9 = −73 dBm; attack 50 ms / decay 500 ms ballistics via rAF; avg as secondary marker), digital tiles PWR / SWR (green <1.5, yellow <2.5, red ≥2.5) / freq+mode from `useAppStore().rigStatus`; TX view swap on `isTransmitting`; 2 s stale → grayed; empty state matching Panadapter's.
- Create: `src/SDRLoggerPlus.Web/src/utils/smeter.ts` — `dbmToSUnit(dbm): {label: string; fraction: number}` (pure, tested).
- Modify: `src/SDRLoggerPlus.Web/src/plugins/index.ts` + `src/SDRLoggerPlus.Web/src/App.tsx` registry: id `'meters'`, name `Meters`, icon `Gauge`, category `'Radio & Equipment'`, tags `['meter','smeter','swr','power']`.
- Test: `src/SDRLoggerPlus.Web/src/utils/smeter.test.ts` (−73→S9, −121→S1, −53→S9+20, −127→S0 clamp, fraction monotonic); `src/SDRLoggerPlus.Web/src/plugins/MeterPlugin.test.tsx` (renders empty state; shows SWR value after callback fires).

- [ ] smeter.ts TDD first; then component; `npm test`, `npm run build`; commit `feat(web): Meters plugin (TCI combo dashboard)`.

### Task 6: Verification + docs

- [ ] `dotnet build` + `dotnet test` (full), `npm run build` + `npm test` — all green, evidence captured.
- [ ] End-to-end vs mock: small Node ws script emulating Thetis TCI (sends ready→expects enables→streams sensor frames); verify SignalR OnTciMeters observed via server logs (or skip if server cannot run headless in env — document).
- [ ] Update memory + commit any doc deltas.
