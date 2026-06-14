# SDRLogger+ Feature Port — Roadmap

**Date:** 2026-06-10
**Goal:** Bring the remaining SDRLogger+ (Python/Flask, `C:\dev\SDRLoggerPlus`) features into SDRLoggerPlus, keeping SDRLoggerPlus's architecture and visual style. SDRLoggerPlus's Electron shell already satisfies the "not in a browser tab" requirement; SDRLogger+'s browser-survival machinery (idle timeout, sendBeacon shutdown, BroadcastChannel cross-tab sync) is intentionally **not** ported.

## Decisions (confirmed with user 2026-06-10)

- Scope: **logger features only** — no SDR-console/DSP ambitions from the SDRLogger+ v2 (Rust/Tauri) plan. Radio control stays at TCI/Hamlib/FlexRadio level.
- No data migration from SDRLogger+ (`hamlog.db`/`pota.db`) — fresh start; SDRLoggerPlus is the source of truth.
- Awards counting **replicates SDRLogger+ behavior** (worked-based, same resolution rules) so numbers match user expectations.
- Weather: all three sources (NWS, Ambient Weather, Ecowitt) — user owns the hardware.
- Git: commit locally to `main`, never push.

## Build order

| # | Feature | Spec | Status |
|---|---------|------|--------|
| 1 | Scheduled auto-backup | [auto-backup-design.md](auto-backup-design.md) | implemented |
| 2 | Awards dashboards (WAS/WAZ/WPX/WAC/5BWAS/5BDXCC) | [awards-dashboards-design.md](awards-dashboards-design.md) | implemented |
| 3 | Hot List + TTS alerts | [hot-list-design.md](hot-list-design.md) | implemented (cluster highlight + TTS + DXpedition add; panadapter/map highlight is a follow-up) |
| 4 | WSJT-X auto-logging | [wsjtx-auto-logging-design.md](wsjtx-auto-logging-design.md) | implemented (QSO Logged type 5 only; Logged ADIF type 12 intentionally ignored to avoid double-logging) |
| 5 | Weather alerts (lightning + high wind) | [weather-alerts-design.md](weather-alerts-design.md) | implemented (mocked tests; live API keys to be entered by user) |
| 6 | CSN S.A.T. controller integration | [sat-controller-design.md](sat-controller-design.md) | implemented (mocked tests; live hardware validation pending; map footprint overlay is a follow-up) |
| 7 | CW decoder | [cw-decoder-feasibility.md](cw-decoder-feasibility.md) | spike done — GO; decoder proven offline, TCI audio streaming is the remaining work |

Ordering logic: low-risk/high-value first (1–3), then protocol work (4–5), then items needing hardware on the bench or a research spike (6–7).

## Common architecture notes

Every feature follows the established SDRLoggerPlus patterns:

- **Backend:** a service class in `src/SDRLoggerPlus.Server/Services/` registered in DI, exposed via a controller in `Controllers/` and/or pushed over the existing `LogHub` SignalR hub.
- **Settings:** new sections on `UserSettings` (`src/SDRLoggerPlus.Contracts/Models/Settings.cs`), persisted through `SettingsRepository`, edited in `SettingsPanel.tsx`.
- **Frontend:** a plugin panel in `src/SDRLoggerPlus.Web/src/plugins/` registered in `plugins/index.ts`, or a tab/section in an existing panel.
- **Storage:** `IQsoRepository` abstracts LiteDB (default, file `sdrloggerplus.db`) and MongoDB. Anything touching backup/restore must handle both providers.
- **Tests:** xUnit in `src/SDRLoggerPlus.Server.Tests` (Category=Unit/Integration), Vitest in `src/SDRLoggerPlus.Web`.

## Explicitly not ported from SDRLogger+

- Idle timeout / explicit-close lifecycle (Electron makes it moot)
- Donate button / DonateNudge
- BroadcastChannel hot-list sync (single-window app; state lives in the backend)
- Font-scale A/A/A+ buttons (SDRLoggerPlus has its own appearance settings)
- ADIF file monitor (VarAC/MSHV watcher) — noted as a possible future feature; WSJT-X UDP covers the main digital-mode use case
