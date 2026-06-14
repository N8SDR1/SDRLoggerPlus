# WSJT-X / JTDX Auto-Logging — Design

**Date:** 2026-06-10 · Part of the [SDRLogger+ port roadmap](sdrloggerplus-port-roadmap.md)

## Purpose

Automatically log FT8/FT4/etc. QSOs completed in WSJT-X, JTDX, or MSHV by listening on the WSJT-X UDP protocol. Was on the SDRLogger+ v2 wishlist; neither app implements it today, so this is a fresh build against the documented protocol (WSJT-X `NetworkMessage.hpp`).

## Protocol summary

- Default endpoint udp://127.0.0.1:2237; WSJT-X can also be configured for multicast (commonly 224.0.0.1) so multiple listeners coexist.
- Datagram framing: magic `0xADBCCBDA` (u32 BE), schema number (u32 BE, ≥2), message type (u32 BE), unique client id (utf-8 string), then type-specific payload. All integers big-endian; strings are u32 length + utf-8 bytes (length `0xFFFFFFFF` = null); booleans 1 byte; `QDateTime` = Julian day (u64) + ms-since-midnight (u32) + timespec byte (+ optional offset).
- Messages we care about:
  - **Heartbeat (0)** — presence/version. Track connected clients for status display.
  - **Status (1)** — dial frequency, mode, DX call, TX state. Used for status display only (first pass).
  - **QSO Logged (5)** — date/time off, DX call, DX grid, TX freq (Hz, u64), mode, report sent/rcvd, TX power, comments, name, date/time on, operator call, my call, my grid, exchange sent/rcvd, ADIF prop mode. → **auto-log**.
  - **Logged ADIF (12)** — the same QSO as an ADIF record. Preferred source when received (parse via existing `AdifService`), with type 5 as fallback; dedupe so one QSO isn't logged twice (key: call+time-off within a small window).
  - **Close (6)** — client went away.

## SDRLoggerPlus design

### Backend — `WsjtxService` (`Services/WsjtxService.cs`)

`IHostedService` singleton owning a `UdpClient`:

- Settings-gated: only binds while enabled; rebinds on settings change (port/multicast), releasing the socket when disabled — same socket-courtesy pattern SDRLogger+ uses for its SAT listeners.
- Unicast bind on configured port; if multicast address configured, join group.
- `WsjtxMessageReader`: a pure parser class (BinaryPrimitives big-endian reads over `ReadOnlySpan<byte>`) producing typed records — fully unit-testable with synthesized datagrams, no socket needed.
- On QSO Logged / Logged ADIF: map to `Qso`, enrich exactly like a manually entered QSO (country/DXCC/continent via existing cty lookup path in `QsoService`), save via `IQsoService`, which already raises the SignalR events that update the log panel, statistics, and `SpotStatusService.OnQsoLogged`.
- Mapping notes: TX freq Hz → MHz `Frequency` + `BandHelper` band; mode from ADIF/mode field; comments → `Comment`; prop mode/exchange fields → `AdifExtra` so nothing is dropped.
- Status surface: connected clients (id, version, last heard), last logged QSO, listening state.

### Settings — `WsjtxSettings` on `UserSettings`

```
Enabled (bool, default false)
Port (int, default 2237)
MulticastAddress (string?, empty = unicast)
```

### API — `WsjtxController`

- `GET /api/wsjtx/status` → `{ listening, port, clients: [...], lastQso }`

SignalR: reuse `LogHub` — `WsjtxClientsChanged`, plus the existing QSO-added event does the rest. A toast on auto-log ("Logged FT8 QSO with …") via the existing notification pattern if one exists; otherwise the log panel update suffices.

### Frontend

Settings section (enable, port, multicast, live status showing detected clients). No dedicated panel in the first pass — the QSO appearing in the log grid in real time is the feature.

## Error handling

- Port already bound (another logger listening) → status shows the bind error; retry with backoff while enabled.
- Malformed/short datagrams, unknown schema or message types → ignore silently (log at debug).
- Duplicate QSO (type 5 + type 12 for the same contact) → dedupe window as above.

## Testing

- Unit: `WsjtxMessageReader` against hand-built byte arrays for Heartbeat, Status, QSO Logged, Logged ADIF (incl. null strings, schema 2/3 variants); QSO mapping (freq→band, datetime conversion from Julian day, AdifExtra passthrough); dedupe logic.
- Integration: loopback UDP send → assert QSO persisted in temp repository.
