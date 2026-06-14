# CSN Technologies S.A.T. Controller Integration — Design

**Date:** 2026-06-10 · Part of the [SDRLogger+ port roadmap](sdrloggerplus-port-roadmap.md)

## Purpose

Satellite operating support via the CSN Technologies S.A.T. controller: live pass status, auto-logged SAT QSOs, and footprint/ground-track display on SDRLoggerPlus's existing map/globe. Ports SDRLogger+ v1.05–v1.10 behavior (`main.py` lines 7511–8000+). Built and tested against mocks; live validation against the user's hardware happens after review.

## Reference behavior (SDRLogger+)

### Inputs from the S.A.T. (three channels)

1. **UDP broadcast, port 9932** — comma-separated `SAT,...` messages driving a state machine:
   - `SAT,BOOT,<serial>,<firmware>` → idle
   - `SAT,START TRACK,<satname>,<catno>` → tracking; clears per-pass state (transponder, AOS/LOS, pass QSO list)
   - `SAT,AOS,<az>` / `SAT,LOS,<az>` → pass started/ended (AOS timestamp kept for elapsed display)
   - `SAT,TRANSPONDER,<name>,<upFreq>,<upMode>,<downFreq>,<downMode>`
   - `SAT,QSO,<sat>,<call>,<grid>,<mode>,<comment>,<rstS>,<rstR>,<upHz>,<downHz>,<name>` → append to pass QSO list **and auto-log** (freqs Hz→MHz; band from downlink; prop_mode SAT)
   - `SAT,STOP` → idle
2. **ADIF over UDP, port 1100** (the S.A.T.'s "QSO LOG TYPE" feature) — a full ADIF record per QSO; preferred fields incl. `sat_name`, `prop_mode`, `freq` (uplink) / `freq_rx` (downlink). Faster than the firmware's UDP QSO message (<3 s arrival). Dedupe against channel 1.
3. **HTTP polling of the controller** — `GET http://<sat-ip>/track` (~1.5 s cache): satname, catno, az/el, AOS/LOS azimuths, time-to-AOS/LOS, plus footprint value. Drives live status when broadcasts are quiet.

Listeners are **mode-gated**: sockets bound only while SAT integration is enabled *and* the app is in SAT mode, released otherwise so ports stay free for other tools.

### Map math (ported decisions)

- Sub-satellite point from station look-angle + range: spherical-Earth ENU→ECEF computation (`_sat_subpoint_from_look`, R=6371 km; <0.3 % error at LEO).
- **Footprint radius computed from altitude** — `r = R · acos(R / (R + h))` (0° geometric horizon). Do **not** treat the controller's `satFootprint` as a radius; it's a diameter (predict/Gpredict convention) and caused the v1.10 2× bug.

## SDRLoggerPlus design

### Backend

```
Services/Sat/
  SatControllerService.cs   (IHostedService: listener lifecycle, state machine, HTTP poller)
  SatMessageParser.cs       (pure: "SAT,..." text → typed events)
  SatGeometry.cs            (pure: subpoint-from-look, footprint radius from altitude)
  SatTrackClient.cs         (HTTP /track with 1.5 s cache; interface-backed)
```

- State machine port is 1:1 (BOOT/START TRACK/AOS/LOS/TRANSPONDER/QSO/STOP), state + rolling event list (capped, like SDRLogger+'s deque) held in the service.
- **Mode gating:** SDRLoggerPlus has no global "SAT mode"; gate is simply `SatSettings.Enabled` plus an explicit "SAT active" toggle exposed in the panel (one click = bind ports + start polling; off = release everything). Functionally equivalent without inventing app modes.
- **Auto-logging:** both QSO channels map to `Qso` (SatName/PropMode→`AdifExtra` ADIF fields `SAT_NAME`, `PROP_MODE`; uplink → `FREQ`-companion field, downlink drives `Frequency`/`Band` via `BandHelper`), saved through `IQsoService` (existing events update all panels). Dedupe key: call + sat + minute window.
- SignalR: `SatStateChanged` pushes the full state on every change/poll tick.

### Settings — `SatSettings` on `UserSettings`

```
Enabled (bool, default false)
ControllerIp (string)
UdpPort (int, default 9932)
AdifPort (int, default 1100)
```

### API — `SatController` (extend the existing `SatellitesController` if it fits naturally, else a new `SatStatusController`)

- `GET /api/sat/status` → state + pass QSOs + elapsed-since-AOS + ports (mirror of SDRLogger+'s endpoint)
- `POST /api/sat/active` body `{ active: bool }` → bind/release listeners

### Frontend

- **SAT status panel** (new plugin, or a section of the existing satellites panel — decide during implementation by looking at `SatellitesController`'s current consumers): controller status, tracked satellite, AOS/LOS azimuths + countdowns, transponder line (↑/↓ freq+mode), pass QSO list, event log, active toggle.
- **Map/globe overlay:** when tracking, draw sub-point marker, footprint circle (radius from altitude via `SatGeometry`), and beam line from station; reuse the existing map overlay pattern (`GrayLineOverlay` precedent).

Ground-track propagation from TLEs (SGP4, ~3 orbits) is **phase 2** of this feature — the live sub-point + footprint from the controller doesn't need TLEs; note it as a follow-on rather than pulling an SGP4 dependency in now.

## Error handling

- Port bind failure (something else on 9932/1100) → status shows error, retry with backoff while active.
- Malformed SAT messages → ignored (parser returns null), logged at debug.
- `/track` unreachable → status degrades to UDP-only; banner shows last-heard age.

## Testing

- Unit: `SatMessageParser` for every message type incl. short/garbage frames; `SatGeometry` against the documented SO-50 case (alt 605.9 km → footprint radius ≈ 2 679 km) and subpoint sanity cases; QSO mapping (Hz→MHz, band from downlink, dedupe).
- Integration: loopback UDP datagrams → state machine assertions + QSO persisted; mocked `/track` JSON → state enrichment.
- Live hardware validation: deferred to user (checklist in PR notes: BOOT seen, pass status, QSO arrives <3 s, footprint matches controller MAP page).
