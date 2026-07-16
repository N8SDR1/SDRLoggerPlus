# Changelog

All notable changes to SDRLoggerPlus v2 are recorded here.
This file is bundled with the app and shown in About → Changelog.

## 2026-07-16 — v2.3.0 "Vega+" 🌟

Native FT8 / digital-mode tools — decode monitoring, smart alerting, and grid
tracking, built right into SDRLoggerPlus. No JTAlert or GridTracker needed.

### New
- **Digital Decodes panel** — the live FT8/FT4 decode stream from WSJT-X / JTDX /
  MSHV over UDP, each decode coloured by what it would give you (new DXCC / band /
  zone / grid), with Needed-only and CQ-only filters. **Double-click a decode**
  and your decoder answers that CQ (WSJT-X / JTDX, with "Accept UDP requests" on).
- **Digital Decode Alerts** (Settings → Digital Decode Alerts) — geo-scoped rules
  that sound / speak / pop only for what you want: an award need (DXCC / band /
  zone / grid) × a region (continent, DXCC entity, US call area, prefix, grid
  field) × band/mode. So "needed grids, North America only, 20m" is one rule.
  Quick-add presets; voice uses the shared Voice section.
- **Grid Tracker panel** — a Maidenhead grid map: worked (green, brighter =
  confirmed), needed (red tint), and stations active right now from the decode
  stream (cyan ring; a needed + live grid pulses "chase now"). Country outlines,
  pan / zoom, hover, VUCC progress, and **Follow Digital Decodes (FDD)** to lock
  the map to your decoding session.
- **Live log-entry populate** — when your decoder's DX Call changes (you call CQ,
  or a station answers you), it fills the Log Entry, fires the callbook lookup
  (QRZ Profile), and drops the station on the map. The finished FT8 QSO auto-logs
  to Log History.
- **Compact Log History summary** — QSOs / Countries / Grids / Today on one line.

### Changed
- **Settings → WSJT-X / JTDX** is now **Decoder Link (UDP)** — it feeds both
  auto-logging and the live decode stream.

### Fixed
- A grid you just worked no longer re-appears as "needed" on the next decode —
  logging now records the grid in the needed-status cache.

## 2026-07-15 — v2.2.1

### Fixed
- **Settings wouldn't save (HTTP 400).** A non-nullable field that shipped empty
  in 2.2.0 tripped implicit validation and blocked every settings save, on every
  tab. Fixed.

## 2026-07-15 — v2.2.0 "Vega" 🌟

### New
- **DX Coach** — turns live spots + your award needs + solar / gray-line data
  into proactive, factually-grounded operating suggestions, with a needs matrix
  (DXCC / WAS / zones), band-class filters, and optional voice.
- **Confirmations** — Log4OM-style confirmation ingest: LoTW / eQSL / QRZ merge
  into your log with a QSL column (L / E / Q / C), one-click downloads, and
  background auto-sync.
- **Voice** — a shared announcement voice (accent + male / female) for band
  openings, Hot List, RBN alerts, and the DX Coach, with a volume control.

## 2026-07-06 — Panadapter deep-clean session

### Fixes
- **Panadapter spectrum orientation** — un-mirror HL2 baseband so the RF
  spectrum reads correctly (USB energy right of the VFO, LSB left).
  Matches Lyra's own panadapter on the same TCI stream.
- **Panadapter waterfall dynamic range** — auto-track noise floor with
  a dB-stretch mapping so background pixels reliably sit at LUT[0]
  regardless of signal strength. Matches Lyra's `AutoDbScaler`.
- **Panadapter waterfall grid alpha** — bumped from 6% → 35% so the grid
  is visible without fighting for attention.

### Additions
- **TCI-driven filter passband overlay** — translucent green rectangle
  over the RX filter's actual Hz range, with a compact mode + width
  chip in the header (`USB 100 to +2700 Hz`, `CWU 500 Hz`). Reads
  TCI's `rx_filter_band` and `cw_pitch` — no per-mode guessing.
- **Four waterfall palettes** — Classic, Heat, Viridis, Rainbow.
  Viridis and Rainbow are byte-for-byte identical to Lyra's palettes
  so the two apps render matching waterfalls side by side.
- **Panadapter STEP picker** — mouse-wheel tuning step
  (1 Hz / 10 Hz / 100 Hz / 500 Hz / 1 kHz / 2.5 kHz / 5 kHz / 10 kHz).
  Shared with the Rig panel's VFO wheel step.
- **Waterfall floor / ceiling sliders** — dB-stretch style controls
  that shift/compress the LUT into the interesting signal window.

## 2026-07-06 — POTA + spot + cluster QoL

### Additions
- **POTA self-spot** — the "Spot Myself" button in the LogEntry POTA
  banner now POSTs to `api.pota.app/spot`. Set POTA credentials in
  Settings → Web Logbooks → POTA.
- **DX cluster outbound spot** — LogEntry's Spot button sends
  `dx FREQ CALL COMMENT` to the primary DX cluster. Only one cluster
  gets the spot at a time (clusters peer + relay upstream, so posting
  to multiple would flag your call as a duplicate source).
- **Primary spot cluster picker** — Settings → Cluster gains an
  "Outbound Spots" dropdown to choose which of your configured
  telnet clusters receives self-spots.
- **DX cluster spot cap + age filter** — Settings → Cluster gains a
  Max Spots slider (50–300) and header dropdown for the age filter
  (5 / 10 / 15 / 30 / 60 min).

### Fixes
- **TCI spot click-twice-to-set** — LogHub now sends frequency before
  mode on the TCI path, matching Lyra's expected order. First click
  lands correctly.
- **TCI CWU/CWL routing** — inbound `modulation:0,CW` now derives
  CWU / CWL from the current dial (>10 MHz = CWU, <10 MHz = CWL,
  standard ham convention).
- **HamQTH lookup stall** — dropped 15 s → 5 s timeout, added a
  negative-result cache, hard-stopped at 6 s in the LogHub chain so
  a slow HamQTH server can't hang the callsign panel.

## 2026-07-05 — eQSL + UDP ADIF + Log-entry mode arc

### Additions
- **eQSL.cc real-time upload** — every logged QSO fires an
  `ImportADIF.cfm` POST if enabled (Settings → Web Logbooks → eQSL).
  Includes a Test Credentials button.
- **Generic UDP ADIF listener** — port 52001 (v1 default) auto-imports
  ADIF records broadcast by VarAC / N1MM / Logger32 / DXKeeper. Enable
  in Settings → ADIF File Monitor.
- **Log-entry mode arc** — the LogEntry panel now has General / POTA /
  SAT tabs (matching v1 SDRLogger+).
  - **General** — mode-switcher + v1.x-style field layout.
  - **POTA** — activating-park chip, P2P park ref field,
    `my_pota_ref` / `pota_ref` tagged in AdifExtra.
  - **SAT** — bespoke fields for satellite name, uplink/downlink
    freq + mode, worked-station grid. Auto-populates from the S.A.T.
    controller when it's tracking a pass.

### Fixes
- **Log History stats** — Countries + Grids now count correctly on
  imported logbooks (was counting by DXCC id, which was null on v1
  ADIF exports; now counts distinct Country name).
- **Log History Remarks column** — the empty column between Country
  and the edit/delete buttons now shows the QSO's Remarks / Comment.
- **Band case normalization in stats** — mixed-case bands ("40m" vs
  "40M") from different ADIF sources now collapse into one bucket.
- **SAT panel v1 parity** — added missing rows (AZ / EL, Max EL,
  Range, Time to LOS / AOS in, Elapsed) and a proper Transponder
  subsection. Text brightened from washed-out dark-300/400 to
  readable dark-100/200. Pass Log always renders its header.

### Settings
- **HamQTH Test Credentials button** — parity with the QRZ tab.
- **Weather alert banner** now renders at the top of the app
  (was pinned at the bottom above the status bar).

## Session notes

Each session's work is grouped chronologically above.
For fine-grained commit history, see `git log` on the `v2-alpha` branch.
