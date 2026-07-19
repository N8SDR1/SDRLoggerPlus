# Changelog

All notable changes to SDRLoggerPlus v2 are recorded here.
This file is bundled with the app and shown in About → Changelog.

## 2026-07-18 — v2.5.0 "Vega+" 🌟

Fit-more-on-screen release: scale the UI to your display, plus two logging fixes.

### New
- **UI Scale** — two independent, down-only (70–100%) zoom controls for smaller /
  high-density screens. **Whole-app zoom** (desktop): the View menu or
  **Ctrl + / − / 0**, and a stepper in **Settings → Appearance → UI Scale**.
  **Per-panel scale** — a `− % +` stepper in each panel's tabset header (click the
  % to reset), saved with your layout. Map / Globe / Panadapter stay at true
  pixels. *(N9BC)*

### Changed
- The old **Compact Mode** toggles (superseded by UI Scale) are gone.
- The per-panel "open settings" affordance on a tab is now a **gear icon**. *(N9BC)*
- **2D Map** fit-fixes at small sizes; the Layers / Solar fly-outs no longer clip
  under the globe. *(N9BC)*

### Fixed
- **"Today" QSO count** stayed at 0 for QSOs logged in the evening — it compared a
  locally-stored QsoDate against a UTC "today" boundary. It now counts by your
  **local** day.
- **Lightning banner distance** showed km even with the Weather **Alert range** set
  to miles — it followed the master unit system instead of the lightning unit.
  It now matches the Alert-range unit you set.

## 2026-07-16 — v2.4.0 "Vega+" 🌟

More reach, less friction: see who's hearing *you*, filter the FT8 firehose down
to your alert rules, and reach rig setup straight from the status bar.

### New
- **"Heard Me" signal-path layers** — PSK Reporter (digital) and RBN skimmer
  (CW/RTTY) arcs from your station to every receiver that recently spotted you,
  on both the **3D globe** and the **2D map**. Band follows your rig (or pick one
  / All bands), per-layer look-back windows, clickable receiver points showing
  the RX report (freq / mode / SNR / age). *(N9BC)*
- **Rig status-bar switcher** — the bottom-right pill shows the connected radio;
  **left-click** to switch or connect any configured rig, **right-click** (or
  "Add / manage radios") to open the Rig panel for setup. *(N9BC + N8SDR)*
- **"Match Alerts" decode filter** — the Digital Decodes list can now be narrowed
  to just the decodes that match your **Digital Decode Alert** rules, so a
  "needed grids, North America" rule filters the list too, not only the alerts.

### Changed
- The **Map's Overlays** menu is now simple toggles (Lightning, Rotate Globe,
  PSK Layer, RBN Layer) with shared band/window controls; RBN cluster-feed
  settings moved into **Settings → Map**. *(N9BC)*

### Fixed
- **8-character extended grids** (e.g. `EN54xl17` from PSK Reporter) were rejected
  by the grid→lat/lon parser, silently dropping every 2D-map arc. Now accepted. *(N9BC)*

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
