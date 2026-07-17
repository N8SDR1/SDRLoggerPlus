# Changelog

All notable changes to SDRLoggerPlus v2 are recorded here.
This file is bundled with the app and shown in About → Changelog.

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

## 2026-07-10 — v2.1.0 "Vega" 🌟

Feature release on the 2.0 "Vega" line.

### New
- **Units — Imperial / Metric master toggle** (Settings → Appearance). One
  switch drives distance, satellite range/altitude, wind, temperature, and
  lightning proximity. The Weather wind switch can still override it (defaults
  to follow the master).
- **WSJT-X / JTDX** now has its own Settings section, with a **second
  independent UDP source** — auto-log from two decoders at once (e.g. WSJT-X
  and JTDX on separate ports).
- **AI talk points — bring your own provider.** Chat AI works with any
  OpenAI-compatible endpoint via a Base-URL field: OpenAI, Anthropic, **Groq**,
  **OpenRouter**, and **Ollama** (local, no key), plus Custom. Provider errors
  now show the real reason. The Help Guide includes an Ollama setup walkthrough.
- **POTA Activators filters** — the DX-cluster toolbar on the POTA panel:
  Follow-rig (Band/Mode), Band, Mode, a Region filter (park location), and
  search. Follow-rig persists across restarts.
- **TCI rig setup — connection check + edit.** Adding a TCI rig probes the
  host:port first, so a wrong port tells you immediately instead of leaving a
  rig that never connects. Saved rigs now have an Edit button (name/host/port).
- **Panadapter** — spectrum height (SPC) and spectrum line-colour controls; the
  palette and line colour live behind a colour gear.
- **ADIF Monitor** — Browse buttons to pick the watched `.adi` files.

### Improvements
- Larger, more legible band-activity tiles in the header bar.
- About page shows the real version number.
- New landscape splash screen.

### Fixes
- **Lightning proximity alert** now reads the freshest strikes (last ~0–10 min)
  instead of data up to an hour old — storm warnings reflect current conditions.

## 2026-07-08 — v2.0.1

### Fixes
- **App icon + splash screen** — replaced the leftover QSOThief-lineage
  artwork with the SDRLogger+ icon. (A dedicated wide splash graphic is
  still to come in a later release.)

## 2026-07-08 — v2.0.0 "Vega" 🌟

First production release of SDRLoggerPlus v2 (.NET 10 backend + React /
Electron). Codename **Vega** — the alpha star of the Lyra constellation,
for the Lyra ↔ SDRLogger+ integration at the heart of this release.

### Lyra Combo Link (headline)
- Two-way link over the existing TCI socket — no bridge app. Grab a call
  in Lyra's CW decoder and it populates SDRLogger+ (call + lookup);
  SDRLogger+ sends the callbook first name back so Lyra's `{NAME}` token
  fills; a `{LOG}`-tagged CW macro logs the QSO in SDRLogger+ as it sends
  the signoff.
- **Auto received-S** — the "S" of RST-Rcvd is computed live from the
  shared S-meter (SNR-gated), with an Auto/Manual toggle; works on CW,
  SSB and digital.
- Survives Lyra restarts (auto-reconnect); a `● Lyra Combo` badge shows
  when linked. Requires Lyra v0.14.0.

### Maps
- **2D Map panel restored** and reworked — a responsive globe + map
  cockpit that stays usable docked small, the focused call/spot offset
  clear of the globe, and a **radio-tower DX marker** on the 2D-map globe.
- **3D globe day/night terminator** with a Settings → Map shade slider.

### DX cluster
- **Follow rig** with independent **Band** and **Mode** toggles — e.g.
  "CW across all bands" or "CW on the current band only" (CWU/CWL fix).

### Elsewhere
- **In-app User Guide** (About → Open the User Guide) covering every
  feature, plus GitHub + Discord links.
- **"Lyra" theme** (Settings → Appearance) styled after the Lyra SDR.
- **Analog meter** shows mode + frequency on one line (white RX / red TX),
  TCI-aware.
- **Updater** matches Lyra: pre-releases visible, once-per-version, shows
  the changelog in the prompt.

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
