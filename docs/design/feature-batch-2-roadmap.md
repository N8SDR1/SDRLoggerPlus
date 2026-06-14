# Feature Batch 2 — Roadmap & Design

**Date:** 2026-06-11
**Source:** User request; reference implementations in SDRLogger+ (`C:\dev\SDRLoggerPlus\main.py`
and `templates/index.html`). Implement in the order below, one commit per feature.

## 1. Settings export / import

Export: a Settings (Database/About area) button downloads the full `/api/settings`
JSON as `sdrloggerplus-settings-YYYY-MM-DD.json` (includes credentials — say so in the UI;
it's a personal backup). Import: file picker → validate it parses and looks like a
settings object (has ≥1 known top-level key) → POST to the existing settings save
endpoint → `loadSettings()` to re-hydrate the store live. Frontend-only if the
existing GET/PUT settings endpoints suffice (they should).

## 2. ITU Region — band plan + out-of-band VFO alerts

Port from `index.html` (`ITU_BANDS`, `_checkOOB`, ~2571-2596, 3300-3340):
- `appearance`? No — new `station.ituRegion` (1|2|3, **default 2** — user is in WI).
  Settings UI: dropdown in Station section ("band plan & out-of-band VFO alerts").
- `src/SDRLoggerPlus.Web/src/core/bandPlan.ts`: the three-region `ITU_BANDS` edge table
  (verbatim values), `bandForFrequency(mhz)`, `checkOutOfBand(mhz, region)` returning
  `{ band, lo, hi } | null`.
- A hook watching the selected radio's frequency (appStore `radioStates`): when VFO is
  inside a band's loose range but outside the strict ITU edges for the configured
  region → one-shot warning toast keyed `band@kHz` (no repeat until freq changes or
  back in band). Use the app's existing toast/notification mechanism.
- Unit tests for `bandPlan.ts` (edges, OOB detection, one-shot keying).

## 3. Club Log

Port `clublog_upload` (`main.py:1131`) — **realtime.php per-QSO uploads only**
(putlogs.php repeatedly = IP ban). Form-urlencoded fields: api, email, password,
callsign, adif (single-record ADIF string with CALL/STATION_CALLSIGN/QSO_DATE/TIME_ON/
BAND/MODE/FREQ/RST_SENT/RST_RCVD). Responses: 200 with OK/Dupe/Updated QSO = success;
**403 or "Login rejected" = set a blocked flag and stop ALL uploads until credentials
are re-saved** (one-strike rule, prevents Club Log IP firewall ban); 400 = QSO
rejected; 500 = transient.
- `ClubLogSettings` (enabled, email, password, callsign, apiKey) — **the API key is
  user-supplied**: Club Log issues application keys to app authors on request
  (helpdesk article 54906); SDRLoggerPlus cannot ship SDRLogger+'s key. Settings UI gets a
  link/hint.
- `ClubLogService`: upload on QSO-logged (hook the same event path QRZ logbook upload
  uses, if any — else subscribe where QSOs are persisted), `TestAsync` (auth check),
  blocked-flag state. Unit tests with a fake HTTP handler (success/dupe/403-blocks-all/
  400).
- Settings UI section + test button. SignalR/toast feedback on failures.

## 4. ADIF File Monitor

Port from `main.py:6370-6520`. Backend hosted service:
- `AdifMonitorSettings`: enabled, `files: string[]` (UI: up to 2 paths like SDRLogger —
  VarAC, MSHV, etc.), `autoImport: bool` (**default true** — user asked for
  "auto-import"; false = hold for confirmation like SDRLogger).
- Poll every ~5 s by byte offset (state file `adif-monitor-state.json` in the config
  dir: path → offset; reset offset to 0 if file shrank). Parse only appended bytes.
- Reuse/share the ADIF record parsing in `AdifService` (it already imports streams);
  parse appended text → QSOs → insert via the same path as normal imports (duplicate
  detection on). On import: SignalR event for a toast ("Imported N QSOs from X").
- If `autoImport` false: emit a pending event; a small UI prompt confirms/discards
  (pending list endpoint + confirm endpoint).
- Unit tests: offset tracking (append/shrink/rotate), appended-fragment parsing,
  duplicate skip.

## 5. Spothole.app cluster source (default) + US spotter default

Spothole is a **read-only REST aggregator** (no telnet): poll
`GET https://spothole.app/api/v1/spots` with `limit=300`, `source=Cluster`, and
`received_since=<last received_time>` (first poll: `max_age=600`), every 30 s
(`main.py:4994-5070`). It cannot accept submitted spots.
- Backend `SpotholeService` (hosted): polls when enabled, maps spothole spot JSON into
  the SAME parsed-spot pipeline `DxClusterService` feeds (hot-list flags, SignalR,
  cluster table) with source tag "spothole".
- **Spotter country filter**: setting `spotterCountry` (default `"United States"`;
  empty = all). Resolve each spot's spotter callsign through the existing
  `CtyService` prefix data; drop non-matching spots in the poller. (SDRLogger filters
  spotter region client-side; we do it server-side at ingest.)
- Cluster settings UI: a "Spothole.app" toggle block above the telnet connections
  (it is not a host/port connection), **enabled by default for new installs**; band/
  source filters optional later. Existing telnet connections unchanged.
- Unit tests: spot JSON mapping, received_since cursor, country filter.

## 6. RBN Band-Opening alerts + voice announce ("same as SDRLogger")

Port `main.py:5073-5260` + `index.html` `processRbnSpot` (~5350-5410):
- Telnet `telnet.reversebeacon.net:7000`, login = station callsign, then DXSpider
  filters: `set/noskimmer`? (NB: SDRLogger sends `set/noskimmer`, `reject/spots all`,
  then `accept/spots N on freq lo/hi` per selected band). Bands: 10m/6m/2m/70cm
  checkboxes (default 6m+2m+70cm like SDRLogger's default param).
- Spot regex: `DX de <skimmer>: <kHz> <dx> <mode> <snr> dB <wpm> WPM <type> <hhmm>Z`.
- Skimmer grid resolution: strip `-#`/digits suffix, cache (incl. negative), resolve
  via existing QRZ lookup service if configured (respect rate limiting; cache in
  memory per session).
- Alert rule: skimmer grid within `distance` (default 500 mi) of station grid AND
  band selected AND per-band cooldown (default 15 min) elapsed → SignalR
  `BandOpeningEvent { band, dxCall, skimmer, distance, unit, snr, mode }`.
- Frontend: persistent toast "Band Opening! 6M — K5XYZ heard by W9ABC (320 mi) 25dB CW"
  + **voice** via SpeechSynthesis when enabled: "Band opening! 6 meters. K 5 X Y Z.
  CW. 320 miles away. 25 dB." (rate 0.9, `cancel()` before `speak()` — Chrome quirk).
  Reuse the Hot List TTS announcer pattern.
- `RbnAlertSettings`: enabled, server, port, bands, distance, unit (mi default),
  cooldownMinutes, voice. NOTE: SDRLoggerPlus already has an `RbnService` feeding cluster
  spots — keep it; this is a separate concern (its own telnet session, filtered
  VHF/UHF), name it `BandOpeningService` to avoid confusion.
- Unit tests: spot regex parse, distance/cooldown/band gating, skimmer-suffix strip.

## Cross-cutting

- Every settings addition: Contracts `Settings.cs` + TS store type/defaults/merge +
  Settings UI section. One commit per feature, tests green before each commit.
- Voice + toasts reuse existing mechanisms (Hot List TTS, UI toast system) — find and
  reuse, don't duplicate.
- Defaults recap: spothole ON w/ US spotters; ITU Region 2; ADIF monitor auto-import;
  RBN alerts OFF until enabled (needs callsign + grid); Club Log OFF until credentials.
