# Hot List + TTS Alerts — Design

**Date:** 2026-06-10 · Part of the [SDRLogger+ port roadmap](sdrloggerplus-port-roadmap.md)

## Purpose

A watched-callsign "Hot List": spots of watched calls are visually highlighted everywhere spots appear (cluster, RBN, panadapter, map) and optionally announced via text-to-speech. One-click add from the DXpeditions panel. Replicates SDRLogger+ behavior (`templates/index.html` ~lines 2694–2750, 5573–5610, 5848).

## Reference behavior (SDRLogger+)

- Settings: enabled flag, TTS flag, comma-separated uppercase callsign list.
- Hot match **overrides every other spot category color** (needed entity, new band, new mode…).
- TTS: per-callsign 15-minute cooldown; utterance "Hot spot. {call}. {band} {mode}"; callsign spoken phonetically-ish (spelled out); `speechSynthesis.cancel()` before `speak()` (Chromium quirk — applies to Electron too).
- DXpedition feed: click a callsign chip → instantly added to Hot List; "+ All" adds every callsign of a multi-op entry.
- Clear-all action in settings.

## SDRLoggerPlus design

### Where hot-match lives: backend

SDRLoggerPlus already classifies spots server-side in `SpotStatusService.GetSpotStatus(dxCall, country, freqKhz, mode)` (returns category strings consumed by cluster/RBN/panadapter UIs). Hot List slots into the same pipeline:

- `HotListService` (singleton): holds the watch set in memory, loaded from settings; exposes `IsHot(callsign)` (exact match on uppercase trimmed call) plus add/remove/clear that persist to settings and raise a SignalR `HotListChanged` event so all panels update live (replaces SDRLogger+'s BroadcastChannel).
- Spot DTOs flowing to the frontend gain an `isHot` flag (set wherever `GetSpotStatus` is applied today). Frontend rule: `isHot` wins over every category color — enforced in one shared spot-color helper, not per-panel.

### Settings — `HotListSettings` on `UserSettings`

```
Enabled (bool, default false)
TtsEnabled (bool, default false)
Callsigns (List<string>, stored uppercase)
TtsCooldownMinutes (int, default 15)
```

### API — `HotListController`

- `GET /api/hotlist` → `{ enabled, ttsEnabled, callsigns }`
- `POST /api/hotlist/calls` body `{ callsigns: [...] }` (add, idempotent)
- `DELETE /api/hotlist/calls/{call}` / `DELETE /api/hotlist/calls` (clear all)
- `PUT /api/hotlist` (enabled/TTS flags)

### TTS: frontend, in the renderer

`window.speechSynthesis` in Electron's renderer, mirroring SDRLogger+: a `useHotSpotAnnouncer` hook subscribes to the existing spot stream, filters `isHot`, applies the per-call cooldown map, spells the callsign character-by-character (e.g. "K 5 P" with phonetic-friendly pacing), calls `cancel()` then `speak()`. Cooldown state is in-memory only (resets on app restart — same as SDRLogger+).

### UI

- **Settings section:** enable, TTS toggle, callsign chip editor (add/remove/clear-all with confirm).
- **DXpeditions panel:** each entry's callsign(s) rendered as clickable chips with a 🔥 add action + "+ All" per entry; chip shows added state. Uses the POST endpoint; SignalR keeps other views in sync.
- **Spot rendering:** hot spots get the hot color (configurable later; fixed accent color first pass) in cluster table, RBN view, panadapter labels, and map spot markers.

## Error handling

- TTS unavailable (no voices) → silently skip speaking; visual highlight unaffected.
- Malformed callsigns on add: trim/uppercase, drop empties; no validation beyond that (SDRLogger+ accepts anything).

## Testing

- Unit (backend): `HotListService` matching, persistence round-trip, change-event raising; spot pipeline sets `isHot` (extend existing `SpotStatusService` tests' patterns).
- Unit (frontend): cooldown gating logic of the announcer hook (fake timers), spot-color precedence (hot beats every category).
- Integration: controller CRUD + settings persistence.

## Out of scope

- Per-callsign expiry dates, wildcard/prefix matching, per-call colors (SDRLogger+ has none of these either).
