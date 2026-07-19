# Sub-project D — Contest entry validation, quick-edit, power scoring

**Date:** 2026-07-17
**Trigger:** live testing of the Wisconsin QSO Party surfaced four gaps.
**Depends on:** A (role engine), B (operating UX), C (per-contest encoding).

## Scope (user decisions)

1. **Band/mode follow the rig** — *fixed already*: seed bands are uppercase
   (`"160M"`) but the app/rig-follow use lowercase (`"20m"`); the entry form now
   normalizes `definition.bands` to lowercase so rig-follow and the logged band
   match. (Commit lands with this batch.)
2. **Valid-data-only S/P/C** — strict on state/province, assist-only on county:
   - **State/province**: hard-reject on log if the 2-char value isn't a real US
     state/DC, Canadian province, or `DX`.
   - **County**: autocomplete from a US county table (3-letter codes, Brown→BRO),
     red-highlight an unknown code, but never block logging (county tables aren't
     guaranteed exact per party).
3. **Power-class scoring** — pick QRP/Low/High at setup; the score is multiplied by
   the contest's power multiplier (encode known values, ×1 otherwise).
4. **Quick edit of a logged QSO** — fix a busted call/exchange from a recent-QSO
   strip; the session score recomputes.

## Design

### County reference data
Restore `countyData.json` (deleted with the map, recovered from git `91aeb3e`) to
`src/SDRLoggerPlus.Web/src/contest/countyData.json` — a `{ ST: [{fips,name,code}] }`
map for 50 states + DC. Only the JSON is restored, not the map/geojson.

A small `src/contest/locations.ts` module exports:
- `US_STATES`, `CA_PROVINCES` (authoritative 2-letter sets) + `isValidStateProv`.
- `countiesFor(states: string[])` → merged county list for the active home area.
- `matchCounties(prefix, states)` → autocomplete matches by code or name.

### Validation in the entry form
For a `state`-typed received field (the "S/P/C" field):
- Suggest home-state counties as you type (dropdown, click to fill the code).
- Mark the field: red if it's a 2-char value that isn't a valid S/P; amber if it's
  a 3+ char code not found in the home-state county table (assist warning).
- `logQso` blocks with a message when a required S/P field holds an invalid 2-char
  value; county mismatches only warn.
- Setup's My-state / My-county inputs validate the same way (state strict).

The home state(s) come from the active `definition.homeArea.states`.

### Power-class scoring
- Model: `ContestDefinition.PowerMultipliers?: { [class]: number }` (e.g.
  `{ QRP: 2, LOW: 1.5, HIGH: 1 }`). Absent ⇒ no power factor.
- `MyExchange.Power` carries the chosen class ("QRP"/"LOW"/"HIGH"); a setup
  dropdown sets it (shown when the contest defines PowerMultipliers).
- `ContestScoringEngine.Recompute` multiplies the final score by
  `PowerMultipliers[me.Power] ?? 1`, rounded. Points and mult counts are unchanged;
  only `Score` is scaled. Seed the values known from research (WI QRP×2/Low×1.5/
  High×1, etc.); ×1 where unknown.

### Quick edit
- Backend: `GET /api/contest/qsos` (active session's QSOs, recent first) and
  `PUT /api/contest/qso/{id}` (update callsign + exchange), which re-enriches,
  re-evaluates the whole session, persists, and broadcasts fresh state.
- Frontend: a "recent QSOs" strip under the entry row; click a row to load it back
  into the call/exchange fields in edit mode; save updates in place.

## Testing
- Backend: engine test for the power multiplier (score scales, points don't);
  update-QSO round-trip recomputes score; seed test for a PowerMultipliers value.
- Frontend: `tsc` clean; unit test for `isValidStateProv` / `matchCounties`.
- Manual: WI QP — county autocomplete (BRO), invalid 2-char S/P blocked, QRP score
  doubles, edit a busted call and watch the score correct.

## Out of scope
Per-party *exact* official county codes (generic 3-letter table is the source),
station-category (rover/mobile) multipliers, and the exotic per-band/distance
scoring still deferred to `ScoringStrategyId`.
