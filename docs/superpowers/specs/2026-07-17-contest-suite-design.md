# Contest Suite — Design

**Date:** 2026-07-17
**Status:** Design — approved for implementation
**Branch:** `feature/contest-suite` (single branch, off `v2-alpha`)

## Summary

Add a full contest-logging suite to SDRLoggerPlus, closing the gap versus N1MM
Logger+ / N3FJP. Twelve user-selected features are delivered on **one branch**,
built around a **data-driven, server-authoritative contest engine** with a
**dedicated contest entry window** (independent of the casual Log Entry form).

The twelve features:

1. Contest **rule presets** (data-driven definitions)
2. **Serial-number** generator
3. Real-time **dupe check**
4. Live **scoring + multipliers**
5. Contest **bandmap**
6. **Call-history / exchange prefill**
7. **Super Check Partial (SCP)**
8. **Rate meter**
9. **Multiplier-needed** panels
10. **Online score reporting**
11. **Cabrillo export**
12. **N1MM-style UDP broadcasts**

Plus three refinements requested during design:

- **Search** the contest list.
- **User-created contests** (create / clone-from-builtin / delete-your-own).
- A visible **"Contest" field/column in the logger** showing which contest a
  QSO was logged for.

Explicitly **out of scope** (deselected): contest macros / voice keyer,
CQ-S&P / ESM, WinKeyer hardware keying, SO2R / networked multi-op.

## Decisions (locked)

- **Delivery:** one branch, all 12 features. Internal build order below.
- **Entry surface:** a dedicated contest window (N1MM-style), decoupled from the
  pending entry-form overhaul. No prerequisite phase.
- **Engine:** Architecture A — declarative contest definitions + authoritative
  server-side engine (.NET) pushing state over SignalR; client keeps a local
  worked-set for optimistic instant dupe/mult coloring. Escape hatch: an optional
  named C# scoring strategy a definition can reference for exotic contests.

## 1. Architecture

- New server area `src/SDRLoggerPlus.Server/Services/Contest/` holds the engine,
  definition loader, session store, and exporters. Pure scoring logic is I/O-free
  and unit-tested in isolation.
- Real-time via the existing `LogHub` / `ILogHubClient` SignalR pattern: the
  server computes dupe/serial/score/mults and pushes `ContestState` updates plus
  `WorkedSetDelta` messages. The React client applies deltas to a local worked-set
  so dupe/mult coloring is instant while typing; the server remains authoritative
  (its recompute wins on conflict).
- Frontend surfaces are ordinary FlexLayout plugins registered in
  `src/SDRLoggerPlus.Web/src/plugins/index.ts`.

## 2. Contest definition schema (declarative, data-driven)

Definitions are JSON. Seeded definitions ship read-only with the app; user
definitions live in `%APPDATA%\SDRLoggerPlus\contests\` and are writable.

```
ContestDefinition {
  id            // stable slug, e.g. "cq-ww-cw"
  name          // display, e.g. "CQ WW DX CW"
  cabrilloName  // Cabrillo CONTEST: token, e.g. "CQ-WW-CW"
  builtin       // true = seeded/read-only; false = user, deletable
  period { start, end, maxHours }   // recurring rule OR explicit; informational
  bands[]                            // valid bands
  modes[]                            // valid modes
  sentExchange:  [Field]             // what I send
  rcvdExchange:  [Field]             // what I capture (per-QSO)
  qsoPointsRule: <PointsRule>        // see vocabulary below
  multiplierRules: [MultRule]        // e.g. { source: CqZone, perBand: true }
  dupeRule: perBand | perBandMode | perContest
  serial:   none | perBand | allBand
  cabrilloMap: [CabrilloColumn]      // ordered mapping → Cabrillo QSO: line
  scoringStrategy?: <named C# strategy id>   // optional escape hatch
}

Field {
  key            // e.g. "rst", "serial", "zone", "state", "section", "grid",
                 //      "name", "power", "check", "precedence", "text"
  label
  type: rst | serial | zone | state | section | grid | name | power | text
  width          // UI hint
  required?
  validate?      // named validator, e.g. "cqZone" (1-40), "arrlSection"
  prefillFrom?   // call-history key, e.g. "state" | "zone" | "name"
}

MultRule   { source: <MultSource>, perBand: bool, perMode?: bool }
PointsRule // declarative table keyed by relation:
           // { sameCountry, sameContinent, otherContinent, sameZone, default }
           // each an int; engine picks the most specific match.
```

**Fixed vocabulary** the engine can evaluate against a QSO (keeps definitions
declarative):

- `MultSource`: `Dxcc`, `CqZone`, `ItuZone`, `State`, `Section`, `WpxPrefix`,
  `Grid` (configurable precision), `Continent`.
- `PointsRule` relations derive from the QSO's DXCC/continent/zone vs the
  operator's own (from session `myExchange`).

The optional `scoringStrategy` names a registered `IContestScoringStrategy` for
contests whose points/mults cannot be expressed in the vocabulary. ~95% of
contests need only the declarative form.

### Validation

`ContestDefinitionService` validates a definition before it can be activated or
saved: non-empty `rcvdExchange`, a resolvable points rule, resolvable mult
sources, and a Cabrillo map that covers the sent/received fields. Invalid
definitions surface an error in the setup dialog and cannot start a session.

## 3. Seeded contest set (extensible)

Ship these built-in (`builtin: true`): CQ WW DX (CW, SSB), CQ WPX (CW, SSB),
ARRL DX (CW, SSB), ARRL November Sweepstakes, ARRL Field Day, ARRL 10-Meter,
NAQP (CW, SSB), IARU HF Championship. Plus fallbacks: **"Generic Serial"** and
**"Generic Grid"** so any unlisted contest is still loggable, and a **"State QSO
Party"** template as a clone base.

## 4. User-created contests

- **Create / Clone / Delete** via the setup dialog:
  - "Create contest" opens an editor building a `ContestDefinition` from the UI
    (name, bands/modes, sent/received exchange fields from the fixed vocabulary,
    points rule, multiplier rules, dupe rule, serial mode, Cabrillo mapping).
  - "Clone" duplicates any built-in as a `builtin: false` starting point.
  - Delete is allowed **only for `builtin: false`**. The server enforces this on
    `DELETE /api/contest/definitions/{id}` (rejects built-ins with 400), not just
    the UI. Built-ins are clone-only, never deletable.
- User definitions persist as files in `%APPDATA%\SDRLoggerPlus\contests\`.

## 5. Contest search

The definition picker in the setup dialog has a **type-to-search filter**
(matches `name`, `cabrilloName`, and mode). The same filter spans built-in and
user contests. Purely client-side over the loaded definition list.

## 6. Data model & persistence

- **New LiteDB collection `ContestSessions`:**
  ```
  ContestSession {
    id, definitionId, label,
    myExchange { cqZone, ituZone, state, section, category, power, grid, ... },
    startedAt, endedAt?, active,
    serialCounters { all?: int, perBand?: map<band,int> },
    bandFilter?, modeFilter?
  }
  ```
  The active session id is also mirrored in settings for fast startup.
- **Extend `ContestInfo`** on `Qso` (Contracts) with structured fields actually
  used for scoring / Cabrillo / display:
  ```
  ContestInfo {
    contestId          // existing — the ContestDefinition id
    sessionId          // NEW — which session
    serialSent         // existing
    serialRcvd         // existing
    exchange           // existing (raw)
    rcvdZone, rcvdState, rcvdSection, rcvdName, rcvdPower, rcvdGrid  // NEW structured
    qsoPoints          // NEW — snapshot at log time (reproducible export)
    isDupe             // NEW — snapshot
    mults[]            // NEW — which mults this QSO claimed
  }
  ```
  Storing computed `qsoPoints` / `mults` makes Cabrillo and score reproducible
  without recomputing, while the engine can still recompute on demand.
- QSOs logged from the contest window carry `sessionId` and `contestId`; casual
  QSOs leave `ContestInfo` null.
- ADIF `CONTEST_ID` maps to/from `ContestInfo.contestId` on import/export.

## 7. Contest field visible in the logger

- Surface `ContestInfo.contestId` as a **"Contest" column in
  `LogHistoryPlugin`**, showing the definition's display name (resolved from id;
  falls back to the raw id if the definition is gone). Blank for casual QSOs.
- Show it in the QSO detail / edit view as well.
- Optional: filter Log History by contest, reusing existing filter machinery.

## 8. Server components

Each is single-purpose and independently testable.

- `ContestDefinitionService` — loads + validates seeded and user definitions;
  create/update/delete (delete rejects built-ins). Exposes
  `GET /api/contest/definitions`, `POST`, `PUT/{id}`, `DELETE/{id}`.
- `ContestSessionService` — start / stop / activate a session; owns serial
  counters (`NextSerial(band)` honoring `serial: none|perBand|allBand`).
- `ContestScoringEngine` — **pure, no I/O**:
  `EvaluateQso(def, session, qso) -> { points, mults, isDupe }` and
  `Recompute(def, session, qsos) -> ScoreSummary`. The crown-jewel unit-test
  target.
- `LogHub` additions — push `ContestState` (score, mults worked/needed, rate,
  next-serial) and `WorkedSetDelta`; a hub method to log a contest QSO.
- `CabrilloExporter` — `GET /api/contest/sessions/{id}/cabrillo`; header from
  station settings + the definition's `cabrilloName`; QSO lines from
  `cabrilloMap`.
- `N1mmUdpBroadcaster` — background service emitting N1MM-compatible
  `<contactinfo>` / `<contactreplace>` / `<dynamicresults>` (score) XML to a
  configurable UDP host/port (opt-in via settings).
- `OnlineScoreReporter` — timer POSTing running score to
  contestonlinescore.com (opt-in; credentials in settings).
- `ScpService` — parses `master.scp` (bundled default + user-updatable); serves
  the call set to the client for local prefix matching.
- `CallHistoryService` — parses an N1MM-format call-history file into an exchange
  lookup; wired into the existing focus-callsign lookup pipeline to prefill the
  received-exchange fields (respecting manual-edit locks).
- `RateService` — QSOs/hr for the active session (last-10, last-100, clock-hour).

## 9. Frontend (dedicated window + panels)

All FlexLayout plugins registered in `plugins/index.ts`.

- **`ContestEntryPlugin`** — the dedicated contest window: call input with inline
  dupe/mult status, ordered exchange fields from the active definition, next-serial
  display, its own Enter/Tab/Space keyboard flow, logs into the active session.
  Independent of `LogEntryPlugin`. Renders SCP suggestions inline from the
  locally-held call set.
- **`ContestBandmapPlugin`** — per-band frequency scale fed by the existing
  cluster/RBN spot streams; click-to-QSY + prefill call; coloring driven by
  contest state (dupe / new-mult / worked / new); stale-aging of spots.
- **`MultNeededPlugin`** — worked-vs-needed grid by band (zones / DXCC / states /
  sections, per the active definition's mult rules).
- **`ContestScorePlugin`** — running QSOs / points / mults / total, plus the
  **rate meter** (readouts + a small rate graph).
- **`ContestSetupDialog`** — pick definition (with search §5), set my exchange /
  category, start / activate a session; create / clone / delete user contests
  (§4); configure UDP, online-score, and SCP / call-history file paths.

## 10. Feature → component map (all 12 + refinements)

| Feature | Where |
|---|---|
| Rule presets | §2/§3, `ContestDefinitionService` |
| Serial generator | `ContestSessionService` |
| Dupe check | `ContestScoringEngine` + optimistic client worked-set |
| Scoring + mults | `ContestScoringEngine` + `ContestScorePlugin` + `MultNeededPlugin` |
| Bandmap | `ContestBandmapPlugin` |
| Call-history prefill | `CallHistoryService` |
| SCP | `ScpService` + inline entry suggestions |
| Rate meter | `RateService` + `ContestScorePlugin` |
| Multiplier-needed | `MultNeededPlugin` |
| Online score | `OnlineScoreReporter` |
| Cabrillo | `CabrilloExporter` |
| N1MM UDP | `N1mmUdpBroadcaster` |
| Search | `ContestSetupDialog` (§5) |
| User-created contests | `ContestDefinitionService` + editor (§4) |
| Contest field in logger | `LogHistoryPlugin` (§7) |

## 11. Error handling

- Invalid / partial definition → validation error in the setup dialog; the
  contest cannot start until valid; generic fallbacks always available.
- Missing `master.scp` / call-history file → feature degrades silently (no
  suggestions / no prefill); never blocks entry.
- Online-score / UDP failures → logged, retried, never block logging.
- Cabrillo with unmapped fields → emits what it can and warns; never silently
  drops QSOs.
- Unknown mult source or points relation → engine skips it and flags it in a
  validation report rather than throwing.
- Delete of a built-in → rejected server-side (400), surfaced in UI.
- Deleting a user definition referenced by past QSOs → allowed; the logger's
  Contest column falls back to the raw id.

## 12. Testing

- **Engine unit tests (primary):** points / mults / dupe correctness per seeded
  contest; per-band vs all-band serial; each dupe-rule variant. Table-driven
  fixtures.
- **Cabrillo golden-file tests** per seeded contest.
- **SCP** parse + prefix-match; **call-history** parse + prefill (lock respect).
- **N1MM UDP** XML-shape tests; **online-score** payload tests (mocked HTTP).
- **Definition validation** tests (reject empty exchange / unresolved mult /
  built-in delete).
- **Frontend:** contest entry keyboard flow; optimistic dupe coloring; bandmap
  coloring from state; setup-dialog search filter; Log History contest column.
- Existing ADIF / QSO / LoTW tests stay green (CONTEST_ID round-trip added).

## 13. Internal build order (one branch)

1. Contracts + `ContestDefinition` schema + seeded JSON + `ContestSessions`
   model + `ContestInfo` extension.
2. `ContestScoringEngine` (pure) + unit tests. `ContestSessionService` (serial).
3. `ContestEntryPlugin` + `LogHub` wiring (log a contest QSO; dupe/serial live).
4. `ContestScorePlugin` + `MultNeededPlugin` + `RateService` (scoring/mults/rate).
5. `CabrilloExporter` (high value — intentionally early once 1–2 exist).
6. `ContestSetupDialog` incl. search + create/clone/delete user contests.
7. `ContestBandmapPlugin`.
8. `ScpService` + `CallHistoryService` (SCP + prefill).
9. `N1mmUdpBroadcaster` + `OnlineScoreReporter`.
10. `LogHistoryPlugin` Contest column + ADIF CONTEST_ID round-trip.

## 14. Contracts / boundaries touched

- **Contracts:** `Qso.ContestInfo` (extended), new `ContestDefinition`,
  `ContestSession`, `ContestState`, `ScoreSummary` DTOs, new SignalR events.
- **Server:** new `Services/Contest/` area, `LogHub` additions, new
  `ContestController`, ADIF CONTEST_ID map in `AdifService`.
- **Frontend:** new plugins (entry, bandmap, mult-needed, score), setup dialog,
  `plugins/index.ts`, `signalr.ts`, `appStore`, `settingsStore`,
  `LogHistoryPlugin` (contest column), `api/client.ts`.

## 15. Out of scope

Contest macros / voice keyer; CQ-S&P / ESM; WinKeyer hardware keying; SO2R /
networked multi-op; rig-audio switching; post-contest adjudication; enriching
WSJT-X QSOs with contest exchange.
