# Contest log separation & per-session operating callsign — design

**Status:** designed 2026-07-26 (deepened from research + a code-grounded buildout pass).
**Stage 1 is a release blocker** — it closes a real wrong-call / LoTW-cert-pollution footgun and
must ship in the same release as the SCP + grid work.

---

## 1. The hazard (verified in code)

Contest QSOs are ordinary `Qso` rows. `ContestService.LogQsoAsync` (`ContestService.cs:162-212`)
builds a QSO via `BuildQso` (`:373-441`) and writes it with `_qsos.CreateAsync` (`:189`) into the
**one shared LiteDB `Qsos` collection**. The only contest marker is `qso.Contest.SessionId`/
`ContestId` (`ContestInfo`, `Qso.cs:217-272`). **No per-QSO operating callsign exists**, and the
operating call used for export is the **global** `settings.Station.Callsign`.

⟹ Run a contest under **any call that isn't your personal one** — a **club call**, a **/P**, a
special-event call — and those QSOs land in your personal logbook and are **auto-uploaded to your
personal QRZ/LoTW under the wrong callsign**, pollute your personal awards, and mis-head your
Cabrillo submission. This is wrong-call logging; it breaks LoTW/award rules. Users must not be able
to do this by accident.

### Which paths actually pick up contest QSOs (corrected — the acute footguns are QRZ + LoTW)
| Path | Selection | Contest QSOs swept? |
|---|---|---|
| **QRZ bulk sync** | `GetUnsyncedToQrzAsync` → `Find(NotSynced\|\|Modified)` (`LiteQsoRepository.cs:359`), from `QrzController.cs:186` | **YES — live hazard** (contest QSOs default `NotSynced`) |
| **LoTW upload** | `GetEligibleQsosAsync` → `SearchAsync` + eligibility (`LotwService.cs:295-322`), stamps global call `:159` | **YES — live hazard** |
| eQSL / ClubLog / HRDLog | per-QSO fire-and-forget in `QsoService.CreateAsync` (`:213-220`) | **NO today** — contest QSOs bypass `QsoService`; still gate for correctness |
| Awards (DXCC/WAS/grids/IOTA/counties/VUCC/FFMA/sat) | `AwardsService.AllQsosAsync` (`:43-46`) **and a direct `GetAllAsync` at `:182`** | **YES — live hazard** |
| Worked-before (spots + WSJT-X decodes) | `SpotStatusService.BuildCacheAsync` → `GetAllAsync` (`:269`) | **YES — live hazard** |
| Dashboard statistics | `GetStatisticsAsync` → `FindAll()` (`LiteQsoRepository.cs:227`) | **YES** |
| ADIF export | `ExportToAdif(list, settings.Station.Callsign)` stamps `STATION_CALLSIGN` (`AdifService.cs:464,796`) | **YES — wrong call** |
| Cabrillo | header + QSO lines from `settings.Station.Callsign` (`ContestService.cs:353`, `CabrilloExporter.cs:34,40,68`) | **YES — mis-headed submission (primary Cabrillo bug)** |

---

## 2. The spine — operating-callsign identity (research-confirmed)

Every serious logger is architected around one rule (N1MM, Log4OM, HRD, Wavelog, RUMlog, cqrlog):
**`STATION_CALLSIGN` is a property of the QSO — the call used on the air — never a global app
setting.** SDRLoggerPlus's global-call assumption is the one thing none of them do.

- **ADIF:** `STATION_CALLSIGN` = the over-the-air call (authoritative QSO identity); `OPERATOR` =
  the person operating; `OWNER_CALLSIGN` = equipment owner; `CONTEST_ID` = the contest tag.
- **LoTW/TQSL:** every signed upload is tied to a **Callsign Certificate** (one specific call) via a
  Station Location. **TQSL validates each QSO's `STATION_CALLSIGN` against the signing certificate** —
  so a club/rover call *cannot* be signed with a personal cert. This is the failure to prevent.
- **Cabrillo:** `CALLSIGN:` = the entrant (contest/club) call; `OPERATORS:` = the op list.

**Non-negotiable invariant:** the call in Cabrillo `CALLSIGN:`, ADIF `STATION_CALLSIGN`, and the
LoTW signing certificate must be the same call, and it must be the call used on the air for that
QSO — the contest/operating call, never a global personal call.

**Two industry camps** (both work *only* because per-QSO callsign is stored correctly):
- **Separate store per contest** — N1MM (a DB file per contest), RUMlog, SkookumLogger, cqrlog;
  merge to the general log afterward via ADIF / native import.
- **Tagged single log** — Log4OM (`CONTEST_ID`), DXKeeper, HRD; filter + export by tag.
- **Per-identity logbooks** — Wavelog: a "Station Location" = callsign + its own upload identity
  (QRZ acct / LoTW cert); the log is routed by identity.

**Best-practice target for us:** a Wavelog-style **operating identity** (a station profile owning
callsign + grid + upload credentials) + RUMlog-style **explicit reviewed merge** contest→general.
We reach it in stages; Stage 1 establishes the per-QSO callsign + the exclusion rule that everything
else builds on.

---

## 3. The central rule

```csharp
// null StationCallsign == legacy/casual == personal. Trimmed, case-insensitive.
public static bool IsPersonalQso(Qso q, string? myCall) =>
    string.IsNullOrWhiteSpace(q.Contest?.StationCallsign)
    || (myCall != null &&
        string.Equals(q.Contest!.StationCallsign.Trim(), myCall.Trim(),
                      StringComparison.OrdinalIgnoreCase));
```
`myCall = settings.Station.Callsign`. **The null branch is the entire installed base + every casual
QSO — it must be provably unchanged.** Store `StationCallsign` on `ContestInfo` (not top-level `Qso`)
so existing BSON docs deserialize to null with **no migration**.

---

## 4. Stage 1 — SAFETY (release blocker)

### 4a. Data model
- `ContestInfo.StationCallsign` (`Qso.cs:217`, `[BsonElement("stationCall")]`, `string?`).
- `ContestSession.OperatingCallsign` (`ContestSession.cs`, `string?`).
- `StartContestSessionRequest.OperatingCallsign` (`ContestDto.cs:6-10`) + TS mirror (`client.ts:1379`).
- **Stamp in `BuildQso`** (`ContestService.cs:393-420`): `Contest.StationCallsign =
  session.OperatingCallsign` — **store-always** (even when equal to personal) for clean Stage-2 filtering.
- **Populate session** in `ContestSessionService.StartAsync` (`:39-73`): accept the call, default to
  `settings.Station.Callsign`. Lock it for the session (changing the operating call mid-session = a new
  session semantically).
- Pass-through `ContestSuiteController.StartSession` (`:96-100`).

### 4b. Export uses the QSO's own call
- **ADIF** `AdifService.cs:796-797`: `qso.Contest?.StationCallsign ?? stationCallsign`.
- **Cabrillo** — the header is the **primary** bug: `GenerateCabrilloAsync` (`ContestService.cs:353-358`)
  must head the log with the **session's `OperatingCallsign`**, not `settings.Station.Callsign`, or a
  club-call session's submission is headed with your personal call. Per-QSO line at `CabrilloExporter.cs:68`
  from `qso.Contest?.StationCallsign ?? stationCallsign`.

### 4c. Upload guards (the footgun)
- **QRZ**: filter at the caller `QrzController.cs:186-187`:
  `qsoList = unsynced.Where(q => IsPersonalQso(q, myCall)).ToList();` — keeps the repo dumb. **Also guard
  the pending-count** `GetPendingSyncCountAsync` (`LiteQsoRepository.cs:371`) so the badge matches what
  actually uploads.
- **LoTW**: add `.Where(q => IsPersonalQso(q, myCall))` in `GetEligibleQsosAsync` (`LotwService.cs:317-321`)
  **and** the explicit-IDs branch (`:300-303`) — a different-call QSO hand-picked into a *personal* cert is
  still wrong; per-cert override comes in Stage 2. myCall already loaded at `:126`.
- **eQSL/ClubLog/HRDLog** (`QsoService.cs:213-220`): gate the three `RecordUpload` calls with
  `IsPersonalQso` — future-proofs + documents the rule (contest QSOs don't reach here today).

### 4d. Awards / stats / worked-before exclusion (operator decision: exclude fully)
- **Awards chokepoint 1**: `AwardsService.AllQsosAsync` (`:43-46`) — `.Where(IsPersonalQso)` **after** the
  `QsoSnapshotCache` returns (keep the snapshot unfiltered/shareable). Inject `ISettingsService` for myCall.
- **Awards chokepoint 2 (easy to miss)**: `AwardsService.cs:182` (`GetVuccStatisticsAsync`) calls
  `GetAllAsync` directly — route it through `AllQsosAsync` or apply the same filter.
- **Worked-before**: `SpotStatusService.cs:279` — `if (!IsPersonalQso(qso, myCall)) continue;` (myCall via the
  scoped provider at `:267`). Confirm contest QSOs trigger `InvalidateCacheAsync` (they broadcast
  `QsoLoggedEvent` at `ContestService.cs:198`).
- **Dashboard stats**: `GetStatisticsAsync` (`LiteQsoRepository.cs:227`) `FindAll()` — the one place with no
  clean caller; filter in-memory with an injected call, or wrap in a service that has settings.

### 4e. Frontend
- **Start form** (`ContestEntryPlugin.tsx` SetupView `:215-236`, inputs `:367-448`): an **Operating
  callsign** input, prefilled from `settings.Station.Callsign`, editable, with an inline warning when it
  differs ("QSOs under W1AW will NOT upload to your personal LoTW/QRZ or count toward your awards —
  Cabrillo only"). Add `operatingCallsign` to the start payload + TS types.
- **Log History badge**: add `StationCallsign` to `QsoResponse` (`QsoDto.cs:61-93`) set in `MapToResponse`
  (`QsoService.cs:293`) from `qso.Contest?.StationCallsign`; show a pill when
  `stationCallsign && stationCallsign !== myCall` (never on the null path).

### 4f. Exhaustive Stage-1 touch-point table
| # | File:line | Change |
|---|---|---|
| 1 | `Qso.cs:217` (`ContestInfo`) | add `StationCallsign` |
| 2 | `ContestSession.cs` | add `OperatingCallsign` |
| 3 | `ContestDto.cs:6-10` | add to `StartContestSessionRequest` |
| 4 | new helper | `IsPersonalQso(qso, myCall)` |
| 5 | `ContestService.cs:393-420` (`BuildQso`) | stamp `Contest.StationCallsign` |
| 6 | `ContestSessionService.cs:39-73` (`StartAsync`) | accept + default + store call |
| 7 | `ContestSuiteController.cs:96-100` | pass through |
| 8 | `AdifService.cs:796-797` | per-QSO `STATION_CALLSIGN` |
| 9 | `ContestService.cs:353-358` + `CabrilloExporter.cs:34,40,68` | header from session call; QSO line per-QSO |
| 10 | `QrzController.cs:186-187` (+ count `LiteQsoRepository.cs:371`) | `IsPersonalQso` filter |
| 11 | `LotwService.cs:317-321` (+ ids `:300-303`) | `IsPersonalQso` filter |
| 12 | `QsoService.cs:213-220` | gate eQSL/ClubLog/HRDLog |
| 13 | `AwardsService.cs:43-46` **and** `:182` | filter after snapshot |
| 14 | `SpotStatusService.cs:279` | skip non-personal |
| 15 | `LiteQsoRepository.cs:227-229` (`GetStatisticsAsync`) | filter (no clean caller) |
| 16 | `QsoDto.cs:61-93` + `QsoService.cs:293` | expose `StationCallsign` |
| 17 | `ContestEntryPlugin.tsx:215-236,367-448` + `client.ts:1379` | operating-call input + warning |
| 18 | `LogHistoryPlugin.tsx` | non-personal-call badge |

### 4g. Correctness risks & tests (the null path is sacred)
- **Installed base unchanged:** `Contest==null` and `Contest.StationCallsign==null` are `IsPersonalQso==true`
  for any myCall → appear in QRZ/LoTW selection, awards, worked-before, and export with the global call.
- **Case/whitespace:** `"w1aw"` == `"W1AW "`.
- **myCall unset (empty):** only null-StationCallsign QSOs are personal (a stamped call never equals empty) —
  verify it doesn't exclude everything.
- **Portable variants:** `W1AW/4` ≠ `W1AW` (distinct LoTW identity) — correct, document so it's not a "bug."
- **QRZ pending-count vs actual:** guard both or the UI count disagrees with the sync.
- **Awards snapshot:** filter after retrieval, never mutate the shared snapshot (two callers, different myCall).
- **Cabrillo header:** a club-call session produces `CALLSIGN: <club>`.

---

## 5. Stage 2 — separation affordances (post-release)
- **Per-contest ADIF export** — add `SessionId`/`ContestId` to `AdifExportRequest` + `QsoSearchRequest`
  (`QsoDto.cs:143-153`) → repo `Find(q => q.Contest.SessionId == …)` (pattern exists: `GetByContestSessionAsync`,
  `LiteQsoRepository.cs:314`). Export just the club Field-Day log for the club's records.
- **Log History filter** — All / Only-contest / Hide-contest / by-session / **by-callsign** (needs a
  `StationCallsign` filter on `QsoSearchRequest`).
- **Per-club LoTW/QRZ upload** — `LotwUploadFilter` already carries `StationCallsign` (`LotwService.cs:153`)
  and TQSL takes a cert (`:178`); map a session's operating call → its own TQSL cert + station location so a
  club run *can* upload under the club identity. This is where Stage 1's blanket exclusion gets a per-cert
  override. Settings needs a call→cert map (the Wavelog "station profile" concept).
- Optional: a toggle to include a specific alt-call in personal award stats (guest-op-at-my-station edge).

---

## 6. Stage 3 — separate / networked contest DB, and the shared-backend convergence
Two forces push toward *storage* separation: (1) **multi-op / LAN** (N1MM model) — a contest is a separate
shared DB for the event, many operators writing concurrently, never attached to a personal logbook, merged
afterward only for the op's own call; (2) **the networked/shared-backend arc** — a shared backend serving
many clients must **route** each QSO to the right store (general vs the active contest) on the same identity
signal Stage 1 introduces.

**Convergence:** Stage 1's `IsPersonalQso` / `StationCallsign` **is** the routing key a shared backend needs.
Design separation as a **second `IQsoRepository` instance** (`ContestQsoRepository`, its own LiteDB file /
networked backend keyed by session), with a thin router: contest-window writes
(`ContestService.LogQsoAsync`) → contest store; casual writes (`QsoService.CreateAsync`) → general store.
Cross-cutting reads (awards, worked-before) union **only the personal-call subset** of the contest store into
the general view — the Stage-1 rule, now enforced at the storage boundary instead of at every consumer.
Merge-to-personal = "copy personal-call QSOs from contest store → general store." **Get `IsPersonalQso` + the
`StationCallsign` stamp right in Stage 1 and Stage 3 is a routing layer over the same key; get it wrong and
every consumer needs its own filter forever.**

**Requirement for the networked arc:** whatever networked `IQsoRepository` is built must accept a
contest-vs-general **store selector**, or contest isolation breaks the moment the backend is shared. See
`networked-shared-database.md`.

---

## 7. Migration / compatibility
`ContestInfo.StationCallsign` is nullable; **all existing QSOs stay null → behave exactly as today**
(uploaded personal, exported with the global call, counted in awards). No migration. The rule treats null as
personal, so nothing changes for solo ops.
