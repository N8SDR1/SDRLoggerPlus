# Time-zone architecture — QSO date/time, once and for all

**Status:** analysis + recommended design (2026-07-28). Not yet implemented.
**Why this doc exists:** QSO date/time handling has been "fixed" 4+ times and each fix moved the
break somewhere else. This is the single source of truth for *what frame a QSO time is in* and how we
stop patching it consumer-by-consumer.

---

## 0. TL;DR

- **Root cause:** there is **no canonical stored time frame**. Every write path stamps whatever
  `DateTimeKind` it happens to have, and **LiteDB 5.0.21 returns every `DateTime` as `Kind=Local` on
  read** — so the true UTC instant is silently re-projected to the server's local wall-clock the moment
  it comes back out. Every historical fix is a *consumer-side* workaround for that one read behavior.
- **The rule the whole app should obey:** a QSO time is a **UTC instant, everywhere, always** —
  stored UTC, read back UTC, converted to local **only** at the moment of display. This is exactly how
  the amateur-radio logging world already works (ADIF and Cabrillo are UTC by spec).
- **The fix is a read-projection change, not a data migration.** LiteDB already stores the correct UTC
  instant on disk; only the *read* mislabels it. Fix the read once (a global `BsonMapper` `DateTime`
  deserializer that returns `Kind=Utc`) and every consumer sees the truth at once. **No rows are
  rewritten, no sync flags are touched — so it cannot re-trigger a single QRZ/LoTW/eQSL upload.**
- **The landmine to never step on:** a bulk migration that re-saves rows through
  `LiteQsoRepository.UpdateAsync` would flip `QrzSyncStatus Synced→Modified` on the entire logbook and
  re-queue it all to QRZ. The read-projection approach avoids this entirely.

---

## 1. How the logging world handles time (the convention we should match)

Every serious amateur logger — N1MM+, N3FJP, Log4OM, DXKeeper, ACLog, Cloudlog, WSJT-X — converges on
the same model, because the interchange formats force it:

- **ADIF** (`QSO_DATE`, `TIME_ON`, `TIME_OFF`) is **UTC**, by specification. No exceptions.
- **Cabrillo** QSO lines are **UTC**.
- **LoTW / eQSL / QRZ / Club Log** all match contacts in **UTC** (call + band + mode + UTC date/time,
  usually with a small tolerance because sloppy loggers disagree at the day boundary).
- Therefore the **canonical stored time is UTC**. The logger stores one unambiguous UTC instant per QSO.
- **Display is a presentation choice made at the edge.** The entry screen almost always shows a **UTC
  clock** (operators log in UTC). Grids/reports may offer a **UTC⇄local toggle**, but the stored value
  never changes — only the rendering does.

The lesson: **store UTC, convert at the view.** Never store local. Never let two fields (a date and a
separate time string) carry the instant independently — derive both from one UTC value.

---

## 2. Current state — the full map

### 2a. Write paths — what `Kind` lands in `QsoDate`

| Path | Frame written | file:line |
|---|---|---|
| ADIF import | `Kind=Utc` (`AssumeUniversal\|AdjustToUniversal` → `SpecifyKind(Utc)`) | `AdifService.cs:647-651, 752` |
| WSJT-X / JTDX | `Kind=Utc` (epoch UTC, timespec discarded) | `WsjtxMessageReader.cs:196-202` |
| ADIF-over-UDP | `Kind=Utc` (shares import pipeline) | `AdifUdpListenerService.cs:118` |
| Contest | `Kind=Utc` (`DateTime.UtcNow`) | `ContestService.cs:379,389` |
| Satellite | `Kind=Utc` (`DateTime.UtcNow`) | `SatControllerService.cs:342,446` |
| **Manual entry** | **Unpinned** — inherits client JSON Kind (`Z`→Utc, offset→Local, none→Unspecified). No `SpecifyKind`. | `QsoService.cs:158` |

Modern manual entries are actually UTC in practice — the Log Entry form submits `.toISOString()` (`Z`) —
but the *server* never guarantees it, which is the one write-side hole.

### 2b. The read projection — the linchpin

- LiteDB opened `Filename=…;Connection=shared` — **`UtcDate` not set** (`LiteDbContext.cs:96`).
- LiteDB 5.0.21 stores dates on disk as **UTC**, but on read **converts to local and returns
  `Kind=Local`**. Commit `1daa146` empirically confirmed that **setting `UtcDate=true` does *not*
  change this** in this version — so the connection-string flag is a dead end.
- Net effect: a `Kind=Utc` instant goes in, the **same instant** comes back but labeled/shifted to the
  server's local wall-clock. Any consumer that formats `QsoDate` **without** `.ToUniversalTime()` emits
  the **local** calendar day — a day off for evening QSOs.

### 2c. Consumers — who assumes what

| Consumer | Frame | Correct? | file:line |
|---|---|---|---|
| ADIF export | derives QSO_DATE **and** TIME_ON from one `.ToUniversalTime()` | ✅ | `AdifService.cs:857-860` |
| LoTW upload | via ADIF export | ✅ | `LotwService.cs:159` |
| WSJT-X / UDP inbound | stored UTC | ✅ | `WsjtxQsoMapper.cs:23-24` |
| Confirmation merge | call+band+mode, **±1-day** tolerance | ✅ (band-aid) | `AdifService.cs:317-319, 504-514` |
| **Import dedupe key** | existing rows read **local** `.Date` vs incoming **UTC** `.Date` | ❌ **misses cross-midnight dupes** | `AdifService.cs:148 vs 181/307` |
| **QRZ upload** | `QsoDate.ToString("yyyyMMdd")`, no convert + raw `TimeOn` | ❌ emits local date | `QrzService.cs:586-587` |
| **eQSL upload** | same pattern | ❌ | `EqslService.cs:45-46` |
| **Club Log upload** | same pattern | ❌ | `ClubLogService.cs:43-44` |
| **HRDLog upload** | same pattern | ❌ | `HrdLogService.cs:76-77` |
| **Cabrillo** | UTC date but **unconverted `TimeOn`** string for time | ⚠️ fragile | `CabrilloExporter.cs:68-69, 191-196` |
| "Today" tile | local day (`ToLocalTime().Date`) — deliberate | ✅ (intentional) | `LiteQsoRepository.cs:268-269` |
| Date-range filter | un-normalized compare | ❌ off by the offset | `LiteQsoRepository.cs:83-92` |
| API JSON `QsoDate` | serialized with server **local offset** (not `Z`) | ⚠️ | `QsoService.cs:353` |
| Grid Date/Time, Log Entry, Edit modal | forced UTC via `qsoDateTime.ts` | ✅ | `LogHistoryPlugin.tsx`, `LogEntryPlugin.tsx` |
| Recent strip + delete dialog | `toLocaleDateString()` (local) | ❌ can show a day off | `LogHistoryPlugin.tsx:1349,1404` |

**The two independent carriers of the instant** (`QsoDate` datetime **and** the separate `TimeOn`
string) are their own hazard: contest once stored `QsoDate=now.Date` (midnight) and kept the real time
only in `TimeOn`, so export — which derives both from `QsoDate` — wrote `TIME_ON=000000` for every
contest QSO (`0c88056`). Two sources of one truth will always eventually disagree.

### 2d. The history — same wound, reopened

| Commit | Fix | Layer |
|---|---|---|
| `34165b3` | export derives both fields from `ToUniversalTime()`; dedupe key defined | baseline |
| `79423e4` | confirmations match ±1 day (1,684 lost) | confirmation |
| `98378d0` | "Today" tile count by local day | stats |
| `2122bd5` | contest rate/hr used `Now`+`ToLocalTime` | contest stats |
| `1daa146` | documents LiteDB `Kind=Local` / `UtcDate` ignored | (test) |
| `62670a7` | edit modal + Date column forced UTC (20.4% off by a day) | frontend |
| `0c88056` | contest carries full timestamp in `QsoDate` (TIME_ON=000000) | contest export |
| `60a763e` | dupe-window keyed on `CreatedAt` to "sidestep QsoDate quirks" | dedupe |
| `b24bdda` | import dedupe drops mode (4.26% dupes) | dedupe |

Every row is a consumer normalizing around the same unfixed read behavior — several openly deferring
"normalize storage" to later. **This is the thrash we are ending.**

---

## 3. Root cause (stated precisely)

1. **No canonical frame is documented or enforced.** `Qso.QsoDate` has no XML-doc saying "UTC instant"
   (`Qso.cs:30`), so every author guesses.
2. **The read projection lies.** LiteDB 5.0.21 hands back `Kind=Local`, and the `UtcDate=true` fix for
   it is inert. So even data written correctly as UTC reads back mislabeled, and any un-defended
   consumer re-locals it.
3. **The instant is stored twice** (`QsoDate` + `TimeOn`), which lets the two drift.

---

## 4. Recommended architecture

**Canonical rule (write it into `Qso.cs` as a doc-comment and hold the line):**
> `QsoDate` is the **UTC instant** of the contact (date **and** time). `TimeOn`/`TimeOff` are the ADIF
> UTC `HH:MM(:SS)` strings **derived from that same instant**. Local time exists **only** in the view
> layer. Nothing stores local.

Achieved in three moves, in this order:

### Phase 0 — prove the mechanism (before touching anything)
Write a throwaway unit test that: opens a LiteDB file, inserts a doc with a `Kind=Utc` `DateTime`, reads
it back, and asserts the Kind/value **with and without** a custom `BsonMapper` `DateTime` deserializer.
Confirm (a) the on-disk instant is UTC, (b) default read is `Kind=Local`, (c) a
`mapper.RegisterType<DateTime>(serialize: d => d.ToUniversalTime(), deserialize: v => DateTime.SpecifyKind(v.AsDateTime, DateTimeKind.Utc))`
returns `Kind=Utc` **without changing the stored bytes**. *Do not build on assumptions — this version
already surprised us once.*

### Phase 1 — fix the read projection (the core fix)
Register the global `DateTime` serializer in `LiteDbContext`'s `BsonMapper` so **every** `DateTime`
(`QsoDate`, `CreatedAt`, `UpdatedAt`, contest timestamps…) reads back `Kind=Utc` = the true stored
instant. Effects:
- **Dedupe key** now compares UTC `.Date` on both sides → **K3UK's re-import bug is fixed** at the source.
- The four "roll-their-own" uploaders (QRZ/eQSL/ClubLog/HRDLog) now emit the correct UTC date on the
  **next** upload of genuinely-unsynced QSOs (already-synced rows are untouched — see §5).
- Defensive consumers stay correct: `.ToUniversalTime()` on a `Kind=Utc` is a no-op; `.ToLocalTime()`
  (Today tile) converts properly → still a correct local day.

### Phase 2 — close the write-side hole + collapse the band-aids
- **Pin manual entry:** `QsoService.CreateAsync/UpdateAsync` do `SpecifyKind(request.QsoDate.ToUniversalTime(), Utc)` so the server no longer trusts the client's Kind.
- **Single source of the instant:** make `TimeOn`/`TimeOff` **derived** from `QsoDate` on write (or make
  the uploaders/Cabrillo derive time from `QsoDate.ToUniversalTime()` like ADIF export already does),
  so the date and time can never disagree. Fix Cabrillo's time field (`CabrilloExporter.cs:191`).
- **Retire the workarounds carefully, one at a time, each behind its own boundary test:** the range
  filter (`LiteQsoRepository.cs:83`), the local render spots (`1349/1404`), and the API-JSON offset
  (serialize `QsoDate` as `Z`). Keep the confirmation ±1-day tolerance (foreign logs are still sloppy)
  and keep the Today tile **local by intent** — but now computed from a correct UTC instant.

---

## 5. Re-upload safety — the hard constraint, and why the plan honors it

**A time change on our side must never make an already-synced QSO look new/edited and re-upload it.**
The research is unambiguous:

- **Upload selection never keys on `QsoDate`.** QRZ selects `QrzSyncStatus ∈ {NotSynced, Modified}`
  (`LiteQsoRepository.cs:389-396`); LoTW selects on `Qsl.Lotw.Sent` (`LotwService.cs:331-344`); eQSL/
  ClubLog/HRDLog fire once at create. **All flag-driven, none date-driven.**
- **The catastrophic path is `UpdateAsync`.** `LiteQsoRepository.UpdateAsync` bumps `UpdatedAt` and
  flips `QrzSyncStatus Synced→Modified` (`:179-184`); `GetUnsyncedToQrzAsync` then re-selects the whole
  logbook. A bulk migration that re-saves rows through `UpdateAsync` / `QsoService.UpdateAsync` would
  **re-queue every QRZ QSO** — and because `QrzService` doesn't emit the stored `QrzLogId`
  (`:580-593`), the remote side would **insert duplicates**, not update in place. This is precisely the
  "user's online logbook doubles" disaster.
- **Why the recommended fix is safe by construction:** Phase 1 changes only how rows **deserialize**.
  It rewrites **no rows**, runs through **no** `UpdateAsync`, and touches **no** sync flag. Every upload
  gate reads `QrzSyncStatus`/`Qsl.Lotw.Sent`, **never `QsoDate`** — so a read-projection change **cannot
  re-queue or re-trigger any upload.**

**Migration rules (must-not-touch list):**
1. **Never** re-save rows via `IQsoRepository.UpdateAsync` / `QsoService.UpdateAsync` for a time
   migration — it flips QRZ status and bumps `UpdatedAt`.
2. **Never** modify `QrzSyncStatus`, `QrzSyncedAt`, `QrzLogId`, `LotwSyncStatus`, `LotwSyncedAt`,
   `Qsl.Lotw.Sent`, or the `QslSync` ledger (`Qso.cs:96-117`).
3. **Never** bump `UpdatedAt` as a side effect.
4. If any row genuinely must be rewritten (see §6), write the datetime field **only**, via a direct
   `_context.Qsos.Update` on a row whose sync flags you have explicitly preserved — mirroring the
   pattern the code already uses to dodge this exact hazard at `LiteQsoRepository.cs:193-201`.

---

## 6. The one data tail: historical double-shifted manual entries

If a manual QSO was ever created from a client that sent a **naive** datetime (`Kind=Unspecified`),
LiteDB's write-side `ToUniversalTime()` treated it as local and shifted it by the server offset **before**
storage — so those specific rows are stored wrong by one offset, and Phase 1 alone won't fix them
(it faithfully returns the wrong-but-stored instant as UTC). Modern entries submit `Z`, so this is
expected to be a small, older tail.

Handle it **separately and only if measured to matter:** identify affected rows (heuristic: `QsoDate`
time-of-day vs `TimeOn` string disagree by exactly the server's historical UTC offset), and correct
them with a **flag-preserving direct write** per §5 rule 4 — never through `UpdateAsync`, never touching
sync state. This is a data-repair job, decoupled from the architectural fix.

---

## 7. Test plan (the boundary that keeps biting)

Add a **UTC-midnight boundary** fixture — a QSO at, say, `2026-03-10 02:30 UTC` (previous local day for
western offsets) — and assert every path agrees:
1. **Import → re-import** the same file: second import skips **100%** (the K3UK regression).
2. **Import → ADIF export**: QSO_DATE + TIME_ON round-trip byte-identical.
3. **Cabrillo**: date and time both UTC and self-consistent.
4. **QRZ/eQSL/ClubLog/HRDLog** ADIF builders: emit the UTC date.
5. **Grid, edit modal, Today tile, range filter**: agree on which day the QSO belongs to (grid=UTC,
   Today=local-by-intent, both derived from the same correct instant).
6. **Re-upload safety regression:** run the Phase-1 mapper change and assert `GetUnsyncedToQrzAsync`
   and LoTW `GetEligibleQsosAsync` return the **same set** before and after (no row re-queued).

Run the suite off-UTC (set the test host to a non-UTC zone) — the CI runner is UTC, which is exactly why
`1daa146`-class bugs hid.

---

## 8. Sequenced work

1. **Phase 0** — LiteDB round-trip probe test. *(confirms the whole approach)*
2. **Phase 1** — global `BsonMapper` `DateTime`→`Kind=Utc` deserializer + the re-upload-safety
   regression test. *Ships the dedupe fix.*
3. **Phase 2** — pin manual write path; derive `TimeOn` from `QsoDate`; fix Cabrillo time, range filter,
   API-JSON `Z`, and the two local render spots — each behind a boundary test; retire redundant
   band-aids.
4. **Phase 3 (optional)** — measure + repair the double-shifted manual-entry tail (§6), flag-preserving.
5. **Later** — this also unblocks multi-op **S4c** (contact-time into host frame for Cabrillo), which
   `d0f2dc4` deferred onto this same foundation.

**Do not** attempt a "rewrite every row to UTC" migration. The data is already UTC on disk; the fix is
the read projection, and rewriting rows is how we'd double someone's QRZ log.
