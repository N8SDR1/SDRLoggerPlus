# QSO Frequency Unit Bug — Investigation & Fix Report

**Date:** 2026-07-19
**Reporter:** Brent (N9BC)
**Status:** **Fixed and verified.** Code on `fix/qso-frequency-mhz` (2 commits, **not pushed**);
Brent's live database repaired.
**Branch:** `fix/qso-frequency-mhz` off `v2-alpha` @ `ff54af9`
· `dacce6b` QSL-service FREQ · `6f0bb1c` write paths

---

## 1. Reported symptom

The Log Entry panel showed a 6 m frequency of **50.125**, but the QSO arrived in the
QRZ.com logbook as **0.050**. Same 1000× error on other bands.

## 2. Bottom line

There were **two** unit bugs, and the reported one was the *smaller*.

| | Bug | Scope | Status |
|---|---|---|---|
| **A** | Four write paths stored **MHz** into a **kHz** field → exporters divided again → 1000× low | **63 QSOs** | fixed + data repaired |
| **B** | ClubLog / eQSL / HrdLog emitted raw **kHz** into ADIF `FREQ` (defined as MHz) → 1000× high | **~21,866 QSOs** | fixed |
| **C** | `FREQ_RX` scaled on import but exported verbatim → satellite round-trip 1000× high | round-trip | fixed |

Bug B was silent, automatic on every QSO create, and had been shipping for a long time.
Bug A is what got noticed.

## 3. Canonical unit: kHz

`Qso.Frequency` (`double?`) is stored in **kHz**. Evidence, strongest first:

1. **Five independent code sites agree.** `AdifService.cs:495` multiplies by 1000 on import;
   `:684` divides on export; `:861` multiplies to reach Hz for band derivation;
   `QrzService.cs:586` divides; `CabrilloExporter.cs:166` and `N1mmXmlBuilder.cs:106,113`
   both treat it as kHz.
2. **The live database**, classifying every QSO against the nominal MHz centre of its own
   `Band`: **23,674 kHz** vs **63 MHz**.
3. **630 m proved it** — 1,871 rows stored `474`, correct as kHz, nonsense as MHz.

## 4. Origin of the 63 bad rows

All 63 were FT8, from the WSJT-X path — 49 on 20m (`14.074`, `14.075287`), 14 on 17m (`18.1`).

`WsjtxQsoMapper.cs:16` had written MHz **since the initial commit** — an original defect, not
a regression. The *manual entry* path broke separately and later, in commit **`344d544`**
(2026-07-06, "port v1.x General mode"), which introduced the `/1_000_000` and the
"Frequency (MHz)" label.

## 5. Why nothing in the DB was ever 50.125

There is **no frequency column in the Log History grid**. The 50.125 was the Log Entry
form's **live pre-submit value**, computed from the rig at `LogEntryPlugin.tsx:286`. No 6 m
QSO was ever stored as 50.125 — the real 6 m rows are `50313` kHz. The mechanism was
confirmed regardless: a 20 m FT8 row at `14.074` uploaded to QRZ as `0.014`.

## 6. What changed

### Writers → kHz (`6f0bb1c`)

| Site | Before | After |
|---|---|---|
| `Wsjtx/WsjtxQsoMapper.cs:16` | `Hz / 1e6` | `Hz / 1e3` |
| `Sat/SatControllerService.cs:363` | `downMhz` | `downMhz * 1000` (band + `FREQ_RX` stay MHz) |
| `QsoService.cs:112` | `request.UplinkFreq` | `* 1000` (DTO is MHz on the wire) |
| `Web/plugins/LogEntryPlugin.tsx` | submitted MHz | converts at the submit boundary |
| `Web/plugins/ContestEntryPlugin.tsx:661` | — | **deliberately unchanged** |

`ContestEntryPlugin` reads `rigStatus.frequency`, which is **Hz** (see `bandFromHz` at `:508`),
so its `/1000` already yields kHz. An early draft of this fix would have "corrected" it and
introduced a brand-new 1000× bug in contest logging. Both reviewers caught this independently.

### Exporters → MHz (`dacce6b`)

`ClubLogService.cs:49`, `EqslService.cs:48`, `HrdLogService.cs:46` now divide by 1000 and
format `F6` (invariant culture), matching `QrzService`.

### `FREQ_RX` asymmetry (`6f0bb1c`)

`AdifExtra` is exported **verbatim** (`AdifService.cs:794`), so `FREQ_RX` must be stored in
MHz. Both satellite writers do that correctly, but ADIF import was scaling it to kHz — so an
imported satellite QSO round-tripped 1000× high. Import now scales only `FREQ`.
*This was found during implementation and appeared in neither review.*

### UI

The log forms **still display MHz** — correct for operators and the labels are honest.
Conversion happens only at the submit/load boundary. This made most of the originally
planned frontend work unnecessary: band derivation (`LogEntryPlugin.tsx:1230`) and the POTA
self-spot multiplier (`:862`) operate on the MHz form value and remain correct as-is.

`LogHistoryPlugin`'s edit form previously round-tripped the raw stored value under a
"Frequency (MHz)" label — editing any QSO would have written kHz into a field read as MHz.
It now converts both ways.

The MHz↔kHz conversions live in `Web/src/utils/frequency.ts` beside the existing spot
helpers, since that file exists to stop exactly this class of mistake. Rounding is applied
because `14.074 * 1000` is `14074.000000000002` in binary floating point.

## 7. Data repair (Brent's database only)

Band-anchored predicate, never value-magnitude:

```
repair iff  Band known AND Frequency > 0
        AND NOT inBandKhz(Frequency)        // not already correct
        AND     inBandKhz(Frequency * 1000) // correct once scaled
```

**Provably unambiguous:** overlap would require a band whose upper/lower edge ratio is ≥ 1000.
The widest amateur band is 80 m at **1.143**. A naive "`< 1000` → `×1000`" rule would instead
have corrupted 630 m (`474`) and 2200 m (`137`), where a *correct* kHz value is under 1000.

**Idempotent by construction** — after repair the row satisfies `inBandKhz`, so clause 2 is
false and it can never re-fire. Verified empirically: the second pass found **0** rows.

Result:

```
TO REPAIR         : 63      (49x 20m, 14x 17m)
date range        : 2026-06-19 .. 2026-06-27   (post-hoc assertion held)
REPAIRED          : 63
second-pass       : 0
stored as MHz now : 0
```

End-to-end check — QRZ would now receive `FREQ=14.075287` where it previously got `0.014`.

`UpdatedAt` was deliberately not bumped, so no QRZ/LoTW sync statuses flipped to `Modified`.
Artifacts in `%APPDATA%\SDRLoggerPlus\`: `*.pre-freq-repair-*.bak`, `freq-repair-manifest.csv`,
`freq-outliers-for-review.csv`.

## 8. Testing

`Server.Tests/Tests/Services/QsoFrequencyUnitTests.cs` (new) pins the exporter contract;
`Web/src/utils/frequency.test.ts` covers the storage conversions including the reported
`50.125 MHz → 50125 kHz` case and a round-trip.

```
backend : 846 unit + 82 integration
frontend: 369 tests, tsc clean
```

Two existing tests encoded the old assumption and were corrected:
`WsjtxQsoMapperTests` (`14.074` → `14074`) and `SatControllerServiceTests` (`436.795` → `436795`).

**Why the old suite passed while production was broken:** every test asserted only its own
service's assumption. Nothing checked the exporters against the stored unit, or each other.

**An important negative result:** a cross-service *"all exporters agree"* invariant — which both
reviews recommended — **does not catch this bug.** All three broken exporters agreed perfectly
with each other at `14074`; they were wrong identically. Verified by running the new tests
against the unfixed code: 6 failed, 2 passed, and the agreement test was one of the two that
passed. What catches it is asserting known kHz inputs against expected MHz outputs.

## 9. Downstream contamination

- **QRZ** (`QrzLogId`, `QrzSyncStatus`) and **LoTW** (`Qsl.Lotw.Sent`) have sync tracking, so
  the 63 bad uploads can be targeted for correction.
- **ClubLog / eQSL / HrdLog have no sync tracking at all** and upload fire-and-forget at QSO
  creation (`QsoService.cs:123`). There is no record of what was sent, so no delta can be
  computed — correcting Bug B historically would mean wholesale re-upload relying on each
  service's dedupe. Adding sent-tracking for these three is worthwhile regardless.
- Mitigating factor: all three also send `BAND`, which most services prefer for matching, so
  QSO matching probably survived despite the wrong `FREQ`.

## 10. Open decisions for Rick

1. **Distribution.** The repair ran on Brent's machine only, via a one-off script. Anyone on
   v2.5.0 who used WSJT-X has the same bad rows and **there is no in-app migration**. Options:
   guarded one-shot startup migration (modelled on `UserConfigService.cs:62` →
   `LegacyMigration.MigrateIfNeeded`, with a `BackupService` call first), a manual admin
   endpoint with dry-run, or ship nothing and let it ride. Needs a decision.
2. **Historic re-upload.** Correct the 63 wrong QRZ/LoTW entries, or leave them?
   Bug B's history is not addressable without wholesale re-upload — accept as-is?
3. **Long-term unit.** kHz is now consistent everywhere. MHz would be more ADIF-natural but
   means migrating ~21,866 rows and flipping five correct consumers. Integer **Hz** is
   cleanest — matches every rig API and `BandHelper.GetBand(long)` — but is the largest blast
   radius. Recommendation: stay on **kHz**; revisit Hz only at a schema bump.
4. **`BandHelper` has no 630 m / 2200 m entries** (`BandHelper.cs:8-23`). The migration needed
   its own band table. Worth fixing in `BandHelper` itself.

## 11. Separate defects found along the way (not fixed)

- **60 outlier QSOs** where band and frequency disagree — e.g. `KB8ZXE 2M 3905` (an 80 m
  frequency on a 2 m label), `RZ6BR 30M 14000` (20 m frequency on a 30 m label, ×6 same day),
  `N3HEE 160m 1000`. These match *neither* unit reading, so the migration skipped them by
  construction. Unlike the 630 m rows, the frequency here looks trustworthy and the band is
  probably recoverable from it. Listed in `freq-outliers-for-review.csv`.
- **1,871 "630 m" QSOs** dated 1995–2022, all `474` kHz, 1,767 of them SSB — impossible, since
  630 m was not allocated to US amateurs until 2017. Brent identified these as a bad import
  from another logger with unrecoverable frequency data and **had them deleted** (backup +
  full CSV manifest retained). Separate from the unit bug; noted because it changed the row
  counts above.

## 12. Method / confidence

Diagnosis was verified against the **live database** (read from a copy while the app was
running) and independently reviewed by two senior reviewers working from the code. Both
independently confirmed kHz-canonical, both caught the `ContestEntryPlugin` trap, and both
flagged Bug B as the larger issue.

A third review (Codex) **never completed** — it timed out mid-investigation, and its relaunched
run did not report before this work finished. Its partial output contradicted nothing here but
reached no verdict. **The one thing no reviewer fully swept is whether any *reader* of
`Qso.Frequency` was missed** — worth a second pair of eyes.

An initial diagnosis in the **opposite** direction (treat storage as MHz; change the six
exporters) was drafted and **discarded** after the database check showed it would have
corrupted exports for 23,674 QSOs in order to fix 63. Nothing from it was committed. This is
the strongest argument for the value-based tests in §8: a plausible-looking, internally
consistent fix was wrong, and only real data caught it.
