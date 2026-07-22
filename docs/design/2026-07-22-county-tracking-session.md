# County tracking + log cleanup — session record, 2026-07-22

Branch `feature/county-capture` → **PR #29** (base `v2-alpha`). Three commits.
This is the *why* document; the code and commit messages carry the *what*.

| Commit | Subject |
|---|---|
| `ba74cc7` | feat(awards): US Counties Award (USA-CA) tracking + ADIF county capture |
| `1c5da8f` | feat(log): capture state + county on live-logged QSOs; fix CT reference data |
| `2eff003` | feat(log): multi-select delete in Log History |

Final state: **937 unit + 82 integration + 369 frontend** pass, `tsc` clean.
Live totals on the real log (24,548 QSOs at end of session): **1,783 / 3,143
counties worked, 315 confirmed, all 50 states**.

> **Count note.** Earlier in the session the log read 24,554; it ends at 24,548.
> Brent deleted QSOs by hand while testing was in progress, which accounts for
> the difference. Verified independently that no test data was left behind and
> nothing was lost to a code path: no `N9TST*`/`N9BULK`/`N0CLD`/`N5CLD` remain,
> the 154 `W1AW` in the log are genuine contacts, and the only mass-delete path
> (`DeleteAllAsync`) is reachable only via ADIF import with "clear existing
> logs", which was never run.

---

## 1. The finding that shaped everything

The feature was specced as "add USA-CA county tracking." Built to spec, it would
have shipped a scoreboard reading **0 / 3,077 forever**.

`Station.County` was populated on **zero** of the ~24.5k QSOs, and no code path
ever wrote it. Cause: a one-line omission in ADIF import — `state` was in the
mapped-fields set, `cnty` never was — so every imported county fell through to
the unmapped-extras bucket.

Measured on the live log before writing any feature code:

- **15,318 QSOs** carried `AdifExtra.cnty`, 100% in `ST,County` form
- ~1,814 distinct counties, entirely invisible to the app
- The county-hunting history was never missing. It was **misfiled**.

**Why this mattered more than the feature:** the spec's premise was wrong, and
no amount of correct implementation would have surfaced it. Checking the data
before building is what turned a dead scoreboard into 56% completion on day one.

### Consequence: read-time fallback, not migration

`CountyResolver` reads `Station.County` first, then falls back to ADIF `cnty` in
the extras. Deliberately **no migration and no data mutation**:

- Existing logs work the instant it ships — nobody has to run anything
- Nothing rewrites 15k rows of the user's primary asset
- The import fix makes newly logged QSOs canonical going forward
- Both paths converge on the same answer

---

## 2. Design decisions worth remembering

**State placement is the caller's WAS verdict.** `CountyResolver.Resolve(qso, state)`
takes the state rather than deriving it, so USA-CA and WAS can never disagree
about which QSOs are US. The `ST,` prefix in the ADIF value is *dropped, not
trusted* — it disagreed with the resolved state on **226 of 15,318** QSOs.

**Worked and confirmed reported separately.** Kept out of
`AwardsService.PortedAwards.cs` on purpose: those are worked-only by design so
their totals match SDRLogger+. USA-CA is a confirmation award. Confirmed = QSL
card or eQSL; **LoTW is excluded** because it doesn't reliably carry CNTY, so it
can't vouch for the county a QSO claims.

**Validation over guessing.** `CountyNameNormalizer` folds case, punctuation and
governing-type suffixes (County/Parish/Borough/Census Area) so one county can't
scatter across several award entries. A name the reference list doesn't
recognise **fails visibly** rather than being binned into a neighbour — a typo
should read as "not worked", never as the wrong county.

**Denominator honesty.** The embedded list is Census-derived (~3,143); MARAC's
official USA-CA list is ~3,077. The CSV header, the loader docs *and* a UI
footnote all say: progress tracking, not a submission count.

---

## 3. The Connecticut bug — why it's the interesting one

While verifying, CT read **0 of 9 worked** despite a county-bearing QSO.

The 2022 Census abolished Connecticut's 8 counties and replaced them with 9
"planning regions" — and the source CSV carried the new ones. But ADIF CNTY,
USA-CA, and every ham logger still use the **historical counties**. So every
single CT county in a real log failed to match.

Corrected to the 8 historical counties (pinned by test). Result: CT went
**0/9 → 8/8**. Connecticut had been complete all along, hidden behind 196
unmatchable QSOs. Totals moved 1,775 → 1,783.

**The lesson:** authoritative upstream data can be authoritatively wrong *for
your domain*. The Census is correct about Connecticut; it's just not the
authority amateur radio uses. Worth re-checking any other place we inherit
reference data from a general-purpose source.

---

## 4. Bugs found by testing live, not by testing

Every one of these passed a green test suite and was only caught by driving the
real app against the real log. Listed because the pattern is the point.

| Bug | How it surfaced | Why tests missed it |
|---|---|---|
| `Station.County` empty on every QSO | Queried the log before coding | Nothing to test — the field was simply never written |
| CT 0/9 worked | Logged a CT QSO, watched the tab | Reference data was internally consistent, just wrong |
| Counter never moved on add/delete | Watched the tab after a mutation | Query-key mismatch is invisible to unit tests |
| Edit QSO had no County field | Brent opened the modal | No test covered the modal's field list |
| API returned `county: null` after storing it | Read the API response, not the DB | `StationInfoDto` lacked the field; storage was fine |

The counter bug is the subtle one: the tab's React Query key was
`['counties-statistics', ...]` while every QSO mutation invalidates
`['statistics']`. React Query matches by **prefix**, so the invalidation never
reached it and the tab sat on cached numbers. Fixed by nesting the keys.

---

## 5. Multi-select delete (commit 3)

Straightforward feature, three deliberate choices:

- **Checkbox-only selection** (`enableClickSelection: false`) — clicking a row
  to read it must never silently arm a delete.
- **Selection scoped to the visible page.** Paging or refiltering clears it, so
  the count in the action bar always matches what's on screen and the delete
  can never reach rows you can't see. (Brent chose this over cross-page
  selection.)
- **One `POST /api/qsos/bulk-delete`, not N DELETEs.** A page is 50 rows; the
  batch costs one LiteDB checkpoint instead of 50 and one contest-session
  refresh instead of 50. POST because the id list travels in the body and
  DELETE bodies are unevenly supported. The response reports `Deleted` alongside
  `Requested` so a QSO already removed elsewhere reads as a smaller count, not
  an error.

---

## 6. Deliberately not done

- **Live-logging capture was a separate commit, not folded into the first.**
  County-without-state would have been worse than neither, so it waited until
  `CreateQsoRequest` could carry both.
- **"Select all matching filter"** — offered, declined for now. It's the answer
  if undoing a bad 500-QSO import ever comes up; per-page means 10 rounds.
- **Older award tabs' staleness, missing delete/update SignalR broadcasts,
  orphaned QSL ledger rows on delete.** All pre-existing, all surfaced by this
  session's testing, none touched — flagged as separate task chips so this PR
  stays reviewable. See "Known gaps" in `PROJECT_PRIMER.md`.

---

## 7. Caveats worth knowing

Two things that are easy to assume wrongly from reading the diff:

**Legacy QSOs *do* round-trip county through ADIF export**, even before the
import fix — `AdifExtra` exports verbatim, so a `cnty` sitting in the extras
comes back out. Confirming that surfaced an adjacent bug: a QSO carrying *both*
a canonical `Station.County` and a stale extras copy emitted `CNTY` twice.
Export now skips the extras copy when the canonical field was written.

**Counties-tab refresh is per-window, not global.** Create, edit, delete and
import all refresh the tab in the window that performed them. A second window
stays stale on edits and deletes, because `UpdateAsync`/`DeleteAsync` broadcast
no SignalR event — only create does. Pre-existing; see "Known gaps".
