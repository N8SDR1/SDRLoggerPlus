# Duplicate management — manual find/remove + contest dupe handling

**Status:** design locked 2026-07-28 (operator decisions recorded below). Not yet implemented.
**Depends on:** `docs/design/timezone-architecture.md` **Phase 1** must land first — the duplicate
finder reuses the canonical identity key, and if dates still read back local it inherits the exact
cross-midnight miss this is meant to fix.

Motivation: a tester re-imported the same 16k-QSO ADIF with dupe-skip on and ~4k re-imported (the
timezone bug). Beyond fixing that bug, users want an explicit "find & remove duplicates" tool like
N1MM+/N3FJP — especially before turning in a contest log.

---

## Operator decisions

1. **Scope = BOTH:** a general **whole-logbook** duplicate remover, **and** a **contest** pre-export
   duplicate check.
2. **Contest dupes = do what other loggers do so the submitted log is not flagged as tampered or
   incomplete** → for a contest, dupes are **kept in the Cabrillo and scored 0** (marked as dupes), not
   deleted. Deletion is only for *accidental double-entries* in the general log.

---

## Two distinct things (do not conflate)

| | General-log cleanup | Contest dupe handling |
|---|---|---|
| Target | Accidental **double-entries** — the *same* QSO logged twice (re-import, fat-finger) | A legitimate **repeat** contact with a station already worked on that band+mode in the contest |
| Correct action | **Remove** the redundant record (keep one) | **Keep** it, mark as dupe, **score 0**, include in Cabrillo |
| Why | Two identical records is bad data | Contest robots expect dupes present at 0 pts; removing them reads as a tampered/incomplete log |

The unifying idea: **identity** = the canonical key (call + UTC date + time + band; mode excluded, per
the import dedupe rule). A *double-entry* is two records with the same identity **and** essentially the
same time. A *contest dupe* is a later QSO whose call+band+mode was already worked in the same contest —
a different, legitimate record — which the scoring engine already flags; we just make sure it stays and
scores 0.

---

## Part A — Whole-logbook duplicate remover

**Find:** group records by the canonical identity key with an optional **±time tolerance** (default
±2 min) so near-identical double-entries collapse. A group of 2+ is a duplicate set.

**Keep-one policy** (delete the others), in priority order:
1. Prefer the record already **synced** to QRZ or LoTW (`QrzSyncStatus==Synced` or `Qsl.Lotw.Sent=="Y"`)
   — keeps the local log consistent with what is already online.
2. Else the **most complete** record (fewest empty fields — grid, name, QTH, etc.).
3. Else the **earliest `CreatedAt`** (the original).
Never delete every member of a group.

**Flow (destructive → guarded):**
1. **Dry-run scan** → "Found N duplicate groups, M redundant records." No writes.
2. **Reviewable list** — each group shown with the keep row highlighted and the delete row(s) marked;
   user can **de-select** any group or flip which one is kept.
3. **Auto-snapshot** via the existing backup engine *before* any delete.
4. **Confirm** → delete the marked redundant records via `IQsoRepository.DeleteManyAsync` (local-only;
   verified it does **not** cascade to QRZ/LoTW — see below).

**Upload safety (hard requirement):**
- Deletes are **local-only**: `QsoService.DeleteAsync/DeleteManyAsync → repository.DeleteAsync` — no
  remote QRZ/LoTW/eQSL delete call. Removing a local dupe cannot alter an online logbook. ✓
- Prefer deleting the **un-synced** twin, so we never leave a "deleted locally but still on QRZ" gap.
- The cleanup **never re-uploads** and never routes through `UpdateAsync` (which would flip
  `QrzSyncStatus Synced→Modified` and re-queue QRZ — see the timezone doc §5). Delete-only, never
  re-save.

**Surface:** Logbook tools → **Find duplicates** (whole log).

## Part B — Contest pre-Cabrillo duplicate check

**Goal:** turn in a *complete, untampered* log. So this does **not** delete contest dupes.
- **Detect** repeat QSOs within the contest session (call + band + mode already worked) — the scoring
  engine already identifies these; expose them in a review panel.
- **Ensure each is marked as a dupe and scored 0** (verify the Cabrillo writer emits them at 0 pts, not
  omitted).
- **Cabrillo export keeps them** — confirm no path silently drops dupes.
- The only thing offered for *removal* here is a genuine **accidental double-entry** (identical identity
  **and** near-identical time = the same log action twice), clearly labeled as such and separate from
  legitimate contest dupes.

**Surface:** Contest → **Check duplicates before export** (part of the Cabrillo export flow).

---

## Build order

1. **Timezone Phase 1** (read-projection UTC) — prerequisite; the finder rides on it.
2. **Backend dupe-scan service** — a read-only `FindDuplicateGroups(scope)` returning groups with the
   keep/delete decision pre-computed, reused by both surfaces. Pure, unit-testable (UTC-midnight fixture).
3. **Part A** whole-log tool (backend delete endpoint + frontend review/confirm + auto-snapshot).
4. **Part B** contest check — verify/instrument the score-0 + Cabrillo-keeps-dupes behavior; add the
   review panel; wire the accidental-double-entry removal path.

**Tests:** dupe-scan finds cross-midnight duplicates only under the UTC projection; keep-policy prefers
synced/complete/earliest; delete is local-only and touches no sync flag; Cabrillo still contains
contest dupes at 0 pts after a Part-B run.
