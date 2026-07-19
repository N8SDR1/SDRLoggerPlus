# Sub-project B — Contest operating UX

**Date:** 2026-07-17
**Depends on:** sub-project A (role-aware scoring engine, committed `1b1aff1`).
**Goal:** make the contest entry window and setup honor each definition's rules
and the operator's role, fixing the concrete bugs the user reported (FT8/FT4 and
WARC bands offered in contests that forbid them; no way to set the operator's
county; no in/out-of-state indication).

## Problem

The engine is role-aware, but the UI still ignores the definition:

1. **Band/mode dropdowns are hardcoded.** `ContestEntryPlugin.tsx:15-16` defines
   module-level `BANDS`/`MODES` and the entry dropdowns render those globals, so
   every contest offers FT8/FT4 and (once WARC bands are added to the list) 30/17/12m
   regardless of what the contest allows. This is the root of the Wisconsin QSO
   Party FT8 complaint.
2. **No operator county/state input.** Setup only shows a "My state" box when the
   *top-level* sent exchange has a state field. QSO-party sent exchange is now
   role-dependent, so the top level often has neither — the operator can't declare
   where they're operating from, which is exactly what `DetermineRole` needs.
3. **No role feedback.** Nothing tells the operator whether the engine classified
   them as in-state, out-of-state, or DX — so a mis-entered state silently scores
   wrong.
4. **Cabrillo sent exchange ignores role + county.** `CabrilloExporter.SentValue`
   has no county case and always uses `def.SentExchange`, never the role override,
   so an in-state op's Cabrillo would emit the wrong sent exchange.

## Changes

### Entry form — enforce bands/modes (fixes bug #1)
- Drop the hardcoded `BANDS`/`MODES` module constants as the source of the
  dropdowns. Keep a small `FALLBACK_BANDS`/`FALLBACK_MODES` only for the (rare)
  case where the definition hasn't loaded yet.
- `EntryView` derives `bands = definition?.bands ?? FALLBACK_BANDS` and
  `modes = definition?.modes ?? FALLBACK_MODES`.
- Initialize `band`/`mode` to the first allowed value once the definition loads,
  and clamp: if the current selection isn't in the allowed set (e.g. carried over
  from a prior session), reset to the first allowed value.
- The follow-rig effect only applies the rig's band/mode when it's within the
  contest's allowed set; otherwise it leaves the last valid selection (the rig may
  legitimately sit on a WARC band during a non-WARC contest while tuning around).

### Setup — My-State + My-County (fixes bug #2)
- Add `county?: string` to the client `ContestMyExchange` (backend `MyExchange`
  already has `County`).
- Add `homeArea?` (`{ kind, states }`) to the client `ContestDefinition` interface
  so setup knows the contest is a QSO party. (The definitions endpoint already
  returns the full model; only the TS type needs the field.)
- When `definition.homeArea?.kind === 'StateCounty'`, always show a **My state**
  and **My county** input (independent of the top-level sent exchange). The state
  drives `DetermineRole`; county is the in-area sent exchange.

### Role badge (fixes bug #3)
- Add `Role` (`ContestRole`) to `ContestStateDto` and the TS `ContestStateEvent`.
  `ContestService.BuildState` passes `session.Role`.
- The entry header renders a small badge: **In-State** / **Out-of-State** / **DX**
  for roles other than `All` (hidden for `All`, i.e. global contests).

### Cabrillo per-role sent exchange (fixes bug #4)
- `CabrilloExporter` resolves the effective sent-exchange fields for the session's
  role (`def.Roles?[session.Role]?.SentExchange ?? def.SentExchange`).
- `SentValue` emits `me.County` for a sent field whose key is `county` (county is a
  `Text` field, not a distinct `ContestFieldType`).

## Out of scope (belongs to sub-project C)
- Populating each definition's `Bands`/`Modes`/`HomeArea`/`Roles` from the research
  batch files. B only makes the UI *honor* whatever the definition says; C fills the
  definitions in. The visible fix (no FT8 in Wisconsin) fully lands only once C sets
  that definition's modes, but the enforcement mechanism is B.
- Per-role *received* exchange rendering. QSO-party definitions will model the
  received location as a single `state`-keyed field (accepts county or S/P/C), which
  the existing top-level `rcvdExchange` rendering already handles.

## Testing
- Backend: extend `CabrilloExporter` coverage with an in-state QSO-party session
  (county sent) asserting the county token appears; role resolves to the override
  sent fields. Existing engine tests already cover role scoring.
- Frontend: `tsc` clean; unit-render the entry dropdowns from a definition with a
  restricted band/mode set and assert FT8 / 30m are absent.
- Manual: start an Ohio-QP session as an out-of-state op, confirm the **Out-of-State**
  badge and that only the contest's bands/modes appear.
