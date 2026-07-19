# Contest customization + ARRL rule audit

**Date:** 2026-07-17
**Branch:** feature/contest-suite
**Status:** approved, implementing

## Goal

Two related asks from the operator:

1. **Strict rule fidelity** — the entry window's exchange fields must match each
   contest's official rules. Scope for this pass: the **ARRL** family.
2. **Full customization** — the operator can author/clone a contest and control
   the **power multiplier**, the **complete QSO exchange**, and **serial-number**
   behavior.

Two decisions locked during brainstorming:

- Contests where the *received* exchange differs for W/VE vs DX stations (10 m,
  160 m, RTTY Roundup) use **true per-QSO branching** — the entry field switches
  as you type a domestic vs a DX call.
- Audit depth is **high-impact fixes** — correct clear field/points/multiplier/
  power mismatches; leave band-weighted VHF/Digital points as a documented
  approximation.

## Background (current state)

- `ContestDefinition` already models `SentExchange`/`RcvdExchange`
  (`List<ContestField>`), `PowerMultipliers`, per-mode points (`PointsRule.ByMode`),
  per-band/per-mode multipliers, and `Serial` (None/PerBand/AllBand). The scoring
  engine consumes all of it.
- `ContestScoringEngine.ClassifyStation(HomeArea?, Qso)` already classifies a
  worked station as InArea/OutArea/Dx (private today).
- The **editor** (`ContestEditor.tsx`) exposes only a subset: no power multiplier,
  no per-mode points, no per-field width/required/key, mult "per band" only. It
  also derives each field's key from its type, so two fields of the same type
  collide.
- The **entry window** renders `definition.rcvdExchange` as a flat list — no
  per-QSO branching.
- `ARRL DX` is already correctly role-split (verified against arrl.org: W/VE send
  RST+state, DX send RST+power; W/VE count DXCC, DX count states/provinces).

## Design

### 1. Model (Contracts) — one new field

`ContestField.AppliesTo : ContestRole?` — show/collect this field only when the
worked station falls in that class (`InArea` = W/VE; `OutArea`/`Dx` = DX). Null ⇒
always. Reuses the existing `ContestRole` enum; no new type. Scoring is unchanged
(it reads whatever exchange values land on the QSO); this only drives which input
the entry window shows and the editor authors.

### 2. Backend — classify the worked station for the UI

- Add `ContestScoringEngine.ClassifyWorked(ContestDefinition, Qso) : ContestRole`
  (public wrapper over the existing private `ClassifyStation`; `All` when the
  contest has no home area).
- Add `WorkedClass` (`ContestRole`) to `ContestCheckResponse`. `CheckAsync`
  already builds an enriched candidate QSO — classify it and return the class so
  the entry window knows, as the operator types, whether to show the state box or
  the serial box.

### 3. Entry window — render fields by class

Filter `definition.rcvdExchange` by `check?.workedClass`, defaulting to `InArea`
before a call resolves (domestic contests are mostly domestic). A field with no
`appliesTo` always shows; `InArea` shows for domestic; `OutArea`/`Dx` shows for
DX. Pure client logic in `ContestEntryPlugin.tsx`.

### 4. Editor — rule-complete

`ContestEditor.tsx` gains:

- **Power multipliers** — `{class → factor}` rows (QRP/LOW/HIGH/custom) →
  `PowerMultipliers`.
- **Per-field control** — explicit **key** (fixes the same-type collision),
  width, required, and an **"Applies to"** selector (Always / In-area / DX).
- **Per-mode points** — CW / Phone / RTTY → `PointsRule.ByMode`.
- **Multiplier "per mode"** toggle (in addition to per band).
- **Serial** mode retained (None / PerBand / AllBand), clearly labeled.

Key derivation changes from "always overwrite from type" to "use the explicit key
if the author set one, else derive-and-deduplicate", so existing behavior is kept
for simple contests but two same-type fields no longer clobber each other.

### 5. ARRL audit — `SeedContests.cs` (high-impact)

| Contest | Change |
|---|---|
| ARRL DX (cw/ssb) | Verified correct — no change |
| Field Day | Add `PowerMultipliers` (HIGH ×1, LOW/100 W ×2, QRP-battery ×5) |
| RTTY Roundup | `HomeArea=WVE`; Rx = RST + state *(InArea)* / serial *(Dx)* |
| ARRL 10 Meter | `HomeArea=WVE`; Rx = RST + state *(InArea)* / serial *(Dx)* |
| ARRL 160 Meter | `HomeArea=WVE`; Rx = RST + section *(InArea)* / RST-only *(Dx)* |
| Sweepstakes, Rookie Roundup | Verify serial/precedence/check/section — no change expected |
| ARRL VHF / Digital | Band-weighted points left as documented approximation |

**Explicitly out of scope** (documented, not built): band-weighted VHF/Digital
points, Mexican-state nuance in ARRL 10 m, IARU HF (previously removed), and
exhaustive value verification against the rules PDFs.

## Testing

- Engine unit tests: conditional-field QSO still scores (mults read regardless of
  branching); Field Day power multiplier scales the final score.
- Editor round-trip: author a contest with a power multiplier + a conditional
  field, save, reload, values preserved.
- Update existing tests for the `ContestCheckResponse` (new `WorkedClass`) and
  `ContestField` (new `AppliesTo`) shapes.
- `tsc` clean; `dotnet build`/test green (excluding the two pre-existing,
  unrelated AI-provider test failures).

## Risks

- Backwards compatibility: `AppliesTo`/`WorkedClass` are additive and nullable;
  existing user JSON contests and stored sessions deserialize unchanged.
- Default class before a call resolves could briefly show the domestic field for a
  DX-first workflow; acceptable and self-corrects on the first check response.
