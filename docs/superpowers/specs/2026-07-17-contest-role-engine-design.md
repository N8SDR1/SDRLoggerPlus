# Contest Role Engine — Design (Sub-project A)

**Status:** approved 2026-07-17. Foundation for making all ~97 built-in contests
rule-accurate (modes, bands, exchange, points, multipliers), especially the
in-state / out-of-state behaviour of QSO parties and ARRL DX.

## Problem

The current `ContestDefinition` holds one flat rule set: one sent/received
exchange, one points rule, one multiplier list. Real contests — every QSO party,
ARRL DX, CQ 160 — apply **different rules depending on where the operator is**:

| Ohio QSO Party | In-state op | Out-of-state op |
|---|---|---|
| Sends | RST + **county** | RST + **S/P/DX** |
| Works for score | everyone | **Ohio stations only** |
| Multipliers | states + provinces + DXCC | Ohio **counties** |

Additionally the entry form ignores each contest's `Bands`/`Modes` (offers FT8
and every band regardless), there is no field for the operator's own **county**,
and points cannot vary **by mode** (CW=2 / PH=1).

## Core concept: operator role + role variants

An operator has a **role** for a given contest, fixed at session start:

- `All` — global contests with no location split (CQ WW, WPX, WAE…). Default.
- `InArea` — inside the contest's home area (in Ohio; W/VE for ARRL DX).
- `OutArea` — outside it (the rest of W/VE, and DX, unless the contest splits DX out).
- `Dx` — used only where a contest treats DX distinctly from OutArea.

Once the role is known the contest collapses to a single rule set. So we **store
the rules per role and select one when the session starts.** One engine, no
per-contest special-casing.

## Model changes (`SDRLoggerPlus.Contracts.Models.Contesting`)

### `ContestDefinition` (additions; all optional → existing defs unchanged)
- `HomeArea? HomeArea` — defines "in-area" membership and how a worked station is
  classified. `null` ⇒ no role split (everyone is `All`).
- `Dictionary<ContestRole, RoleRules>? Roles` — per-role overrides. Missing role
  or missing field ⇒ fall back to the top-level `SentExchange` / `RcvdExchange` /
  `QsoPoints` / `MultiplierRules`. This is what keeps the ~90 non-role contests
  working with zero edits.

### New types
```csharp
public enum ContestRole { All, InArea, OutArea, Dx }
public enum HomeAreaKind { None, StateCounty, WVE }
public enum WorkTarget { Everyone, InAreaOnly, OutAreaOnly }

public class HomeArea {
    public HomeAreaKind Kind { get; set; }        // StateCounty (QSO parties) | WVE (ARRL DX)
    public List<string> States { get; set; } = new(); // in-area S/P codes: ["OH"], or 7QP's 7, NEQP's 6
}

public class RoleRules {
    public List<ContestField>? SentExchange { get; set; }
    public List<ContestField>? RcvdExchange { get; set; }
    public PointsRule? QsoPoints { get; set; }
    public List<MultRule>? MultiplierRules { get; set; }
    public WorkTarget WorksForPoints { get; set; } = WorkTarget.Everyone;
}
```

### `PointsRule` (addition)
- `Dictionary<string,int>? ByMode` — base points per mode class ("CW","PH","RTTY").
  Resolution per QSO: the existing relationship overrides win first
  (`SameCountry` → `SameContinent` → `OtherContinent` → `SameZone`), otherwise the
  base is `ByMode[modeClass] ?? Default`.
- Per-band point *scaling* (CQ WPX low-band doubling) is **out of scope for A** —
  noted for encoding (sub-project C); model can gain a `BandBonus` later without
  breaking anything.

### `MyExchange` (addition)
- `string? County` — the operator's own county (in-area sent exchange).

### `ContestSession` (addition)
- `ContestRole Role` — resolved at start; stored so scoring stays consistent.

## Engine changes (`ContestScoringEngine`)

Pure and I/O-free as today.

- `ContestRole DetermineRole(HomeArea?, MyExchange)` — `All` when `HomeArea` is
  null/None; for `StateCounty`, `InArea` iff `MyExchange.State ∈ HomeArea.States`
  else `OutArea`; for `WVE`, `InArea` iff operator is W/VE else `OutArea`/`Dx`.
- `EffectiveRules(def, role)` — returns the role's rules with top-level fallback.
- `StationClass ClassifyStation(HomeArea?, qso)` — `InArea` iff the worked
  station's received S/P (or county's state) ∈ `HomeArea.States`; W/VE vs DX for
  `WVE`; otherwise `OutArea`/`Dx`.
- Scoring per QSO uses the effective rules:
  - If `WorksForPoints` excludes the station's class ⇒ **0 points, no mults**
    (still logged; still dupe-checked).
  - Points from the effective `PointsRule` (relationship overrides, else `ByMode`/`Default`).
  - Mults from the effective `MultiplierRules`.
- `Recompute(def, myExchange, qsos)` determines the role from
  `myExchange`+`def.HomeArea` (or takes an explicit role) and applies the above.

## Back-compat & migration

- No definition sets `HomeArea`/`Roles` yet ⇒ every contest resolves to `All` and
  uses today's top-level fields ⇒ **identical behaviour, no regressions.**
- Global contests (CQ/WPX/ARRL DX-as-today/etc.) need no change.
- State QSO parties keep their current single-field behaviour until sub-project C
  encodes their `HomeArea` + roles. A does **not** require re-encoding all 97.

## Explicitly deferred
- **Sub-project B** (operating UX): entry-form band/mode enforcement, My-County
  input, auto in/out-of-state role display, valid Cabrillo per role.
- **Sub-project C** (per-contest encoding): fill `HomeArea`/`Roles`/`ByMode`/exact
  bands+modes for each contest from the research files, in verified batches.
- Per-band point scaling (CQ WPX); QTC traffic (WAE); multi-weekend split (CQ VHF).

## Testing

- Unit tests for `DetermineRole`, `ClassifyStation`, `EffectiveRules` fallback.
- Scoring tests: an Ohio-QSO-Party-style def with `HomeArea=["OH"]` +
  InArea/OutArea roles — in-state op scores everyone & counts state/prov mults;
  out-of-state op scores only OH stations & counts county mults; `WorksForPoints`
  zeroes an out-out contact.
- Regression: the existing 715+ contest tests stay green (All-role fallback).
- `ByMode` points test (CW=2/PH=1).
