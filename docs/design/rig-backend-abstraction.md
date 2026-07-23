# Rig backend abstraction + native FlexRadio support

Status: **DESIGN (Stage 0)** — targets v2.8.0
Author: N8SDR + Claude
Date: 2026-07-23

## Motivation

Today there is **no common rig abstraction**. TCI, Hamlib, and flrig are three
independent `BackgroundService`s (`Services/TciRadioService.cs`,
`Services/HamlibService.cs`, `Services/FlrigService.cs`) that share nothing but the
SignalR events they emit. Coordination lives inline in `Hubs/LogHub.cs` as
**hardcoded `TCI → Hamlib → flrig` if/else ladders repeated in ~7 places**
(`SelectSpot`, `TuneToFrequency`, `SetRadioMode`, plus connect / disconnect / status /
discovery). Rig "kind" is represented four different ways (C# `RadioType` enum in
`Contracts/Events/LogEvents.cs`, an entity `RadioType` string, a settings
`ActiveRigType` string, and a TS union in `signalr.ts`), plus `radioId` prefix
conventions (`tci-`, `hamlib-`, `flrig`).

Adding a **native FlexRadio (SmartSDR)** backend the same way would mean pasting a
4th branch into every ladder + a 4th hardcoded UI button — the exact shim-accretion
we want to avoid, and it gets worse with each future backend (native CAT, etc.).

**Decision (2026-07-23):** introduce a proper `IRigBackend` abstraction + registry
FIRST (behavior-preserving), collapse the ladders, then add FlexRadio as a clean
plugin. This matches the standing "rewrite it correctly, don't patch" rule.

## Staged plan

- **S0 — Contract.** This document + the `IRigBackend` interface, capability
  interfaces, and an empty `RigRegistry`. No behavior change.
- **S1 — Port.** TCI / Hamlib / flrig each implement `IRigBackend`. Purely
  mechanical wrapping; **behavior-preserving**; existing tests stay green.
- **S2 — Collapse.** Replace LogHub's ~7 ladders with a single registry walk.
- **S3 — FlexRadio.** New `FlexRadioService : IRigBackend` — FlexLib (or raw
  TCP 4992), UDP-4992 discovery patterned on the TCI discovery loop, slice→VFO
  binding, freq / mode / PTT / split. LAN-first.
- **S4 — Frontend.** Data-drive the rig-type picker (kill the hardcoded 3-button
  grid in `RigConfig.tsx`), add a Flex config/discovery form, extend the
  enum / TS union / storage discriminator.

## The divergences the contract must absorb

| | TCI | Hamlib | flrig |
|---|---|---|---|
| Instances | **many** (manager keyed by `radioId` + per-connection object) | one | one |
| Connect | `ConnectAsync(radioId)` / `ConnectDirectAsync(host,port,name)` | `ConnectAsync(HamlibRigConfig)` | none — self-connects from `settings.Radio.Flrig.Enabled` |
| Tune order | freq → mode | mode → freq | mode → freq |
| CW keying | yes | yes (currently stubbed → false) | no |
| Spots / combo push | yes | no | no |
| Meters / spectrum | yes | no | no |
| Discovery | UDP (port 1024) | no | no |

The **tune-ordering quirk** is documented at `LogHub.cs:499` — TCI must tune
freq-before-mode (it re-derives CWU/CWL and USB/LSB from the *current* dial), while
Hamlib/flrig must set mode-before-freq (they apply a CW pitch offset on mode change).
Repeating this in three sites is the strongest argument for an **atomic tune**.

## Core interface

`radioId`-keyed uniformly. Single-instance backends (Hamlib, flrig) own exactly one
synthetic id (`hamlib-{model}`, `flrig`) and validate/ignore ids that aren't theirs.

```csharp
namespace SDRLoggerPlus.Server.Services.Rig;

public interface IRigBackend
{
    /// The rig kind this backend serves (Tci / Hamlib / Flrig / Flex).
    RadioType Type { get; }

    /// radioId prefix this backend owns ("tci-", "hamlib-", "flrig", "flex-").
    string IdScheme { get; }

    /// True if this backend owns/serves the given radioId.
    bool OwnsRadio(string radioId);

    /// radioIds this backend currently has an open connection to.
    IReadOnlyList<string> ConnectedRadioIds { get; }
    bool IsRadioConnected(string radioId);

    /// Connect a radio the backend already knows how to resolve (saved config,
    /// discovery entry, or — for flrig — flipping its enabled flag).
    Task<bool> ConnectAsync(string radioId, CancellationToken ct = default);
    Task DisconnectAsync(string radioId, CancellationToken ct = default);

    /// Atomic tune: the backend applies ITS OWN correct freq/mode ordering.
    /// mode == null → tune frequency only. Returns false if not applicable.
    Task<bool> TuneAsync(string radioId, long frequencyHz, string? mode, CancellationToken ct = default);

    /// Single-axis setters for when only one changes.
    Task<bool> SetFrequencyAsync(string radioId, long frequencyHz, CancellationToken ct = default);
    Task<bool> SetModeAsync(string radioId, string mode, long frequencyHz = 0, CancellationToken ct = default);

    /// State snapshots for UI hydration + the aggregate broadcasts.
    IEnumerable<RadioStateChangedEvent> GetRadioStates();
    IEnumerable<RadioConnectionStateChangedEvent> GetConnectionStates();
    Task<IEnumerable<RadioDiscoveredEvent>> GetDiscoveredRadiosAsync();
}
```

Backends continue to **broadcast** `OnRadioDiscovered` / `OnRadioConnectionStateChanged`
/ `OnRadioStateChanged` themselves via the existing `IHubContext<LogHub, ILogHubClient>`
extensions — the registry does not intermediate the event path (S1 keeps that wiring
intact, minimizing risk).

## Capability interfaces (queried with `is`)

Non-universal features live behind marker interfaces so nobody stubs what they can't
do. This **removes** the current Hamlib CW stubs that return `false`.

```csharp
public interface ICwKeyer            // TCI, Hamlib, (Flex? later)
{
    Task<bool> SendCwAsync(string radioId, string message, int speedWpm);
    Task<bool> SetCwSpeedAsync(string radioId, int speedWpm);
}

public interface ISpotSink           // TCI only (push spots to a panadapter)
{
    Task BroadcastSpotAsync(string callsign, string mode, long freqHz, uint argb);
    Task ClearAllSpotsAsync();
    Task PushComboContactAsync(string callsign, string? name, string? grid);
}

public interface ISupportsDiscovery  // TCI, Flex
{
    Task StartDiscoveryAsync();
    Task StopDiscoveryAsync();
}

public interface IInstanceSelectable // TCI, Hamlib (RX2 / VFO instance)
{
    Task SelectInstanceAsync(string radioId, int instance);
}
```

LogHub asks e.g. `if (backend is ICwKeyer cw) await cw.SendCwAsync(...)`.

## Registry

```csharp
public interface IRigRegistry
{
    /// Backends in precedence order: TCI, Hamlib, flrig, (Flex).
    IReadOnlyList<IRigBackend> Backends { get; }

    /// Which backend owns a given radioId (targeted ops).
    IRigBackend? ResolveOwner(string radioId);

    /// First backend that has a live radio, in precedence order — the target
    /// for a spot-click / global tune when the UI hasn't pinned a radioId.
    (IRigBackend Backend, string RadioId)? ActiveTuner();

    /// Aggregates across all backends (UI hydration + fan-out).
    IEnumerable<RadioStateChangedEvent> AllRadioStates();
    IEnumerable<RadioConnectionStateChangedEvent> AllConnectionStates();
    Task<IEnumerable<RadioDiscoveredEvent>> AllDiscoveredRadiosAsync();
}
```

Registered in `Program.cs` with the three (later four) backends injected in
precedence order. After S2, LogHub's tune path is:

```csharp
var target = _rigRegistry.ActiveTuner();          // or ResolveOwner(pinnedId)
if (target is var (backend, radioId))
    await backend.TuneAsync(radioId, frequencyHz, evt.Mode);
```

…replacing every TCI→Hamlib→flrig ladder.

## Oddity mappings (honest, not shims)

- **flrig has no Connect.** `ConnectAsync("flrig")` sets `settings.Radio.Flrig.Enabled
  = true` (and disconnect clears it); `ConnectedRadioIds` reflects `IsConnected`. This
  is the real semantics, surfaced through the common shape.
- **Config-carrying connects stay backend-specific.** `HamlibService.ConnectAsync(HamlibRigConfig)`
  and `TciRadioService.ConnectDirectAsync(host,port,name)` are called from the config
  **save** hub methods (`SaveTciConfig`, Hamlib save/connect), NOT from the generic
  tune path. `IRigBackend.ConnectAsync(radioId)` resolves an *already-saved* radio.
- **Instance model.** Backends own their instances. The TCI manager is already
  multi-instance; Hamlib/flrig expose exactly one id. No global "active rig" object —
  the registry's `ActiveTuner()` derives it on demand from precedence + liveness
  (matching today's behavior exactly).

## How FlexRadio slots in (S3 preview)

`FlexRadioService : IRigBackend, ISupportsDiscovery` (and `ICwKeyer` if we add CW):

- **Transport:** FlexLib (.NET) or raw TCP **4992** command/status protocol.
- **Discovery:** VITA-49 datagrams on UDP **4992** → mirror the TCI discovery loop
  structure (bind `UdpClient`, parse, emit `RadioDiscoveredEvent`, stale-prune).
  `IdScheme = "flex-"`, `radioId = "flex-{serial}"`.
- **Slice→VFO binding:** the one Flex-specific decision — bind to the operator's
  active station/slice and follow it; expose a picker when >1 slice. `TuneAsync`
  tunes that slice.
- **Multi-client:** connects to the radio's 4992 API **alongside** SmartSDR/Aether —
  it does not take exclusive ownership. (The "Combo link, but for Flex" story.)
- **Scope v1:** control-only, LAN-only. OUT: SmartLink (remote/OAuth), panadapter
  streaming, DAX audio.

## Behavior-preservation + testing

- S1 wraps existing methods with zero logic changes; the wrapped services keep their
  current tests. Add `IRigBackend`-level tests (a fake backend) for the registry
  precedence + `ActiveTuner` + `ResolveOwner`.
- S2 is the only stage that changes LogHub control flow — verify against the
  documented tune-ordering cases (TCI CWU/CWL sideband on first spot-click; Hamlib/flrig
  no ±700 Hz shift) and the spot-tune precedence (TCI beats Hamlib beats flrig).
- Every stage: backend build 0-err, full backend + FE suites green before commit.

## Files touched (per stage)

- **S0:** new `Services/Rig/IRigBackend.cs`, `Services/Rig/Capabilities.cs`,
  `Services/Rig/IRigRegistry.cs` + `RigRegistry.cs`. `Program.cs` registers the
  registry (backends added in S1).
- **S1:** the three services implement the interface; `Program.cs` DI.
- **S2:** `Hubs/LogHub.cs` (the ~7 ladders → registry).
- **S3:** new `Services/FlexRadioService.cs`; `RadioType` gains `Flex`
  (`Contracts/Events/LogEvents.cs`); storage discriminator / settings as needed.
- **S4:** `RigConfig.tsx` (data-driven picker + Flex form), `signalr.ts`
  (`RadioType` union + any Flex hub methods), stores.
