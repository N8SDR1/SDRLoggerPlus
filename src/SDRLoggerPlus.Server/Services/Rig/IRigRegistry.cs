using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services.Rig;

/// <summary>
/// The single place the hub asks about rigs, replacing the per-action
/// <c>TCI → Hamlib → flrig</c> if/else ladders. Backends are held in a fixed
/// precedence order (TCI, Hamlib, flrig, FlexRadio).
/// See docs/design/rig-backend-abstraction.md.
/// </summary>
public interface IRigRegistry
{
    /// <summary>All backends, in precedence order.</summary>
    IReadOnlyList<IRigBackend> Backends { get; }

    /// <summary>The backend that owns a given radioId, or null.</summary>
    IRigBackend? ResolveOwner(string radioId);

    /// <summary>
    /// The first backend (in precedence order) that has a live radio, paired with that
    /// radio's id — the target for a spot-click / global tune when the UI hasn't pinned a
    /// specific radioId. Null when nothing is connected.
    /// </summary>
    RigTarget? ActiveTuner();

    /// <summary>Per-radio state snapshots across every backend (UI hydration + fan-out).</summary>
    IEnumerable<RadioStateChangedEvent> AllRadioStates();

    /// <summary>Per-radio connection-state snapshots across every backend.</summary>
    IEnumerable<RadioConnectionStateChangedEvent> AllConnectionStates();

    /// <summary>Discovered/known radios across every backend.</summary>
    Task<IEnumerable<RadioDiscoveredEvent>> AllDiscoveredRadiosAsync();
}

/// <summary>A resolved (backend, radioId) pair to act on.</summary>
public readonly record struct RigTarget(IRigBackend Backend, string RadioId);
