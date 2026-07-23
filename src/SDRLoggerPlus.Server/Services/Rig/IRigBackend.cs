using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services.Rig;

/// <summary>
/// A radio connection backend (TCI, Hamlib, flrig, FlexRadio, …). Backends are
/// uniformly <c>radioId</c>-keyed: a multi-instance backend (TCI) serves many ids,
/// a single-instance one (Hamlib, flrig) owns exactly one synthetic id
/// (<c>"hamlib-{model}"</c>, <c>"flrig"</c>) and rejects ids that aren't its own.
///
/// Backends broadcast <c>OnRadioDiscovered</c> / <c>OnRadioConnectionStateChanged</c> /
/// <c>OnRadioStateChanged</c> to clients themselves via the LogHub hub-context
/// extensions; the <see cref="IRigRegistry"/> does not intermediate that event path.
///
/// Non-universal features (CW keying, spot push, discovery, instance selection) live
/// behind the capability interfaces in this namespace and are discovered with
/// <c>is</c>, so a backend never has to stub what it cannot do.
///
/// See docs/design/rig-backend-abstraction.md.
/// </summary>
public interface IRigBackend
{
    /// <summary>The rig kind this backend serves.</summary>
    RadioType Type { get; }

    /// <summary>
    /// The <c>radioId</c> prefix this backend owns, e.g. <c>"tci-"</c>, <c>"hamlib-"</c>,
    /// <c>"flrig"</c>, <c>"flex-"</c>. Used by the registry to route by id.
    /// </summary>
    string IdScheme { get; }

    /// <summary>True if this backend owns/serves the given <paramref name="radioId"/>.</summary>
    bool OwnsRadio(string radioId);

    /// <summary>radioIds this backend currently has an open connection to.</summary>
    IReadOnlyList<string> ConnectedRadioIds { get; }

    /// <summary>True if the given radio is currently connected on this backend.</summary>
    bool IsRadioConnected(string radioId);

    /// <summary>
    /// Connect a radio the backend already knows how to resolve — a saved config, a
    /// discovery entry, or (flrig) flipping its enabled flag. Config-carrying connects
    /// that need host/port/model up front stay backend-specific and are called from the
    /// config-save paths, not here.
    /// </summary>
    Task<bool> ConnectAsync(string radioId, CancellationToken ct = default);

    /// <summary>Disconnect the given radio (flrig: clears its enabled flag).</summary>
    Task DisconnectAsync(string radioId, CancellationToken ct = default);

    /// <summary>
    /// Atomically tune frequency and (optionally) mode. The backend applies its OWN
    /// correct freq/mode ordering — TCI tunes freq-before-mode (re-derives CWU/CWL and
    /// USB/LSB from the current dial), Hamlib/flrig tune mode-before-freq (avoid the CW
    /// pitch-offset shift). <paramref name="mode"/> null ⇒ frequency only. Returns false
    /// if the radio isn't connected/applicable.
    /// </summary>
    Task<bool> TuneAsync(string radioId, long frequencyHz, string? mode, CancellationToken ct = default);

    /// <summary>Set frequency only.</summary>
    Task<bool> SetFrequencyAsync(string radioId, long frequencyHz, CancellationToken ct = default);

    /// <summary>Set mode only. <paramref name="frequencyHz"/> (when &gt; 0) lets the backend
    /// resolve a sideband from the dial.</summary>
    Task<bool> SetModeAsync(string radioId, string mode, long frequencyHz = 0, CancellationToken ct = default);

    /// <summary>Current per-radio state snapshots (freq/mode/TX/band/filter) for UI hydration.</summary>
    IEnumerable<RadioStateChangedEvent> GetRadioStates();

    /// <summary>Current per-radio connection-state snapshots.</summary>
    IEnumerable<RadioConnectionStateChangedEvent> GetConnectionStates();

    /// <summary>Radios this backend has discovered/knows about (configured or found on the network).</summary>
    Task<IEnumerable<RadioDiscoveredEvent>> GetDiscoveredRadiosAsync();
}
