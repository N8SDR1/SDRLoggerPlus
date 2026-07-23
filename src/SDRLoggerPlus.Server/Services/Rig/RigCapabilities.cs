namespace SDRLoggerPlus.Server.Services.Rig;

// Optional rig-backend capabilities. A backend implements only what it actually
// supports; callers probe with `is` (e.g. `if (backend is ICwKeyer cw) …`) so nothing
// is ever stubbed to return false. See docs/design/rig-backend-abstraction.md.

/// <summary>CW keying over the rig link (TCI, Hamlib).</summary>
public interface ICwKeyer
{
    Task<bool> SendCwAsync(string radioId, string message, int speedWpm);
    Task<bool> SetCwSpeedAsync(string radioId, int speedWpm);
}

/// <summary>
/// Pushing DX spots / combo contacts to the radio's panadapter as click-to-tune
/// markers (TCI — Lyra / Thetis). These are radio-wide, not per-radioId.
/// </summary>
public interface ISpotSink
{
    Task BroadcastSpotAsync(string callsign, string mode, long freqHz, uint argb);
    Task ClearAllSpotsAsync();
    Task PushComboContactAsync(string callsign, string? name, string? grid);
}

/// <summary>Network discovery of radios (TCI UDP broadcast, FlexRadio VITA-49 on 4992).</summary>
public interface ISupportsDiscovery
{
    Task StartDiscoveryAsync();
    Task StopDiscoveryAsync();
}

/// <summary>Selecting a receiver/VFO instance on a multi-slice/multi-RX radio (TCI, Hamlib).</summary>
public interface IInstanceSelectable
{
    Task SelectInstanceAsync(string radioId, int instance);
}
