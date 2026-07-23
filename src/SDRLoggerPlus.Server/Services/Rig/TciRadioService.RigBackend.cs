using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services;

// Stage 1 of the rig abstraction: expose TciRadioService through IRigBackend +
// capability interfaces. Members are EXPLICIT so they add the adapter surface
// without disturbing the existing public overloads (e.g. ConnectAsync(radioId))
// that LogHub still calls directly until Stage 2. Pure delegation — no behavior
// change. See docs/design/rig-backend-abstraction.md.
public partial class TciRadioService
    : Rig.IRigBackend, Rig.ICwKeyer, Rig.ISpotSink, Rig.ISupportsDiscovery, Rig.IInstanceSelectable
{
    RadioType Rig.IRigBackend.Type => RadioType.Tci;
    string Rig.IRigBackend.IdScheme => "tci-";

    bool Rig.IRigBackend.OwnsRadio(string radioId)
        => HasRadio(radioId) || radioId.StartsWith("tci-", StringComparison.Ordinal);

    IReadOnlyList<string> Rig.IRigBackend.ConnectedRadioIds
        => GetConnectionStates()
            .Where(c => c.State == RadioConnectionState.Connected)
            .Select(c => c.RadioId)
            .ToList();

    bool Rig.IRigBackend.IsRadioConnected(string radioId)
        => GetConnectionStates().Any(c => c.RadioId == radioId && c.State == RadioConnectionState.Connected);

    async Task<bool> Rig.IRigBackend.ConnectAsync(string radioId, CancellationToken ct)
    {
        await ConnectAsync(radioId);
        return ((Rig.IRigBackend)this).IsRadioConnected(radioId);
    }

    Task Rig.IRigBackend.DisconnectAsync(string radioId, CancellationToken ct) => DisconnectAsync(radioId);

    // TCI order: frequency BEFORE mode — the receiver re-derives CWU/CWL and USB/LSB
    // from the current dial, so mode must be applied after the new frequency.
    async Task<bool> Rig.IRigBackend.TuneAsync(string radioId, long frequencyHz, string? mode, CancellationToken ct)
    {
        var ok = await SetFrequencyAsync(radioId, frequencyHz);
        if (!string.IsNullOrEmpty(mode))
            await SetModeAsync(radioId, mode, frequencyHz);
        return ok;
    }

    Task<bool> Rig.IRigBackend.SetFrequencyAsync(string radioId, long frequencyHz, CancellationToken ct)
        => SetFrequencyAsync(radioId, frequencyHz);

    Task<bool> Rig.IRigBackend.SetModeAsync(string radioId, string mode, long frequencyHz, CancellationToken ct)
        => SetModeAsync(radioId, mode, frequencyHz);

    IEnumerable<RadioStateChangedEvent> Rig.IRigBackend.GetRadioStates() => GetRadioStates();
    IEnumerable<RadioConnectionStateChangedEvent> Rig.IRigBackend.GetConnectionStates() => GetConnectionStates();
    Task<IEnumerable<RadioDiscoveredEvent>> Rig.IRigBackend.GetDiscoveredRadiosAsync() => GetDiscoveredRadiosAsync();

    // -- capabilities -------------------------------------------------------
    Task<bool> Rig.ICwKeyer.SendCwAsync(string radioId, string message, int speedWpm) => SendCwAsync(radioId, message, speedWpm);
    Task<bool> Rig.ICwKeyer.SetCwSpeedAsync(string radioId, int speedWpm) => SetCwSpeedAsync(radioId, speedWpm);

    Task Rig.ISpotSink.BroadcastSpotAsync(string callsign, string mode, long freqHz, uint argb) => BroadcastSpotAsync(callsign, mode, freqHz, argb);
    Task Rig.ISpotSink.ClearAllSpotsAsync() => ClearAllSpotsAsync();
    Task Rig.ISpotSink.PushComboContactAsync(string callsign, string? name, string? grid) => PushComboContactAsync(callsign, name, grid);

    Task Rig.ISupportsDiscovery.StartDiscoveryAsync() => StartDiscoveryAsync();
    Task Rig.ISupportsDiscovery.StopDiscoveryAsync() => StopDiscoveryAsync();

    Task Rig.IInstanceSelectable.SelectInstanceAsync(string radioId, int instance) => SelectInstanceAsync(radioId, instance);
}
