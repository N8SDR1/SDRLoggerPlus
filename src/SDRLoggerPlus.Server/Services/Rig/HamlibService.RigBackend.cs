using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services;

// Stage 1 of the rig abstraction: expose HamlibService through IRigBackend.
// Explicit members delegate to the existing surface; no behavior change.
// Hamlib deliberately does NOT implement ICwKeyer — its CW methods are stubs that
// return false, so advertising the capability would be dishonest; the stubs stay
// until a later cleanup. See docs/design/rig-backend-abstraction.md.
public partial class HamlibService : Rig.IRigBackend
{
    RadioType Rig.IRigBackend.Type => RadioType.Hamlib;
    string Rig.IRigBackend.IdScheme => "hamlib-";

    bool Rig.IRigBackend.OwnsRadio(string radioId) => radioId.StartsWith("hamlib-", StringComparison.Ordinal);

    IReadOnlyList<string> Rig.IRigBackend.ConnectedRadioIds
        => IsConnected && RadioId is not null ? new[] { RadioId } : Array.Empty<string>();

    bool Rig.IRigBackend.IsRadioConnected(string radioId) => IsConnected && radioId == RadioId;

    // The single saved Hamlib config resolves the connection; the radioId identifies
    // which backend, not which of several rigs.
    async Task<bool> Rig.IRigBackend.ConnectAsync(string radioId, CancellationToken ct)
    {
        var config = await LoadConfigAsync();
        if (config is null) return false;
        await ConnectAsync(config);
        return IsConnected;
    }

    Task Rig.IRigBackend.DisconnectAsync(string radioId, CancellationToken ct) => DisconnectAsync();

    // Hamlib order: mode BEFORE frequency — setting mode applies a CW pitch offset
    // that would shift the dial if done after tuning.
    async Task<bool> Rig.IRigBackend.TuneAsync(string radioId, long frequencyHz, string? mode, CancellationToken ct)
    {
        if (!string.IsNullOrEmpty(mode))
            await SetModeAsync(mode, frequencyHz);
        return await SetFrequencyAsync(frequencyHz);
    }

    Task<bool> Rig.IRigBackend.SetFrequencyAsync(string radioId, long frequencyHz, CancellationToken ct)
        => SetFrequencyAsync(frequencyHz);

    Task<bool> Rig.IRigBackend.SetModeAsync(string radioId, string mode, long frequencyHz, CancellationToken ct)
        => SetModeAsync(mode, frequencyHz);

    IEnumerable<RadioStateChangedEvent> Rig.IRigBackend.GetRadioStates() => GetRadioStates();

    IEnumerable<RadioConnectionStateChangedEvent> Rig.IRigBackend.GetConnectionStates()
        => IsConnected && RadioId is not null
            ? new[] { new RadioConnectionStateChangedEvent(RadioId, RadioConnectionState.Connected) }
            : Array.Empty<RadioConnectionStateChangedEvent>();

    Task<IEnumerable<RadioDiscoveredEvent>> Rig.IRigBackend.GetDiscoveredRadiosAsync() => GetDiscoveredRadiosAsync();
}
