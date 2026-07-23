using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services;

// Stage 1 of the rig abstraction: expose FlrigService through IRigBackend.
// flrig has no explicit connect — it self-connects from settings.Radio.Flrig.Enabled
// via its poll loop — so ConnectAsync/DisconnectAsync flip that flag (the honest
// mapping). No behavior change to the poll path. See docs/design/rig-backend-abstraction.md.
public partial class FlrigService : Rig.IRigBackend
{
    RadioType Rig.IRigBackend.Type => RadioType.Flrig;
    string Rig.IRigBackend.IdScheme => FlrigRadioId;

    bool Rig.IRigBackend.OwnsRadio(string radioId) => radioId == FlrigRadioId;

    IReadOnlyList<string> Rig.IRigBackend.ConnectedRadioIds
        => IsConnected ? new[] { FlrigRadioId } : Array.Empty<string>();

    bool Rig.IRigBackend.IsRadioConnected(string radioId) => IsConnected && radioId == FlrigRadioId;

    async Task<bool> Rig.IRigBackend.ConnectAsync(string radioId, CancellationToken ct)
    {
        var settings = await _settingsRepository.GetAsync();
        if (settings is null) return false;
        settings.Radio.Flrig.Enabled = true;
        await _settingsRepository.UpsertAsync(settings);
        return IsConnected; // the poll loop establishes the actual connection
    }

    async Task Rig.IRigBackend.DisconnectAsync(string radioId, CancellationToken ct)
    {
        var settings = await _settingsRepository.GetAsync();
        if (settings is null) return;
        settings.Radio.Flrig.Enabled = false;
        await _settingsRepository.UpsertAsync(settings);
    }

    // flrig order: mode BEFORE frequency (avoids the CW/SSB pitch-offset shift). flrig
    // translates mode names internally, so the dial (frequencyHz) isn't needed for mode.
    async Task<bool> Rig.IRigBackend.TuneAsync(string radioId, long frequencyHz, string? mode, CancellationToken ct)
    {
        if (!string.IsNullOrEmpty(mode))
            await SetModeAsync(mode, ct);
        return await SetFrequencyAsync(frequencyHz, ct);
    }

    Task<bool> Rig.IRigBackend.SetFrequencyAsync(string radioId, long frequencyHz, CancellationToken ct)
        => SetFrequencyAsync(frequencyHz, ct);

    Task<bool> Rig.IRigBackend.SetModeAsync(string radioId, string mode, long frequencyHz, CancellationToken ct)
        => SetModeAsync(mode, ct);

    IEnumerable<RadioStateChangedEvent> Rig.IRigBackend.GetRadioStates()
        => IsConnected
            ? new[]
            {
                new RadioStateChangedEvent(
                    RadioId: FlrigRadioId,
                    FrequencyHz: LastFrequencyHz,
                    Mode: LastMode,
                    IsTransmitting: false,
                    Band: BandFromHz(LastFrequencyHz),
                    SliceOrInstance: null),
            }
            : Array.Empty<RadioStateChangedEvent>();

    IEnumerable<RadioConnectionStateChangedEvent> Rig.IRigBackend.GetConnectionStates()
        => IsConnected
            ? new[] { new RadioConnectionStateChangedEvent(FlrigRadioId, RadioConnectionState.Connected) }
            : Array.Empty<RadioConnectionStateChangedEvent>();

    async Task<IEnumerable<RadioDiscoveredEvent>> Rig.IRigBackend.GetDiscoveredRadiosAsync()
    {
        if (!IsConnected) return Array.Empty<RadioDiscoveredEvent>();
        var settings = await _settingsRepository.GetAsync();
        var cfg = settings?.Radio.Flrig;
        return new[]
        {
            new RadioDiscoveredEvent(
                Id: FlrigRadioId,
                Type: RadioType.Flrig,
                Model: string.IsNullOrEmpty(_rigModel) ? "flrig" : _rigModel,
                IpAddress: cfg?.Host ?? "127.0.0.1",
                Port: cfg?.Port ?? 12345,
                Nickname: null),
        };
    }
}
