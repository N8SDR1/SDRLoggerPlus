using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services.Rig;

namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// Drives <see cref="QrvPresenceClient"/> from station settings and the connected radio.
/// The log Mode select is reported separately by the UI; the rig mode is the fallback.
/// </summary>
public sealed class QrvPresenceService : BackgroundService
{
    private readonly QrvPresenceClient _client;
    private readonly IRigRegistry _rigs;
    private readonly IServiceScopeFactory _scopes;
    private readonly ILogger<QrvPresenceService> _logger;

    public QrvPresenceService(
        QrvPresenceClient client,
        IRigRegistry rigs,
        IServiceScopeFactory scopes,
        ILogger<QrvPresenceService> logger)
    {
        _client = client;
        _rigs = rigs;
        _scopes = scopes;
        _logger = logger;
    }

    public QrvPresenceClient Client => _client;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SampleAsync(stoppingToken);
                await _client.TickAsync(stoppingToken);
                await PersistKeyAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "QRV presence tick failed");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await base.StopAsync(cancellationToken);
        }
        finally
        {
            using var qrt = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            try
            {
                await _client.ShutdownAsync(qrt.Token);
                await PersistKeyAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "QRV QRT on shutdown failed");
            }
        }
    }

    private async Task SampleAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopes.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var settings = await settingsService.GetSettingsAsync();
        var qrv = settings.Qrv ?? new QrvSettings();
        _client.ApplySettings(qrv.Enabled, qrv.BaseUrl, settings.Station?.Callsign, qrv.InstallKey, qrv.BoundCall);

        var states = _rigs.AllRadioStates().ToList();
        var preferred = _client.PreferredRadioId;
        RadioStateChangedEvent? state = null;
        if (!string.IsNullOrEmpty(preferred))
            state = states.FirstOrDefault(s => s.RadioId == preferred && s.FrequencyHz > 0);
        if (state == null)
        {
            var active = _rigs.ActiveTuner();
            if (active != null)
                state = states.FirstOrDefault(s => s.RadioId == active.Value.RadioId);
        }

        if (state == null)
            _client.SetRadio(false, 0, null);
        else
            _client.SetRadio(true, state.FrequencyHz, state.Mode);

        cancellationToken.ThrowIfCancellationRequested();
    }

    private async Task PersistKeyAsync(CancellationToken cancellationToken)
    {
        var pending = _client.PeekKeyUpdate();
        if (pending == null) return;

        using var scope = _scopes.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var settings = await settingsService.GetSettingsAsync();
        settings.Qrv ??= new QrvSettings();
        settings.Qrv.InstallKey = pending.InstallKey;
        settings.Qrv.BoundCall = pending.BoundCall;
        await settingsService.SaveSettingsAsync(settings);
        _client.AcknowledgeKeyUpdate(pending);
        cancellationToken.ThrowIfCancellationRequested();
    }
}
