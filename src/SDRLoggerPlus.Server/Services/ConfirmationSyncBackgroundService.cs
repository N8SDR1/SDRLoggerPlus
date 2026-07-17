using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Hands-off confirmation sync (Log4OM style): periodically downloads LoTW /
/// eQSL confirmations and merges them into the log, gated by
/// ConfirmationSyncSettings. Ticks every 15 min and runs when the configured
/// interval has elapsed (optionally once shortly after startup).
/// </summary>
public class ConfirmationSyncBackgroundService : BackgroundService
{
    private static readonly TimeSpan TickInterval = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(30);

    private readonly IServiceProvider _services;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;
    private readonly ILogger<ConfirmationSyncBackgroundService> _logger;
    private DateTime? _lastRun;

    public ConfirmationSyncBackgroundService(
        IServiceProvider services,
        IHubContext<LogHub, ILogHubClient> hub,
        ILogger<ConfirmationSyncBackgroundService> logger)
    {
        _services = services;
        _hub = hub;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try { await Task.Delay(StartupDelay, stoppingToken); }
        catch (OperationCanceledException) { return; }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _services.CreateScope();
                var settings = await scope.ServiceProvider.GetRequiredService<ISettingsService>().GetSettingsAsync();
                var cfg = settings.ConfirmationSync;

                if (cfg.AutoSync)
                {
                    var interval = TimeSpan.FromHours(Math.Max(1, cfg.IntervalHours));
                    bool run;
                    if (_lastRun is null)
                    {
                        run = cfg.SyncOnStartup;
                        if (!run) _lastRun = DateTime.UtcNow; // start the clock without running now
                    }
                    else
                    {
                        run = DateTime.UtcNow - _lastRun.Value >= interval;
                    }

                    if (run)
                    {
                        await RunSyncAsync(scope.ServiceProvider, settings, stoppingToken);
                        _lastRun = DateTime.UtcNow;
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Confirmation auto-sync tick failed");
            }

            try { await Task.Delay(TickInterval, stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task RunSyncAsync(IServiceProvider sp, UserSettings settings, CancellationToken ct)
    {
        var sync = sp.GetRequiredService<IConfirmationSyncService>();
        var cfg = settings.ConfirmationSync;

        if (cfg.Lotw &&
            !string.IsNullOrWhiteSpace(settings.Lotw.Username) &&
            !string.IsNullOrWhiteSpace(settings.Lotw.Password))
        {
            await RunOneAsync("LoTW", () => sync.SyncLotwAsync(ct));
        }

        if (cfg.Eqsl &&
            !string.IsNullOrWhiteSpace(settings.Eqsl.Username) &&
            !string.IsNullOrWhiteSpace(settings.Eqsl.Password))
        {
            await RunOneAsync("eQSL", () => sync.SyncEqslAsync(ct));
        }

        if (cfg.Qrz && !string.IsNullOrWhiteSpace(settings.Qrz.ApiKey))
        {
            await RunOneAsync("QRZ", () => sync.SyncQrzAsync(ct));
        }
    }

    private async Task RunOneAsync(string source, Func<Task<ConfirmationMergeResponse>> run)
    {
        try
        {
            var r = await run();
            _logger.LogInformation("Auto-sync {Source}: {Updated} newly confirmed, {Unmatched} unmatched",
                source, r.Updated, r.Unmatched);
            // Only nudge the UI when something actually changed.
            if (r.Updated > 0)
                await _hub.Clients.All.OnConfirmationSyncCompleted(
                    new ConfirmationSyncCompletedEvent(source, r.Matched, r.Updated, r.Unmatched));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Auto-sync {Source} failed", source);
            await _hub.Clients.All.OnConfirmationSyncCompleted(
                new ConfirmationSyncCompletedEvent(source, 0, 0, 0, ex.Message));
        }
    }
}
