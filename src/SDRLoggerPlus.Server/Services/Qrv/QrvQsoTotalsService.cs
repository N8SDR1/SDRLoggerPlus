using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// Recounts every non-contest contact already in the log and drives <see cref="QrvQsoTotalsClient"/>.
/// The presence install key is reused. Totals stay quiet while the switch is off.
/// </summary>
public sealed class QrvQsoTotalsService : BackgroundService
{
    private readonly QrvQsoTotalsClient _client;
    private readonly QrvPresenceClient _presence;
    private readonly QrvQsoTotalsSignal _signal;
    private readonly IServiceScopeFactory _scopes;
    private readonly ILogger<QrvQsoTotalsService> _logger;
    private DateTime _lastCountUtc = DateTime.MinValue;
    private bool _wasEnabled;

    public QrvQsoTotalsService(
        QrvQsoTotalsClient client,
        QrvPresenceClient presence,
        QrvQsoTotalsSignal signal,
        IServiceScopeFactory scopes,
        ILogger<QrvQsoTotalsService> logger)
    {
        _client = client;
        _presence = presence;
        _signal = signal;
        _scopes = scopes;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SampleAsync(stoppingToken);
                await _client.TickAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "QRV QSO totals tick failed");
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
            using var flush = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            try
            {
                await _client.ShutdownAsync(flush.Token);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "QRV QSO totals shutdown post failed");
            }
        }
    }

    private async Task SampleAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopes.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var settings = await settingsService.GetSettingsAsync();
        var qrv = settings.Qrv;
        var creds = _presence.Credentials();
        var enabled = qrv?.QsoTotalsEnabled == true;
        var key = creds.InstallKey ?? qrv?.InstallKey;
        var call = !string.IsNullOrEmpty(creds.Call) ? creds.Call : settings.Station?.Callsign;
        var baseUrl = string.IsNullOrWhiteSpace(qrv?.BaseUrl) ? creds.ApiBase : qrv!.BaseUrl;
        _client.ApplySettings(enabled, baseUrl, call, key);

        var logged = _signal.Consume();
        var now = DateTime.UtcNow;
        var due = !_wasEnabled && enabled
            || logged
            || (enabled && now - _lastCountUtc >= TimeSpan.FromSeconds(60));
        _wasEnabled = enabled;
        if (enabled && due)
        {
            var repository = scope.ServiceProvider.GetRequiredService<IQsoRepository>();
            var qsos = await repository.GetAllAsync();
            _client.SetCounts(QrvQsoTotals.Count(qsos, now));
            _lastCountUtc = now;
        }

        cancellationToken.ThrowIfCancellationRequested();
    }
}
