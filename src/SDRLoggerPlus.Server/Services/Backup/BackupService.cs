using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services.Backup;

/// <summary>
/// Hosted scheduler for auto-backups. Wakes every 60 s; fires when
/// now - lastRun >= interval (anchored to last successful run, persisted in
/// backup-state.json). "on_exit" fires from StopAsync instead, with a 10 s
/// budget so shutdown is never blocked indefinitely.
/// </summary>
public class BackupService : IHostedService, IDisposable
{
    private readonly ISettingsService _settingsService;
    private readonly IAdifService _adifService;
    private readonly string _configDir;
    private readonly string? _liteDbPath;
    private readonly ILogger<BackupService> _logger;
    private readonly ILogger<BackupRunner> _runnerLogger;
    private readonly BackupStateStore _stateStore;
    private readonly SemaphoreSlim _runLock = new(1, 1);
    private PeriodicTimer? _timer;
    private readonly CancellationTokenSource _cts = new();
    private bool _disposed;

    public BackupService(ISettingsService settingsService, IAdifService adifService,
        string configDir, string? liteDbPath, ILoggerFactory loggerFactory)
    {
        _settingsService = settingsService;
        _adifService = adifService;
        _configDir = configDir;
        _liteDbPath = liteDbPath;
        _logger = loggerFactory.CreateLogger<BackupService>();
        _runnerLogger = loggerFactory.CreateLogger<BackupRunner>();
        _stateStore = new BackupStateStore(Path.Combine(configDir, "backup-state.json"));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _timer = new PeriodicTimer(TimeSpan.FromSeconds(60));
        _ = Task.Run(async () =>
        {
            try
            {
                while (await _timer.WaitForNextTickAsync(_cts.Token))
                    await TickAsync();
            }
            catch (OperationCanceledException) { }
        });
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        try
        {
            var backup = (await _settingsService.GetSettingsAsync()).Backup;
            if (backup.Enabled && backup.Interval == "on_exit")
            {
                var run = RunNowAsync("on_exit");
                await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(10), cancellationToken));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "On-exit backup failed");
        }
    }

    /// <summary>One scheduler tick — public for tests.</summary>
    public async Task TickAsync()
    {
        try
        {
            var backup = (await _settingsService.GetSettingsAsync()).Backup;
            if (!backup.Enabled) return;
            var state = _stateStore.Load();
            if (BackupSchedule.IsDue(backup.Interval, state.LastRunUtc, DateTime.UtcNow))
                await RunNowAsync("scheduled");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Backup tick failed");
        }
    }

    public async Task<BackupRunResult> RunNowAsync(string trigger = "manual")
    {
        await _runLock.WaitAsync();
        try
        {
            var backup = (await _settingsService.GetSettingsAsync()).Backup;
            var runner = new BackupRunner(
                dbPath: _liteDbPath,
                adifExport: () => _adifService.ExportQsosAsync(null),
                destinationRoot: ResolveDestination(backup.DestinationPath),
                retention: Math.Max(1, backup.Retention),
                stateStore: _stateStore,
                logger: _runnerLogger);
            return await runner.RunAsync(trigger);
        }
        finally
        {
            _runLock.Release();
        }
    }

    public async Task<BackupStatusDto> GetStatusAsync()
    {
        var backup = (await _settingsService.GetSettingsAsync()).Backup;
        var state = _stateStore.Load();
        return new BackupStatusDto(
            backup.Enabled, backup.Interval, backup.Retention,
            ResolveDestination(backup.DestinationPath),
            state.LastRunUtc, state.Ok, state.Message, state.Path,
            BackupSchedule.NextDue(backup.Interval, state.LastRunUtc));
    }

    private string ResolveDestination(string? configured) =>
        string.IsNullOrWhiteSpace(configured) ? Path.Combine(_configDir, "backups") : configured;

    public void Dispose()
    {
        // Disposed twice on graceful shutdown: once as a hosted service, once
        // as a container singleton (same instance, see Program.cs).
        if (_disposed) return;
        _disposed = true;
        _cts.Cancel();
        _timer?.Dispose();
        _cts.Dispose();
    }
}
