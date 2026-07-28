using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Keeps a continuously-current ADIF copy of the log on a configured drive (S6 — the pull-and-go
/// evacuation copy for multi-op failover, and general storm insurance). Every ~30 s, if the log has
/// grown since the last write, it re-exports the full log as ADIF to
/// <c>&lt;LiveMirrorPath&gt;/sdrloggerplus-live.adi</c>, written atomically (temp file + move) so a
/// drive yanked mid-write never leaves a truncated file. Off unless a path is set.
///
/// Full ADIF (not a raw DB copy) on purpose: it's the universal format — import it into a new host, a
/// different logger, LoTW, anything. Failover = pull the drive from the dead host, import this file.
/// </summary>
public sealed class LiveMirrorService : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(30);
    private const string FileName = "sdrloggerplus-live.adi";

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<LiveMirrorService> _log;
    private int _lastCount = -1;

    public LiveMirrorService(IServiceScopeFactory scopeFactory, ILogger<LiveMirrorService> log)
    {
        _scopeFactory = scopeFactory;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await MirrorOnce(stoppingToken); }
            catch (Exception ex) { _log.LogDebug("Live mirror cycle failed: {Msg}", ex.Message); }
            try { await Task.Delay(Interval, stoppingToken); } catch { }
        }
    }

    private async Task MirrorOnce(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var sp = scope.ServiceProvider;

        var settings = await sp.GetRequiredService<ISettingsService>().GetSettingsAsync();
        var dir = settings.Backup.LiveMirrorPath;
        if (string.IsNullOrWhiteSpace(dir)) return;

        // Skip when nothing new since the last mirror (the common quiet second). New QSOs are the
        // dominant change; a rare in-place edit rides out on the next count change or restart.
        var count = await sp.GetRequiredService<IQsoRepository>().GetCountAsync();
        if (count == _lastCount) return;

        var content = await sp.GetRequiredService<IAdifService>().ExportQsosAsync();

        Directory.CreateDirectory(dir);
        var file = Path.Combine(dir, FileName);
        var tmp = file + ".tmp";
        await File.WriteAllTextAsync(tmp, content, ct);
        File.Move(tmp, file, overwrite: true); // atomic on the same volume — the .adi is never half-written

        _lastCount = count;
        _log.LogInformation("Live mirror: {Count} QSOs → {File}.", count, file);
    }
}
