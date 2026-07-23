using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Persists per-file byte offsets for the ADIF monitor (adif-monitor-state.json
/// in the config dir), so already-imported QSOs are not re-read across restarts.
/// </summary>
public class AdifMonitorStateStore
{
    private readonly string _path;
    private Dictionary<string, long> _offsets;

    public AdifMonitorStateStore(string path)
    {
        _path = path;
        try
        {
            _offsets = File.Exists(path)
                ? JsonSerializer.Deserialize<Dictionary<string, long>>(File.ReadAllText(path)) ?? new()
                : new();
        }
        catch
        {
            _offsets = new();
        }
    }

    public long GetOffset(string file) => _offsets.TryGetValue(file, out var o) ? o : 0;
    public void SetOffset(string file, long offset) => _offsets[file] = offset;

    public void Save()
    {
        try
        {
            File.WriteAllText(_path, JsonSerializer.Serialize(_offsets));
        }
        catch
        {
            // best effort — worst case some QSOs re-import as duplicates and are skipped
        }
    }
}

/// <summary>
/// ADIF File Monitor (SDRLogger+ port): watches up to two external ADIF files
/// (VarAC, MSHV, …) and auto-imports newly appended QSOs. Byte-offset tracking
/// means only appended bytes are ever read; a shrunken file (rotation/replace)
/// restarts from zero. Imports go through the normal ADIF import path with
/// duplicate skipping, and each import broadcasts a SignalR event for a toast.
/// </summary>
public class AdifMonitorService : BackgroundService
{
    private readonly ILogger<AdifMonitorService> _logger;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;
    private readonly AdifMonitorStateStore _state;

    public AdifMonitorService(ILogger<AdifMonitorService> logger, IServiceScopeFactory scopeFactory,
        IHubContext<LogHub, ILogHubClient> hub, string configDir)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _hub = hub;
        _state = new AdifMonitorStateStore(Path.Combine(configDir, "adif-monitor-state.json"));
    }

    /// <summary>
    /// Reads bytes appended since lastOffset. Returns null when the file is
    /// missing or unchanged; restarts from zero when the file shrank.
    /// </summary>
    public static (string Fragment, long NewOffset)? ReadAppended(string path, long lastOffset)
    {
        try
        {
            var info = new FileInfo(path);
            if (!info.Exists) return null;

            var start = info.Length < lastOffset ? 0 : lastOffset;
            if (info.Length == start) return null;

            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            fs.Seek(start, SeekOrigin.Begin);
            using var reader = new StreamReader(fs, Encoding.UTF8);
            var fragment = reader.ReadToEnd();
            return (fragment, info.Length);
        }
        catch (IOException)
        {
            return null; // writer holds an exclusive lock — retry next tick
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ADIF monitor starting");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await TickAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "ADIF monitor tick failed");
            }
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var settings = (await scope.ServiceProvider.GetRequiredService<ISettingsService>().GetSettingsAsync()).AdifMonitor;
        if (!settings.Enabled) return;

        foreach (var file in new[] { settings.File1, settings.File2 })
        {
            if (string.IsNullOrWhiteSpace(file)) continue;

            var appended = ReadAppended(file, _state.GetOffset(file));
            if (appended is not { } result) continue;

            // Only import fragments that contain at least one complete record;
            // a partially written record stays pending until its <EOR> arrives.
            if (!result.Fragment.Contains("<eor>", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var adifService = scope.ServiceProvider.GetRequiredService<IAdifService>();
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(result.Fragment));
            // Monitored files are the LIVE output of logging apps (JTDX / WSJT-X / MSHV /
            // VarAC …) — i.e. NEW QSOs, not a QRZ export. Import them as NOT-synced so the
            // QRZ uploader still picks them up. (Marking-as-synced is only correct when
            // importing your existing QRZ log to avoid re-uploading it.)
            var import = await adifService.ImportAdifAsync(stream, skipDuplicates: true, markAsSyncedToQrz: false, cancellationToken: ct);

            _state.SetOffset(file, result.NewOffset);
            _state.Save();

            if (import.ImportedCount > 0 || import.SkippedDuplicates > 0)
            {
                _logger.LogInformation("ADIF monitor: {File} → imported {Imported}, skipped {Skipped}",
                    file, import.ImportedCount, import.SkippedDuplicates);
                await _hub.Clients.All.OnAdifMonitorImport(new AdifMonitorImportEvent(
                    Path.GetFileName(file), import.ImportedCount, import.SkippedDuplicates));
            }
        }
    }
}
