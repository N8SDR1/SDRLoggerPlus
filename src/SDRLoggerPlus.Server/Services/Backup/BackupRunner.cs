using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services.Backup;

/// <summary>
/// Executes one backup pass: timestamped folder + raw DB copy (LiteDB only)
/// + ADIF export, then retention prune. Prune runs ONLY after a successful
/// write, so a failed run never destroys prior backups.
/// </summary>
public class BackupRunner
{
    public const string FolderPrefix = "SDRLoggerPlus-";

    private readonly string? _dbPath;            // null → DB path unavailable (ADIF-only backup)
    private readonly Func<Task<string>> _adifExport;
    private readonly string _destinationRoot;
    private readonly int _retention;
    private readonly BackupStateStore _stateStore;
    private readonly ILogger<BackupRunner> _logger;

    public BackupRunner(string? dbPath, Func<Task<string>> adifExport, string destinationRoot,
        int retention, BackupStateStore stateStore, ILogger<BackupRunner> logger)
    {
        _dbPath = dbPath;
        _adifExport = adifExport;
        _destinationRoot = destinationRoot;
        _retention = retention;
        _stateStore = stateStore;
        _logger = logger;
    }

    public async Task<BackupRunResult> RunAsync(string trigger)
    {
        var now = DateTime.UtcNow;
        var folder = Path.Combine(_destinationRoot, $"{FolderPrefix}{now:yyyy-MM-dd_HHmm}");
        try
        {
            Directory.CreateDirectory(folder);
        }
        catch (Exception ex)
        {
            return Fail(now, folder, $"Cannot create backup folder: {ex.Message}", trigger);
        }

        var written = new List<string>();
        var failures = new List<string>();

        if (_dbPath != null)
        {
            try
            {
                if (File.Exists(_dbPath))
                {
                    File.Copy(_dbPath, Path.Combine(folder, Path.GetFileName(_dbPath)), overwrite: true);
                    written.Add(Path.GetFileName(_dbPath));
                }
                else
                {
                    failures.Add($"db: file not found ({_dbPath})");
                }
            }
            catch (Exception ex)
            {
                failures.Add($"db: {ex.Message}");
            }
        }

        try
        {
            var adif = await _adifExport();
            if (!string.IsNullOrEmpty(adif))
            {
                await File.WriteAllTextAsync(Path.Combine(folder, "sdrloggerplus.adi"), adif);
                written.Add("sdrloggerplus.adi");
            }
        }
        catch (Exception ex)
        {
            failures.Add($"adif: {ex.Message}");
        }

        if (written.Count == 0)
        {
            var msg = "No files written. " + string.Join(" ; ", failures);
            return Fail(now, folder, msg, trigger);
        }

        var pruned = Prune();

        var okMsg = $"Wrote {written.Count} file(s): {string.Join(", ", written)}";
        if (_dbPath == null) okMsg += " (ADIF only — no DB file)";
        if (failures.Count > 0) okMsg += $" (partial — {string.Join(" ; ", failures)})";
        if (pruned > 0) okMsg += $" [pruned {pruned} old folder(s)]";

        _stateStore.Save(new BackupState { LastRunUtc = now, Ok = true, Message = okMsg, Path = folder });
        _logger.LogInformation("Auto-backup ({Trigger}) OK → {Folder}", trigger, folder);
        return new BackupRunResult(true, okMsg, folder);
    }

    private BackupRunResult Fail(DateTime now, string folder, string message, string trigger)
    {
        _stateStore.Save(new BackupState { LastRunUtc = now, Ok = false, Message = message, Path = folder });
        _logger.LogWarning("Auto-backup ({Trigger}) FAILED: {Message}", trigger, message);
        return new BackupRunResult(false, message, folder);
    }

    /// <summary>Keep newest N SDRLoggerPlus-* folders; never touch anything else.</summary>
    private int Prune()
    {
        var pruned = 0;
        try
        {
            if (_retention < 1 || !Directory.Exists(_destinationRoot)) return 0;
            var siblings = Directory.GetDirectories(_destinationRoot, $"{FolderPrefix}*")
                .Select(p => (Path: p, Mtime: Directory.GetLastWriteTimeUtc(p)))
                .OrderByDescending(t => t.Mtime)
                .ToList();
            foreach (var (path, _) in siblings.Skip(_retention))
            {
                try { Directory.Delete(path, recursive: true); pruned++; }
                catch { /* best effort per folder */ }
            }
        }
        catch { /* prune must never fail the run */ }
        return pruned;
    }
}
