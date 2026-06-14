using System.Text.Json;

namespace SDRLoggerPlus.Server.Services.Backup;

public class BackupState
{
    public DateTime? LastRunUtc { get; set; }
    public bool? Ok { get; set; }
    public string? Message { get; set; }
    public string? Path { get; set; }
}

/// <summary>
/// Persists backup state to a small JSON file next to the database so the
/// schedule anchor survives restarts without writing UserSettings every run.
/// </summary>
public class BackupStateStore
{
    private readonly string _path;

    public BackupStateStore(string path) => _path = path;

    public BackupState Load()
    {
        try
        {
            if (!File.Exists(_path)) return new BackupState();
            return JsonSerializer.Deserialize<BackupState>(File.ReadAllText(_path)) ?? new BackupState();
        }
        catch
        {
            return new BackupState();
        }
    }

    public void Save(BackupState state)
    {
        try
        {
            File.WriteAllText(_path, JsonSerializer.Serialize(state));
        }
        catch
        {
            // best effort — a failed state write must never break a backup run
        }
    }
}
