using System.Text.Json;

namespace SDRLoggerPlus.Server.Services.Qsl;

public class QslBlockState
{
    /// <summary>When Club Log's one-strike auth block was tripped, or null if clear.</summary>
    public DateTime? ClubLogBlockedUtc { get; set; }

    public string? ClubLogReason { get; set; }
}

/// <summary>
/// Persists the Club Log one-strike auth block across restarts.
///
/// Club Log firewalls IP addresses that repeat failed POSTs to realtime.php,
/// so the service stops all uploads after a single 403 until credentials are
/// re-saved. That flag was a <c>volatile bool</c> — process state — so every
/// restart cleared it and the next logged QSO fired another 403. An operator
/// who restarts the app a few times while troubleshooting bad credentials was
/// walking straight into the ban the one-strike rule exists to avoid.
///
/// Stored as a small JSON file beside the database, mirroring BackupStateStore,
/// rather than in UserSettings — this is machine state, not operator
/// preference, and it must not ride along in a settings export.
/// </summary>
public class QslBlockStateStore
{
    private readonly string _path;
    private readonly object _gate = new();

    public QslBlockStateStore(string path) => _path = path;

    public QslBlockState Load()
    {
        lock (_gate)
        {
            try
            {
                if (!File.Exists(_path)) return new QslBlockState();
                return JsonSerializer.Deserialize<QslBlockState>(File.ReadAllText(_path)) ?? new QslBlockState();
            }
            catch
            {
                // A malformed state file must not stop uploads. Failing OPEN is
                // right here: the alternative is refusing to upload forever
                // because a JSON file got truncated.
                return new QslBlockState();
            }
        }
    }

    public void Save(QslBlockState state)
    {
        lock (_gate)
        {
            try
            {
                File.WriteAllText(_path, JsonSerializer.Serialize(state));
            }
            catch
            {
                // Best effort. The in-memory flag still protects this run; the
                // cost of a failed write is only that a restart forgets.
            }
        }
    }
}
