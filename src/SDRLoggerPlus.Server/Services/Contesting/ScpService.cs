namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Super Check Partial: loads a master.scp callsign database (one call per line,
/// '#' comments) from %APPDATA%\SDRLoggerPlus\contests\master.scp if present. The
/// merged set (master file ∪ the operator's own logged calls) is served to the
/// contest entry window, which matches partial calls locally for instant
/// suggestions. Degrades silently (empty) when no file exists.
/// </summary>
public class ScpService
{
    private readonly ILogger<ScpService> _logger;
    private readonly string _scpPath;
    private IReadOnlyList<string> _calls = Array.Empty<string>();
    private readonly object _lock = new();

    public ScpService(IUserConfigService userConfig, ILogger<ScpService> logger)
    {
        _logger = logger;
        var configDir = Path.GetDirectoryName(userConfig.GetConfigPath()) ?? ".";
        _scpPath = Path.Combine(configDir, "contests", "master.scp");
        Reload();
    }

    /// <summary>Master.scp calls (uppercase, sorted, deduped). Empty when no file.</summary>
    public IReadOnlyList<string> MasterCalls
    {
        get { lock (_lock) return _calls; }
    }

    /// <summary>Re-read the master.scp file (e.g. after the operator drops in a new one).</summary>
    public void Reload()
    {
        try
        {
            if (!File.Exists(_scpPath))
            {
                lock (_lock) _calls = Array.Empty<string>();
                return;
            }
            var parsed = Parse(File.ReadAllLines(_scpPath));
            lock (_lock) _calls = parsed;
            _logger.LogInformation("Loaded {Count} SCP calls from {Path}", parsed.Count, _scpPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load master.scp from {Path}", _scpPath);
            lock (_lock) _calls = Array.Empty<string>();
        }
    }

    /// <summary>Parse master.scp lines: skip '#' comments and blanks; uppercase; dedupe; sort.</summary>
    public static List<string> Parse(IEnumerable<string> lines)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var raw in lines)
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#')) continue;
            // A line may hold multiple space/comma-separated calls; take each token.
            foreach (var token in line.Split(new[] { ' ', '\t', ',' }, StringSplitOptions.RemoveEmptyEntries))
            {
                var call = token.ToUpperInvariant();
                if (IsPlausibleCall(call)) set.Add(call);
            }
        }
        return set.OrderBy(c => c, StringComparer.Ordinal).ToList();
    }

    // A call is letters/digits and optional '/'; at least one digit and one letter.
    private static bool IsPlausibleCall(string s)
    {
        if (s.Length < 3 || s.Length > 15) return false;
        bool digit = false, letter = false;
        foreach (var c in s)
        {
            if (char.IsDigit(c)) digit = true;
            else if (char.IsLetter(c)) letter = true;
            else if (c != '/') return false;
        }
        return digit && letter;
    }
}
