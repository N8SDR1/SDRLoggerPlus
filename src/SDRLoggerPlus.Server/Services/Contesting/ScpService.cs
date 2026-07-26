namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Super Check Partial: loads a master.scp callsign database (one call per line,
/// '#' comments) from %APPDATA%\SDRLoggerPlus\contests\master.scp if present, else
/// falls back to a bundled seed list (embedded <c>Data/master.scp</c>) so suggestions
/// work out of the box. The merged set (master ∪ the operator's own logged calls) is
/// served to both the contest entry window and the general Log Entry, which match
/// partial calls locally for instant suggestions.
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
                // No user file — fall back to the bundled seed so suggestions work out of the box.
                var seed = LoadEmbeddedSeed();
                lock (_lock) _calls = seed;
                _logger.LogInformation("No user master.scp; using bundled seed ({Count} calls)", seed.Count);
                return;
            }
            var parsed = Parse(File.ReadAllLines(_scpPath));
            lock (_lock) _calls = parsed;
            _logger.LogInformation("Loaded {Count} SCP calls from {Path}", parsed.Count, _scpPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load master.scp from {Path}", _scpPath);
            lock (_lock) _calls = LoadEmbeddedSeed();
        }
    }

    /// <summary>
    /// Save a downloaded MASTER.SCP to the user file (which takes precedence over the bundled
    /// seed) and reload. Returns the resulting call count. Creates the folder if needed.
    /// </summary>
    public int ImportMaster(string rawText)
    {
        var dir = Path.GetDirectoryName(_scpPath)!;
        Directory.CreateDirectory(dir);
        File.WriteAllText(_scpPath, rawText);
        Reload();
        return MasterCalls.Count;
    }

    /// <summary>When the user master.scp was last written, or null if only the seed is in use.</summary>
    public DateTime? UserFileUpdatedUtc =>
        File.Exists(_scpPath) ? File.GetLastWriteTimeUtc(_scpPath) : null;

    /// <summary>The bundled fallback list from embedded <c>Data/master.scp</c>; empty if missing.</summary>
    private static List<string> LoadEmbeddedSeed()
    {
        var assembly = typeof(ScpService).Assembly;
        var resourceName = assembly.GetManifestResourceNames()
            .FirstOrDefault(n => n.EndsWith("master.scp", StringComparison.OrdinalIgnoreCase));
        if (resourceName is null) return new List<string>();

        using var stream = assembly.GetManifestResourceStream(resourceName)!;
        using var reader = new StreamReader(stream);
        var lines = new List<string>();
        while (reader.ReadLine() is { } line) lines.Add(line);
        return Parse(lines);
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
