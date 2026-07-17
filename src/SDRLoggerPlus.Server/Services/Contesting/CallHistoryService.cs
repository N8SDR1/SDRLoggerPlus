namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Call-history / exchange prefill: parses a callhistory.txt file (header row of
/// field names, then comma-separated rows) from
/// %APPDATA%\SDRLoggerPlus\contests\callhistory.txt if present, so entering a
/// known call prefills its expected exchange (name, state, zone, grid, …).
///
/// Header names are mapped to the engine's exchange field keys. An N1MM-style
/// leading "!!Order!!" token is ignored. Degrades silently when no file exists.
/// </summary>
public class CallHistoryService
{
    private readonly ILogger<CallHistoryService> _logger;
    private readonly string _path;
    private IReadOnlyDictionary<string, Dictionary<string, string>> _byCall
        = new Dictionary<string, Dictionary<string, string>>();
    private readonly object _lock = new();

    public CallHistoryService(IUserConfigService userConfig, ILogger<CallHistoryService> logger)
    {
        _logger = logger;
        var configDir = Path.GetDirectoryName(userConfig.GetConfigPath()) ?? ".";
        _path = Path.Combine(configDir, "contests", "callhistory.txt");
        Reload();
    }

    public bool HasData { get { lock (_lock) return _byCall.Count > 0; } }

    /// <summary>Exchange field values for a call (by field key), or null if unknown.</summary>
    public Dictionary<string, string>? Lookup(string callsign)
    {
        var key = callsign.Trim().ToUpperInvariant();
        lock (_lock)
            return _byCall.TryGetValue(key, out var v) ? v : null;
    }

    public void Reload()
    {
        try
        {
            if (!File.Exists(_path))
            {
                lock (_lock) _byCall = new Dictionary<string, Dictionary<string, string>>();
                return;
            }
            var parsed = Parse(File.ReadAllLines(_path));
            lock (_lock) _byCall = parsed;
            _logger.LogInformation("Loaded {Count} call-history entries from {Path}", parsed.Count, _path);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load callhistory.txt from {Path}", _path);
            lock (_lock) _byCall = new Dictionary<string, Dictionary<string, string>>();
        }
    }

    /// <summary>
    /// Parse: first non-comment line is the header of field names; each subsequent
    /// row is comma-separated values aligned to the header. Returns call → {key:value}.
    /// </summary>
    public static Dictionary<string, Dictionary<string, string>> Parse(IEnumerable<string> lines)
    {
        var result = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
        string[]? header = null;
        int callIdx = -1;

        foreach (var raw in lines)
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#') || line.StartsWith("//")) continue;

            var cols = line.Split(',').Select(c => c.Trim()).ToArray();

            if (header == null)
            {
                header = cols
                    .Where(c => !c.Equals("!!Order!!", StringComparison.OrdinalIgnoreCase))
                    .Select(MapHeader).ToArray();
                callIdx = Array.IndexOf(header, "call");
                continue;
            }

            // Drop a leading order token if the data rows carry one.
            var values = cols;
            if (cols.Length == header.Length + 1) values = cols.Skip(1).ToArray();
            if (callIdx < 0 || callIdx >= values.Length) continue;

            var call = values[callIdx].ToUpperInvariant();
            if (string.IsNullOrWhiteSpace(call)) continue;

            var entry = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < header.Length && i < values.Length; i++)
            {
                if (header[i] == "call") continue;
                if (!string.IsNullOrWhiteSpace(values[i]))
                    entry[header[i]] = values[i];
            }
            result[call] = entry;
        }

        return result;
    }

    // Normalize header names to the engine's exchange field keys.
    private static string MapHeader(string h) => h.Trim().ToLowerInvariant() switch
    {
        "call" or "callsign" => "call",
        "name" => "name",
        "state" or "st" or "spc" => "state",
        "sect" or "section" or "arrlsect" => "section",
        "cqz" or "cqzone" or "zone" => "zone",
        "ituz" or "ituzone" => "ituzone",
        "grid" or "gridsquare" or "loc" => "grid",
        "pwr" or "power" => "power",
        "ck" or "check" => "check",
        _ => h.Trim().ToLowerInvariant(),
    };
}
