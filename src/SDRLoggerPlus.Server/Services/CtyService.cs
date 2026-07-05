using System.Text.RegularExpressions;

namespace SDRLoggerPlus.Server.Services;

// Lat/Lon are in STANDARD convention (east-positive, north-positive) — the
// parser negates cty.dat's longitude column which is stored east-negative.
internal record CtyEntity(string Country, string Continent, int CqZone, int ItuZone, double Lat, double Lon);

/// <summary>
/// Snapshot of the currently-loaded cty.dat: how many prefixes it holds, when
/// the file on disk was last updated, whether it's the bundled default or a
/// user-supplied override, and — when detectable — a friendly version string
/// pulled from Jim Reisert's release-date comment inside the file.
/// </summary>
public record CtyStatus(int PrefixCount, DateTime? UpdatedUtc, string Source, string? Version, string SourceUrl);

internal static partial class CtyService
{
    // The public download source for Jim Reisert's (AD1C) cty.dat.
    // A "Update Country Files" action in Settings POSTs to /api/cty/update
    // which downloads from here, validates, and writes to the user-override
    // path returned by GetUserOverridePath().
    public const string SourceUrl = "https://www.country-files.com/cty/cty.dat";

    // Backing fields — swappable at runtime so a user-triggered update can
    // reload cty.dat without a process restart. Guarded by _loadLock so a
    // concurrent lookup can't observe a torn state during a reload.
    private static Dictionary<string, CtyEntity>? _prefixMap;
    private static Dictionary<string, string>? _countryToContinentMap;
    private static string? _cachedVersion;
    private static readonly object _loadLock = new();

    // Regex to strip override annotations from prefixes: (#), [#], <#/#>, {aa}, ~#~
    [GeneratedRegex(@"\(\d+\)|\[\d+\]|<\d+/\d+>|\{[a-zA-Z]+\}|~\d+~")]
    private static partial Regex AnnotationRegex();

    private static Dictionary<string, CtyEntity> PrefixMap
    {
        get
        {
            if (_prefixMap != null) return _prefixMap;
            lock (_loadLock)
            {
                _prefixMap ??= LoadCurrentCtyDat();
                return _prefixMap;
            }
        }
    }

    public static (string? Country, string? Continent) GetCountryFromCallsign(string callsign)
    {
        if (string.IsNullOrEmpty(callsign))
            return (null, null);

        var entity = FindEntity(callsign);
        return entity is null ? (null, null) : (entity.Country, entity.Continent);
    }

    public static (string? Country, string? Continent, int? CqZone) GetEntityFromCallsign(string callsign)
    {
        if (string.IsNullOrEmpty(callsign))
            return (null, null, null);

        var entity = FindEntity(callsign);
        return entity is null ? (null, null, null) : (entity.Country, entity.Continent, entity.CqZone);
    }

    /// <summary>
    /// Approximate lat/lon of the DXCC entity that owns this callsign — the
    /// country/entity centroid from AD1C's cty.dat. This is the last-ditch
    /// fallback the callsign-lookup chain uses when QRZ + HamQTH have nothing.
    /// A country centroid is not the operator's real QTH (a W6 in LA and a W6
    /// in Redding both centroid to central California) — good enough to draw a
    /// bearing line to roughly the right corner of the world.
    /// </summary>
    public static (double Lat, double Lon)? GetCentroidFromCallsign(string callsign)
    {
        if (string.IsNullOrEmpty(callsign))
            return null;

        var entity = FindEntity(callsign);
        return entity is null ? null : (entity.Lat, entity.Lon);
    }

    public static string? GetContinentFromCountryName(string countryName)
    {
        if (string.IsNullOrEmpty(countryName))
            return null;

        return CountryToContinentMap.TryGetValue(countryName, out var continent)
            ? continent
            : null;
    }

    public static int PrefixCount => PrefixMap.Count;

    /// <summary>
    /// Return a status snapshot for the Settings UI: prefix count, mtime of
    /// the user override (or null if we're on the bundled default), which
    /// source is active, and — when detectable — a version string.
    /// </summary>
    public static CtyStatus GetStatus()
    {
        var userPath = GetUserOverridePath();
        var isOverride = File.Exists(userPath);
        var updatedUtc = isOverride ? File.GetLastWriteTimeUtc(userPath) : (DateTime?)null;
        return new CtyStatus(
            PrefixCount: PrefixMap.Count,
            UpdatedUtc: updatedUtc,
            Source: isOverride ? "user-updated" : "bundled",
            Version: _cachedVersion,
            SourceUrl: SourceUrl);
    }

    /// <summary>
    /// The user-writable path where a downloaded cty.dat is stored. Loaded in
    /// preference to the embedded resource when present. Same directory as
    /// UserConfigService's config.json so all user data lives together.
    /// </summary>
    public static string GetUserOverridePath()
    {
        string configDir;
        if (OperatingSystem.IsMacOS())
        {
            configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                "Library", "Application Support", "SDRLoggerPlus");
        }
        else if (OperatingSystem.IsWindows())
        {
            configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "SDRLoggerPlus");
        }
        else
        {
            configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                ".config", "SDRLoggerPlus");
        }
        return Path.Combine(configDir, "cty.dat");
    }

    /// <summary>
    /// Reload cty.dat from disk — called after a successful /api/cty/update
    /// so subsequent lookups see the new data without a process restart.
    /// </summary>
    public static void ReloadFromDisk()
    {
        lock (_loadLock)
        {
            _prefixMap = LoadCurrentCtyDat();
            _countryToContinentMap = null; // rebuilt lazily on next access
        }
    }

    private static Dictionary<string, string> CountryToContinentMap
    {
        get
        {
            if (_countryToContinentMap != null) return _countryToContinentMap;
            lock (_loadLock)
            {
                _countryToContinentMap ??= BuildCountryToContinentMap();
                return _countryToContinentMap;
            }
        }
    }

    private static CtyEntity? FindEntity(string callsign)
    {
        var map = PrefixMap;
        // Longest-prefix match: try from length 6 down to 1
        for (int len = Math.Min(callsign.Length, 6); len >= 1; len--)
        {
            var prefix = callsign[..len];
            if (map.TryGetValue(prefix, out var entity))
                return entity;
        }
        return null;
    }

    // ── Loaders ─────────────────────────────────────────────────────────────

    private static Dictionary<string, CtyEntity> LoadCurrentCtyDat()
    {
        // Prefer a user-supplied override (from /api/cty/update). If it's
        // missing, malformed, or throws — fall back to the embedded bundle
        // so the app still works with the shipped defaults.
        var userPath = GetUserOverridePath();
        if (File.Exists(userPath))
        {
            try
            {
                var content = File.ReadAllText(userPath);
                var map = ParseCtyDat(content);
                if (map.Count > 100) // sanity check: real cty.dat has ~5000+ prefixes
                {
                    _cachedVersion = ExtractVersion(content);
                    return map;
                }
            }
            catch
            {
                // fall through to embedded
            }
        }
        return ParseEmbeddedCtyDat();
    }

    private static Dictionary<string, CtyEntity> ParseEmbeddedCtyDat()
    {
        var assembly = typeof(CtyService).Assembly;
        var resourceName = assembly.GetManifestResourceNames()
            .First(n => n.EndsWith("cty.dat", StringComparison.OrdinalIgnoreCase));

        using var stream = assembly.GetManifestResourceStream(resourceName)!;
        using var reader = new StreamReader(stream);
        var content = reader.ReadToEnd();
        _cachedVersion = ExtractVersion(content);
        return ParseCtyDat(content);
    }

    /// <summary>
    /// Pull a version identifier out of cty.dat's optional comment lines.
    /// AD1C's file conventionally opens with lines like "# VER20260428"
    /// (release date). We look for that pattern; returns null if absent.
    /// </summary>
    private static string? ExtractVersion(string content)
    {
        var head = content.Length > 500 ? content[..500] : content;
        var match = Regex.Match(head, @"VER\s*(\d{8})", RegexOptions.IgnoreCase);
        if (match.Success && match.Groups.Count > 1)
        {
            var raw = match.Groups[1].Value;
            // Format YYYYMMDD → YYYY-MM-DD
            if (raw.Length == 8)
                return $"{raw[..4]}-{raw.Substring(4, 2)}-{raw.Substring(6, 2)}";
            return raw;
        }
        return null;
    }

    internal static Dictionary<string, CtyEntity> ParseCtyDat(string content)
    {
        var map = new Dictionary<string, CtyEntity>(StringComparer.OrdinalIgnoreCase);
        var lines = content.Split('\n');

        string? country = null;
        string? continent = null;
        int cqZone = 0;
        int ituZone = 0;
        double lat = 0;
        double lon = 0;

        var i = 0;
        while (i < lines.Length)
        {
            var line = lines[i].TrimEnd('\r');

            // Skip empty lines
            if (string.IsNullOrWhiteSpace(line))
            {
                i++;
                continue;
            }

            // Header line: no leading whitespace, colon-delimited fields
            if (line.Length > 0 && !char.IsWhiteSpace(line[0]))
            {
                // Format: Country:  CQ:  ITU:  Continent:  Lat:  Lon:  UTC:  Primary Prefix:
                //
                // NOTE on longitude: cty.dat stores lon as east-negative
                // (positive = west of Greenwich). We negate here so downstream
                // code deals in standard east-positive convention throughout.
                var fields = line.Split(':');
                if (fields.Length >= 8)
                {
                    country = fields[0].Trim();
                    int.TryParse(fields[1].Trim(), out cqZone);
                    int.TryParse(fields[2].Trim(), out ituZone);
                    continent = fields[3].Trim();
                    _ = double.TryParse(fields[4].Trim(),
                        System.Globalization.NumberStyles.Float,
                        System.Globalization.CultureInfo.InvariantCulture,
                        out lat);
                    _ = double.TryParse(fields[5].Trim(),
                        System.Globalization.NumberStyles.Float,
                        System.Globalization.CultureInfo.InvariantCulture,
                        out lon);
                    lon = -lon; // AD1C convention → standard east-positive
                    // fields[6] = UTC offset (unused), fields[7] = primary prefix
                    var primaryPrefix = fields[7].Trim().TrimStart('*');
                    if (!string.IsNullOrEmpty(primaryPrefix))
                    {
                        var entity = new CtyEntity(country, continent, cqZone, ituZone, lat, lon);
                        map.TryAdd(primaryPrefix, entity);
                    }
                }
                i++;
                continue;
            }

            // Prefix line: leading whitespace, comma-separated prefixes, semicolon terminates entity
            if (country != null && continent != null)
            {
                var entity = new CtyEntity(country, continent, cqZone, ituZone, lat, lon);
                var trimmed = line.Trim().TrimEnd(';');
                var prefixes = trimmed.Split(',', StringSplitOptions.RemoveEmptyEntries);

                foreach (var rawPrefix in prefixes)
                {
                    var prefix = rawPrefix.Trim();

                    // Skip exact callsign matches (prefixed with =)
                    if (prefix.StartsWith('='))
                        continue;

                    // Strip override annotations
                    prefix = AnnotationRegex().Replace(prefix, "");

                    // Strip WAEDC indicator
                    prefix = prefix.TrimStart('*');

                    if (!string.IsNullOrEmpty(prefix))
                    {
                        map.TryAdd(prefix, entity);
                    }
                }
            }

            i++;
        }

        return map;
    }

    private static Dictionary<string, string> BuildCountryToContinentMap()
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in PrefixMap)
        {
            map.TryAdd(kvp.Value.Country, kvp.Value.Continent);
        }
        return map;
    }
}
