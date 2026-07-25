namespace SDRLoggerPlus.Server.Services.Ffma;

/// <summary>
/// The FFMA denominator: the set of four-character Maidenhead grids required for
/// the Fred Fish Memorial Award.
///
/// Loaded once from the embedded <c>Data/ffma_grids.txt</c> (same embedded-resource
/// pattern as CountyReference / CtyService) and cached for process lifetime. The file
/// is the source of truth: the award scores against exactly what's in it. When the
/// count is <see cref="OfficialCount"/> the list is treated as the official roster;
/// anything else is reported as incomplete so a placeholder or a bad edit can never
/// masquerade as a real award total.
/// </summary>
public static class FfmaGridReference
{
    /// <summary>The award requires all 488 grids in the contiguous 48 states.</summary>
    public const int OfficialCount = 488;

    private static readonly Lazy<IReadOnlySet<string>> Loaded =
        new(Load, LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>The required grids, uppercased four-char. Empty if the file is missing.</summary>
    public static IReadOnlySet<string> RequiredGrids => Loaded.Value;

    /// <summary>How many grids the loaded file defines — the award denominator.</summary>
    public static int TotalRequired => Loaded.Value.Count;

    /// <summary>True only when the loaded list is the full official 488.</summary>
    public static bool IsOfficial => Loaded.Value.Count == OfficialCount;

    private static IReadOnlySet<string> Load()
    {
        var set = new HashSet<string>(StringComparer.Ordinal);

        var assembly = typeof(FfmaGridReference).Assembly;
        var resourceName = assembly.GetManifestResourceNames()
            .FirstOrDefault(n => n.EndsWith("ffma_grids.txt", StringComparison.OrdinalIgnoreCase));
        if (resourceName is null) return set;

        using var stream = assembly.GetManifestResourceStream(resourceName)!;
        using var reader = new StreamReader(stream);

        while (reader.ReadLine() is { } line)
        {
            var text = line.Trim();
            if (text.Length == 0 || text[0] == '#') continue;

            // Accept commas, spaces or tabs — so either one-per-line or the comma-
            // separated form communities publish can be pasted in as-is.
            foreach (var token in text.Split([',', ' ', '\t'], StringSplitOptions.RemoveEmptyEntries))
            {
                var grid = token.Trim().ToUpperInvariant();
                if (IsLegalGrid(grid)) set.Add(grid);
            }
        }

        return set;
    }

    /// <summary>A legal four-character Maidenhead field+square (AA00–RR99).</summary>
    private static bool IsLegalGrid(string g) =>
        g.Length == 4 &&
        g[0] is >= 'A' and <= 'R' && g[1] is >= 'A' and <= 'R' &&
        g[2] is >= '0' and <= '9' && g[3] is >= '0' and <= '9';
}
