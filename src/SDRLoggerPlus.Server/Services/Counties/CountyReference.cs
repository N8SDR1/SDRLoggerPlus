namespace SDRLoggerPlus.Server.Services.Counties;

/// <summary>
/// The USA-CA denominator: which counties exist, per state.
///
/// Loaded once from the embedded <c>Data/us_counties.csv</c> (see CtyService
/// for the same embedded-resource pattern) and cached for process lifetime —
/// the list only changes when we ship a new one.
///
/// The shipped list is Census-derived and carries ~3,144 counties. MARAC's
/// official USA-CA list is ~3,077; the two differ over Virginia's independent
/// cities and Alaska's boroughs/census areas. Progress reported against this
/// list is indicative, not an award submission — the file's own header says so
/// and the UI repeats it.
/// </summary>
public static class CountyReference
{
    private static readonly Lazy<Data> Loaded = new(Load, LazyThreadSafetyMode.ExecutionAndPublication);

    private sealed record Data(
        IReadOnlyDictionary<string, IReadOnlyList<string>> CountiesByState,
        IReadOnlyDictionary<string, IReadOnlySet<string>> KeysByState,
        int Total);

    /// <summary>State codes present in the reference, ascending. All 50.</summary>
    public static IReadOnlyList<string> States => Loaded.Value.CountiesByState.Keys.OrderBy(s => s, StringComparer.Ordinal).ToList();

    /// <summary>Total counties across every state — the award denominator.</summary>
    public static int TotalCounties => Loaded.Value.Total;

    /// <summary>How many counties a state has, or 0 for an unknown state code.</summary>
    public static int CountyCount(string? stateCode) =>
        stateCode != null && Loaded.Value.CountiesByState.TryGetValue(stateCode.ToUpperInvariant(), out var list)
            ? list.Count : 0;

    /// <summary>Display names of a state's counties, ascending. Empty for an unknown state.</summary>
    public static IReadOnlyList<string> CountiesIn(string? stateCode) =>
        stateCode != null && Loaded.Value.CountiesByState.TryGetValue(stateCode.ToUpperInvariant(), out var list)
            ? list : Array.Empty<string>();

    /// <summary>
    /// True when the state has a county whose normalized name matches. Used to
    /// keep a typo or a foreign locality from being counted as a US county.
    /// </summary>
    public static bool IsKnownCounty(string? stateCode, string? countyName)
    {
        if (stateCode == null) return false;
        var key = CountyNameNormalizer.Normalize(countyName);
        if (key.Length == 0) return false;
        return Loaded.Value.KeysByState.TryGetValue(stateCode.ToUpperInvariant(), out var keys) && keys.Contains(key);
    }

    private static Data Load()
    {
        var assembly = typeof(CountyReference).Assembly;
        var resourceName = assembly.GetManifestResourceNames()
            .First(n => n.EndsWith("us_counties.csv", StringComparison.OrdinalIgnoreCase));

        using var stream = assembly.GetManifestResourceStream(resourceName)!;
        using var reader = new StreamReader(stream);

        var byState = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var keysByState = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        while (reader.ReadLine() is { } line)
        {
            var text = line.Trim();
            if (text.Length == 0 || text[0] == '#') continue;

            var comma = text.IndexOf(',');
            if (comma <= 0 || comma == text.Length - 1) continue;

            var state = text[..comma].Trim().ToUpperInvariant();
            var county = text[(comma + 1)..].Trim();
            if (state.Length != 2 || county.Length == 0) continue;

            var key = CountyNameNormalizer.Normalize(county);
            if (key.Length == 0) continue;

            if (!keysByState.TryGetValue(state, out var keys))
                keysByState[state] = keys = new HashSet<string>(StringComparer.Ordinal);
            // A state listing the same county twice must not inflate its target.
            if (!keys.Add(key)) continue;

            if (!byState.TryGetValue(state, out var list))
                byState[state] = list = new List<string>();
            list.Add(county);
        }

        foreach (var list in byState.Values)
            list.Sort(StringComparer.OrdinalIgnoreCase);

        return new Data(
            byState.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<string>)kv.Value, StringComparer.Ordinal),
            keysByState.ToDictionary(kv => kv.Key, kv => (IReadOnlySet<string>)kv.Value, StringComparer.Ordinal),
            byState.Values.Sum(v => v.Count));
    }
}
