using System.Text.RegularExpressions;

namespace SDRLoggerPlus.Server.Services.Counties;

/// <summary>
/// Folds the many spellings of a US county name onto one comparison key.
///
/// ADIF CNTY is conventionally "ST,County Name" and that is exactly how this
/// log carries it (measured: 15,318 of 15,318 values on the author's log use
/// the prefixed form). But the prefix is a convention, not a guarantee, and
/// operators and callbooks vary the rest freely — "Los Angeles", "LOS ANGELES",
/// "Los Angeles County", "St. Louis" vs "St Louis", "Miami-Dade" vs
/// "Miami Dade". Matching raw strings would scatter one county across several
/// award entries and silently understate progress.
///
/// The normalizer is deliberately lossy in one direction only: it strips
/// things that never distinguish two real counties (case, punctuation, the
/// governing-type suffix) and leaves everything else alone. It does NOT try to
/// correct misspellings — a wrong county name should fail to match the
/// reference list and be visible, not be guessed into the wrong bucket.
/// </summary>
public static partial class CountyNameNormalizer
{
    /// <summary>
    /// Suffixes that name the governing type rather than the county. Census
    /// data carries them ("Aleutians East Borough", "St. Martin Parish"),
    /// ADIF usually does not. Order matters: longest first, so "Census Area"
    /// is consumed before "Area" could be.
    /// </summary>
    private static readonly string[] TypeSuffixes =
    {
        "census area", "city and borough", "borough", "parish", "county", "municipality",
    };

    [GeneratedRegex(@"[^a-z0-9]+", RegexOptions.CultureInvariant)]
    private static partial Regex NonAlphanumeric();

    /// <summary>
    /// Splits an ADIF CNTY value into its state prefix (when present) and the
    /// county name. "MN,Hennepin" → ("MN", "Hennepin"); "Hennepin" → (null,
    /// "Hennepin"). The prefix is only treated as a state when it is two
    /// letters, so a comma inside a county name cannot be mistaken for one.
    /// </summary>
    public static (string? State, string County) SplitStatePrefix(string? raw)
    {
        var value = (raw ?? string.Empty).Trim();
        if (value.Length == 0) return (null, string.Empty);

        var comma = value.IndexOf(',');
        if (comma < 0) return (null, value);

        var prefix = value[..comma].Trim();
        var rest = value[(comma + 1)..].Trim();
        if (prefix.Length == 2 && prefix.All(char.IsLetter) && rest.Length > 0)
            return (prefix.ToUpperInvariant(), rest);

        return (null, value);
    }

    /// <summary>
    /// Comparison key for a county name: lower-cased, type-suffix removed, and
    /// stripped of everything that is not a letter or digit. Returns an empty
    /// string for null/blank input so callers can test it with one check.
    /// </summary>
    public static string Normalize(string? countyName)
    {
        var value = (countyName ?? string.Empty).Trim().ToLowerInvariant();
        if (value.Length == 0) return string.Empty;

        // Strip a trailing governing-type word. Only one — "Foo County County"
        // is not a thing, and looping would eat a county genuinely named after
        // one of these words.
        foreach (var suffix in TypeSuffixes)
        {
            if (value.Length > suffix.Length + 1 && value.EndsWith(" " + suffix, StringComparison.Ordinal))
            {
                value = value[..^(suffix.Length + 1)].TrimEnd();
                break;
            }
        }

        // "St. Louis"/"St Louis", "Miami-Dade"/"Miami Dade", "O'Brien"/"OBrien"
        // all collapse here.
        return NonAlphanumeric().Replace(value, string.Empty);
    }
}
