namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Parsing helpers for compound / portable amateur callsigns.
///
/// A compound call carries a '/'. Two shapes exist:
///   • PREFIX form — "F/HB9GUX": the home operator HB9GUX (Switzerland)
///     is operating FROM France (F is France's DXCC prefix). The DXCC
///     entity is France; the operator identity is HB9GUX.
///   • SUFFIX form — "HB9GUX/P" (portable), "W1AW/4" (in the 4th US call
///     area), "DL1ABC/MM" (maritime mobile): the home call comes first and
///     the trailing token is an operating qualifier, not a new entity.
///
/// QRZ and HamQTH are keyed on the operator's home ("base") call, so looking
/// up the literal compound string usually returns nothing and the profile
/// panel spins. We strip to the base call for the callbook lookup while
/// resolving the DXCC entity from the FULL call via cty.dat.
/// </summary>
public static class CallsignHelper
{
    // Single-token operating qualifiers that are never the base call.
    private static readonly HashSet<string> KnownSuffixes = new(StringComparer.OrdinalIgnoreCase)
    {
        "P",    // portable
        "M",    // mobile
        "MM",   // maritime mobile
        "AM",   // aeronautical mobile
        "A",    // alternate / portable (some administrations)
        "QRP",  // low power
        "QRPP", // very low power
        "LH",   // lighthouse activation
        "B",    // beacon
        "R",    // repeater
    };

    /// <summary>
    /// The best "home" callsign to hand a callbook lookup. Strips a leading
    /// country prefix (F/…) or a trailing operating qualifier (…/P, …/4).
    /// Returns the (upper-cased, trimmed) input when it isn't compound or when
    /// nothing in it looks like a base call.
    /// </summary>
    public static string ExtractBaseCall(string? callsign)
    {
        if (string.IsNullOrWhiteSpace(callsign)) return callsign?.Trim().ToUpperInvariant() ?? "";
        var call = callsign.Trim().ToUpperInvariant();
        if (!call.Contains('/')) return call;

        var parts = call.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return call;
        if (parts.Length == 1) return parts[0];

        // The base call is the token that actually looks like a full callsign:
        // at least one letter AND one digit, not a known qualifier. Among
        // qualifying tokens the longest wins (handles "PA/DL1ABC/P").
        string? best = null;
        foreach (var p in parts)
        {
            if (KnownSuffixes.Contains(p)) continue;
            var hasLetter = p.Any(char.IsLetter);
            var hasDigit = p.Any(char.IsDigit);
            if (hasLetter && hasDigit && p.Length >= 3 && (best is null || p.Length > best.Length))
                best = p;
        }
        // Fall back to the longest token overall (covers unusual forms).
        return best ?? parts.OrderByDescending(p => p.Length).First();
    }

    /// <summary>
    /// True when the call is compound (has a base call that differs from the
    /// literal input) — i.e. a callbook lookup should target the base call.
    /// </summary>
    public static bool IsCompound(string? callsign)
    {
        if (string.IsNullOrWhiteSpace(callsign)) return false;
        var call = callsign.Trim().ToUpperInvariant();
        return call.Contains('/') && !string.Equals(call, ExtractBaseCall(call), StringComparison.Ordinal);
    }

    /// <summary>
    /// True for the PREFIX form (e.g. "F/HB9GUX") where the base call is NOT the
    /// first token — meaning the operator is portable in the leading token's
    /// DXCC entity, so the entity comes from the full call, not the base call.
    /// </summary>
    public static bool IsPrefixForm(string? callsign)
    {
        if (!IsCompound(callsign)) return false;
        var call = callsign!.Trim().ToUpperInvariant();
        var parts = call.Split('/', StringSplitOptions.RemoveEmptyEntries);
        return parts.Length >= 2
            && !string.Equals(parts[0], ExtractBaseCall(call), StringComparison.Ordinal);
    }

    /// <summary>
    /// A short human-readable note for a compound call, or null for a plain
    /// base call. <paramref name="countryOfFull"/> resolves the DXCC country of
    /// the FULL call (cty.dat) so a PREFIX form can name where the operator is.
    /// </summary>
    public static string? DescribeCompound(string? callsign, Func<string, string?> countryOfFull)
    {
        if (!IsCompound(callsign)) return null;
        var call = callsign!.Trim().ToUpperInvariant();
        var baseCall = ExtractBaseCall(call);
        var parts = call.Split('/', StringSplitOptions.RemoveEmptyEntries);

        if (IsPrefixForm(call))
        {
            var country = countryOfFull(call);
            return country != null
                ? $"{baseCall} operating from {country}"
                : $"{baseCall} portable ({parts[0]}/…)";
        }

        // Suffix form — the trailing token qualifies the base call.
        var suffix = parts[^1];
        var meaning = suffix switch
        {
            "P" => "portable",
            "M" => "mobile",
            "MM" => "maritime mobile",
            "AM" => "aeronautical mobile",
            "QRP" or "QRPP" => "low power",
            "LH" => "lighthouse",
            _ when suffix.Length == 1 && char.IsDigit(suffix[0]) => $"in call area {suffix}",
            _ => $"/{suffix}",
        };
        return $"{baseCall} ({meaning})";
    }
}
