namespace SDRLoggerPlus.Server.Services.Adif;

/// <summary>What a normalizer did to one field of one record.</summary>
public enum AdifFieldAction
{
    /// <summary>Value was already canonical. Nothing recorded.</summary>
    Unchanged,

    /// <summary>Value was rewritten to the canonical spelling with no loss of meaning.</summary>
    Corrected,

    /// <summary>Value is not one this app recognises. Kept verbatim, surfaced for review.</summary>
    Flagged,
}

/// <param name="Field">"band" or "mode".</param>
/// <param name="Original">Exactly what the file contained.</param>
/// <param name="Result">What was stored — equal to <paramref name="Original"/> when flagged.</param>
/// <param name="Note">Why, in words the operator can act on.</param>
public record AdifFieldIssue(
    string Field,
    string Original,
    string Result,
    AdifFieldAction Action,
    string Note);

/// <summary>
/// Canonicalises the two ADIF fields this log has repeatedly been damaged by, and reports
/// everything it could not canonicalise.
///
/// The rule throughout is that a rewrite must lose no information. Band case is safe to fix
/// (ADIF declares band case-insensitive, so "40M" and "40m" are the same value spelled two
/// ways). A mode is only rewritten when the source spelling has exactly one meaning — "PH"
/// is phone, and phone is SSB. Anything else, including plainly corrupt values like "FT2",
/// is kept exactly as it arrived and flagged: an unrecognised mode is still the truth about
/// that contact, and guessing at it would destroy the evidence needed to repair it properly.
///
/// This exists because none of it was checked before. Import validated CALL and QSO_DATE and
/// took everything else verbatim, which let 1,130 records into a 24.5k log carrying modes the
/// ADIF enumeration does not define — and, because the duplicate key was built from the mode,
/// let each of them import a second time as a "new" contact.
/// </summary>
public static class AdifFieldNormalizer
{
    /// <summary>
    /// Modes this app treats as canonical. Drawn from the ADIF MODE enumeration plus the
    /// submodes that logging programs conventionally write into MODE (FT4, PSK31, JT65B) —
    /// this log contains all three, and rewriting them would lose the submode, which Qso has
    /// nowhere to store. Deliberately permissive: the cost of accepting an unusual-but-real
    /// mode is nil, while wrongly "correcting" a real one is data loss.
    /// </summary>
    private static readonly HashSet<string> CanonicalModes = new(StringComparer.OrdinalIgnoreCase)
    {
        "AM", "ARDOP", "ATV", "C4FM", "CHIP", "CLO", "CONTESTI", "CW", "DIGITALVOICE",
        "DOMINO", "DYNAMIC", "FAX", "FM", "FREEDV", "FSK441", "FT4", "FT8", "HELL",
        "ISCAT", "JS8", "JT4", "JT6M", "JT9", "JT44", "JT65", "JT65B", "JT65C",
        "MFSK", "MSK144", "MT63", "OLIVIA", "OPERA", "PAC", "PAX", "PKT", "PSK",
        "PSK31", "PSK63", "PSK125", "PSK2K", "Q65", "QRA64", "ROS", "RTTY", "RTTYM",
        "SSB", "SSTV", "T10", "THOR", "THRB", "TOR", "V4", "VOI", "WINMOR", "WSPR",
        "VARA HF", "VARA FM", "VARA SATELLITE",
        // ADIF submodes of SSB, but loggers routinely write them into MODE and they carry
        // real information (which sideband). This log holds 192 of them; the edit form
        // offers both; the confirmation-merge key already collapses them to SSB. Flagging
        // them as unrecognised would tell the operator to go check 192 perfectly good QSOs.
        "USB", "LSB",
    };

    /// <summary>
    /// Mode spellings with exactly one possible meaning, safe to rewrite.
    ///
    /// Kept deliberately short. "PH" is the only entry earned by evidence rather than
    /// assumption: all 340 PH records in this log have an SSB twin at the same callsign,
    /// date and second, so the source demonstrably meant SSB. Truncations like "FT2",
    /// "PSK3" and "29" are NOT here — they look guessable, but the guess would be ours,
    /// not the operator's, and a wrong mode is worse than a flagged one.
    /// </summary>
    private static readonly Dictionary<string, string> ModeAliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["PH"] = "SSB",
        ["PHONE"] = "SSB",
        ["SSB/USB"] = "SSB",
        ["SSB/LSB"] = "SSB",
    };

    /// <summary>
    /// Bands this app knows, in their canonical lower-case spelling. Matching is
    /// case-insensitive, so "40M" resolves to "40m" and is corrected on the way in.
    /// </summary>
    private static readonly string[] CanonicalBands =
    {
        "2200m", "630m", "160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m",
        "12m", "10m", "6m", "4m", "2m", "1.25m", "70cm", "33cm", "23cm", "13cm",
        "9cm", "6cm", "3cm", "1.25cm",
    };

    private static readonly Dictionary<string, string> BandLookup =
        CanonicalBands.ToDictionary(b => b, b => b, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Canonicalise a band. Case differences are corrected silently-but-recorded; an
    /// unknown band is kept and flagged.
    /// </summary>
    public static (string? Value, AdifFieldIssue? Issue) NormalizeBand(string? band)
    {
        var raw = band?.Trim();
        if (string.IsNullOrEmpty(raw)) return (band, null);

        if (BandLookup.TryGetValue(raw, out var canonical))
        {
            if (string.Equals(canonical, raw, StringComparison.Ordinal)) return (canonical, null);

            return (canonical, new AdifFieldIssue(
                "band", raw, canonical, AdifFieldAction.Corrected,
                $"Band case normalised to '{canonical}'. ADIF treats band as case-insensitive, " +
                "so this is the same band spelled differently."));
        }

        return (raw, new AdifFieldIssue(
            "band", raw, raw, AdifFieldAction.Flagged,
            $"'{raw}' is not a band this app recognises. Stored unchanged."));
    }

    /// <summary>
    /// Canonicalise a mode. Only unambiguous aliases are rewritten; anything unrecognised is
    /// kept verbatim and flagged so the operator can decide what it was meant to be.
    /// </summary>
    public static (string? Value, AdifFieldIssue? Issue) NormalizeMode(string? mode)
    {
        var raw = mode?.Trim();
        if (string.IsNullOrEmpty(raw)) return (mode, null);

        if (ModeAliases.TryGetValue(raw, out var mapped))
        {
            return (mapped, new AdifFieldIssue(
                "mode", raw, mapped, AdifFieldAction.Corrected,
                $"'{raw}' is not an ADIF mode; it means {mapped} and was stored as such."));
        }

        if (CanonicalModes.TryGetValue(raw, out var canonical))
        {
            if (string.Equals(canonical, raw, StringComparison.Ordinal)) return (canonical, null);

            return (canonical, new AdifFieldIssue(
                "mode", raw, canonical, AdifFieldAction.Corrected,
                $"Mode case normalised to '{canonical}'."));
        }

        return (raw, new AdifFieldIssue(
            "mode", raw, raw, AdifFieldAction.Flagged,
            $"'{raw}' is not a mode the ADIF enumeration defines. Stored unchanged — " +
            "check the source log, it may be a truncated field."));
    }

    /// <summary>True when this value is one we would store without complaint.</summary>
    public static bool IsKnownMode(string? mode) =>
        !string.IsNullOrWhiteSpace(mode) &&
        (CanonicalModes.Contains(mode.Trim()) || ModeAliases.ContainsKey(mode.Trim()));

    /// <summary>True when this value is a band we recognise, in any casing.</summary>
    public static bool IsKnownBand(string? band) =>
        !string.IsNullOrWhiteSpace(band) && BandLookup.ContainsKey(band.Trim());

    /// <summary>
    /// The canonical spelling used for identity comparisons — duplicate keys, worked-before
    /// lookups. Falls back to the trimmed upper-case original so unknown values still compare
    /// consistently with themselves.
    /// </summary>
    public static string CanonicalBandKey(string? band)
    {
        var raw = band?.Trim();
        if (string.IsNullOrEmpty(raw)) return "";
        return BandLookup.TryGetValue(raw, out var c) ? c : raw.ToUpperInvariant();
    }
}
