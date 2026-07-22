using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services.Counties;

/// <summary>
/// Works out which US county a QSO belongs to, for USA-CA counting.
///
/// Reads <see cref="StationInfo.County"/> first, then falls back to the ADIF
/// CNTY value in <c>AdifExtra</c>. The fallback is not defensive padding — it
/// is where the data actually is. ADIF import mapped STATE but not CNTY, so
/// every imported county landed in the unmapped-extras bucket: on the author's
/// log that is 15,318 QSOs (~1,814 distinct counties) with a populated CNTY
/// and an empty County field. Reading both means the award works on existing
/// logs immediately, with no migration and nothing rewritten; the import fix
/// makes newly logged QSOs canonical going forward.
///
/// Eligibility mirrors <see cref="UsStateResolver"/> deliberately: whatever
/// produced the county, the QSO's country must be a US entity, so a "Kent"
/// logged in England cannot land in Kent County, Michigan.
/// </summary>
public static class CountyResolver
{
    /// <summary>A QSO's county placement. State is a 2-letter code.</summary>
    public readonly record struct CountyPlacement(string State, string County);

    /// <summary>ADIF field carrying the worked station's county.</summary>
    private const string AdifCountyField = "CNTY";

    /// <summary>
    /// Satellite QSOs are excluded from USA-CA. Delegates to
    /// <see cref="Satellites.SatelliteResolver"/> so "is this a satellite QSO"
    /// has exactly one answer across the app — the SAT award counting a QSO
    /// that USA-CA also counted would be a contradiction, not a rounding error.
    /// </summary>
    public static bool IsSatellite(Qso qso) => Satellites.SatelliteResolver.IsSatellite(qso);

    /// <summary>
    /// Resolves the county for a QSO, or null when it cannot count: satellite,
    /// no state (the caller's US-eligibility verdict), no county recorded, or a
    /// county name the reference list does not recognise.
    ///
    /// <paramref name="state"/> comes from the caller's existing WAS state
    /// resolution rather than being re-derived here, so USA-CA and WAS always
    /// agree about which QSOs are US and which state they sit in. A null state
    /// is the caller saying "not an eligible US QSO", and ends the matter.
    /// </summary>
    public static CountyPlacement? Resolve(Qso qso, string? state)
    {
        if (state == null) return null;
        if (IsSatellite(qso)) return null;

        var raw = FirstNonBlank(qso.Station?.County, ReadAdifCounty(qso));
        if (raw == null) return null;

        var (_, countyName) = CountyNameNormalizer.SplitStatePrefix(raw);
        if (countyName.Length == 0) return null;

        // The "ST," prefix is dropped rather than trusted. On this log it
        // disagrees with the resolved state on 226 of 15,318 QSOs, and the
        // resolved state is the one WAS already counts — letting the prefix
        // win would place those counties in a state WAS says they aren't in.
        if (!CountyReference.IsKnownCounty(state, countyName)) return null;

        return new CountyPlacement(state, Canonical(state, countyName));
    }

    /// <summary>
    /// True when a QSO confirms a county. USA-CA is a confirmation award, but
    /// LoTW is deliberately excluded: it does not reliably carry CNTY, so a
    /// LoTW match cannot vouch for the county the QSO claims.
    /// </summary>
    public static bool IsConfirmed(Qso qso)
    {
        var qsl = qso.Qsl;
        if (qsl == null) return false;
        return string.Equals(qsl.Rcvd, "Y", StringComparison.OrdinalIgnoreCase)
            || string.Equals(qsl.Eqsl?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Reference spelling for a county, so the same place reported twice reads
    /// identically. Falls back to the logged name when the reference has no
    /// display form (it will not, given Resolve validated it first).
    /// </summary>
    private static string Canonical(string state, string countyName)
    {
        var key = CountyNameNormalizer.Normalize(countyName);
        foreach (var candidate in CountyReference.CountiesIn(state))
            if (CountyNameNormalizer.Normalize(candidate) == key)
                return candidate;
        return countyName;
    }

    private static string? ReadAdifCounty(Qso qso) => ReadAdifField(qso, AdifCountyField);

    private static string? ReadAdifField(Qso qso, string name) => AdifExtraReader.Read(qso, name);

    private static string? FirstNonBlank(params string?[] values)
    {
        foreach (var v in values)
            if (!string.IsNullOrWhiteSpace(v)) return v.Trim();
        return null;
    }
}
