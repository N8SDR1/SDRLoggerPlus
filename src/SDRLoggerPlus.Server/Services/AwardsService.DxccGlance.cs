using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

public partial class AwardsService
{
    /// <summary>
    /// Bands Statistics shows, longest wavelength first. Same list and order as
    /// <c>ALL_BANDS</c> in <c>src/SDRLoggerPlus.Web/src/utils/spotBands.ts</c>.
    /// </summary>
    internal static readonly string[] DxccGlanceBands =
    [
        "2200m", "630m", "160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m",
        "6m", "2m", "1.25m", "70cm", "33cm", "23cm",
    ];

    private static readonly HashSet<string> DxccGlanceBandSet = new(DxccGlanceBands, StringComparer.OrdinalIgnoreCase);

    public async Task<DxccEntityGlance?> GetDxccEntityGlanceAsync(string? callsign)
    {
        var entity = ResolveGlanceEntity(callsign);
        if (entity is null) return null;

        var personal = (await AllQsosAsync()).ToList();
        var columns = GlanceColumns(personal);
        var counted = QsosForEntity(personal, entity);

        var buckets = new Dictionary<(string Band, string Group), List<Qso>>();
        foreach (var qso in counted)
        {
            var band = (qso.Band ?? "").Trim().ToLowerInvariant();
            if (!DxccGlanceBandSet.Contains(band)) continue;
            var group = DxccGlanceModes.Group(qso.Mode);
            if (group is null) continue;
            var key = (band, group);
            if (!buckets.TryGetValue(key, out var list))
                buckets[key] = list = [];
            list.Add(qso);
        }

        var rows = new List<DxccGlanceRow>(3);
        foreach (var group in DxccGlanceModes.Groups)
        {
            var cells = new List<DxccGlanceCell>(columns.Count);
            foreach (var band in columns)
            {
                buckets.TryGetValue((band, group), out var hits);
                var count = hits?.Count ?? 0;
                // Stage 2 fills CallsignWorked. Stage 1 leaves it false.
                cells.Add(new DxccGlanceCell(
                    Worked: count > 0,
                    Confirmed: hits?.Any(IsConfirmed) == true,
                    QsoCount: count,
                    CallsignWorked: false));
            }
            rows.Add(new DxccGlanceRow(group, cells));
        }

        return new DxccEntityGlance(entity.Country, entity.Prefix, columns, rows);
    }

    private static List<string> GlanceColumns(List<Qso> personal)
    {
        if (personal.Count == 0)
            return [..DxccGlanceBands];

        var present = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var qso in personal)
        {
            var band = (qso.Band ?? "").Trim().ToLowerInvariant();
            if (DxccGlanceBandSet.Contains(band))
                present.Add(band);
        }
        return DxccGlanceBands.Where(present.Contains).ToList();
    }

    /// <summary>
    /// Personal QSOs that belong to this entity, using the same keys Statistics uses.
    /// An older row with an empty <see cref="Qso.Country"/> and a filled station country
    /// is painted as that station country. A row with both empty is dropped.
    /// </summary>
    private static List<Qso> QsosForEntity(List<Qso> personal, CtyEntity entity)
    {
        var eligible = personal.Where(q => !string.IsNullOrWhiteSpace(EffectiveCountry(q))).ToList();
        var byPrefix = new List<Qso>();
        var rest = new List<Qso>();
        foreach (var qso in eligible)
        {
            var prefix = CtyService.GetPrimaryPrefixFromCallsign(qso.Callsign ?? "");
            if (prefix != null && string.Equals(prefix, entity.Prefix, StringComparison.OrdinalIgnoreCase))
                byPrefix.Add(qso);
            else
                rest.Add(qso);
        }

        var codes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var qso in byPrefix)
        {
            var key = GlanceEntityKey(qso);
            if (key.StartsWith("dxcc:", StringComparison.Ordinal))
                codes.Add(key);
        }

        var countryKey = "country:" + entity.Country.ToUpperInvariant();
        var counted = new List<Qso>(byPrefix);
        foreach (var qso in rest)
        {
            var prefix = CtyService.GetPrimaryPrefixFromCallsign(qso.Callsign ?? "");
            var key = GlanceEntityKey(qso);
            if (prefix == null && string.Equals(key, countryKey, StringComparison.OrdinalIgnoreCase))
            {
                counted.Add(qso);
                continue;
            }
            if (key.StartsWith("dxcc:", StringComparison.Ordinal) && codes.Contains(key))
                counted.Add(qso);
        }
        return counted;
    }

    private static string? EffectiveCountry(Qso qso) =>
        !string.IsNullOrWhiteSpace(qso.Country) ? qso.Country
        : string.IsNullOrWhiteSpace(qso.Station?.Country) ? null
        : qso.Station!.Country;

    /// <summary>
    /// <see cref="GetEntityKey"/> with the station country substituted when the QSO
    /// country is empty. The snapshot itself is not mutated.
    /// </summary>
    private static string GlanceEntityKey(Qso qso)
    {
        if (!string.IsNullOrWhiteSpace(qso.Country) || string.IsNullOrWhiteSpace(qso.Station?.Country))
            return GetEntityKey(qso);

        return GetEntityKey(new Qso
        {
            Callsign = qso.Callsign,
            Dxcc = qso.Dxcc,
            Country = qso.Station.Country,
            Station = qso.Station,
        });
    }

    /// <summary>
    /// One DXCC entity for the Log Entry string, or null when the text is empty,
    /// still ambiguous, or not an entity. Does not change <see cref="CtyService.FindEntity"/>.
    /// </summary>
    internal static CtyEntity? ResolveGlanceEntity(string? callsign)
    {
        if (string.IsNullOrWhiteSpace(callsign)) return null;
        var call = callsign.Trim().ToUpperInvariant();
        if (call.Length == 0) return null;

        if (CtyService.ExactCalls.TryGetValue(call, out var exact))
            return exact;

        if (CallsignHelper.IsCompound(call) && !CallsignHelper.IsPrefixForm(call))
            return ResolveGlanceEntity(CallsignHelper.ExtractBaseCall(call));

        if (CallsignHelper.IsPrefixForm(call))
        {
            var parts = call.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0) return null;
            return CtyService.Prefixes.TryGetValue(parts[0], out var prefixed) ? prefixed : null;
        }

        var matches = new List<CtyEntity>();
        foreach (var (key, candidate) in CtyService.Prefixes)
        {
            if (CallForm(key).StartsWith(call, StringComparison.OrdinalIgnoreCase))
                matches.Add(candidate);
        }

        var primaries = matches
            .Select(e => e.Prefix)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (primaries.Count > 1) return null;
        if (primaries.Count == 1) return matches[0];

        var slash = LongestSlashEntity(call);
        if (slash != null) return slash;

        var (country, _, _) = CtyService.GetEntityFromCallsign(call);
        if (string.IsNullOrEmpty(country)) return null;
        var primary = CtyService.GetPrimaryPrefixFromCallsign(call);
        if (string.IsNullOrEmpty(primary)) return null;
        return CtyService.Prefixes.TryGetValue(primary, out var resolved) ? resolved : null;
    }

    /// <summary>A cty.dat key <c>AAA/x</c> matches a call that begins <c>AAAX</c>.</summary>
    private static string CallForm(string key)
    {
        var slash = key.IndexOf('/');
        return slash < 0 ? key : key.Remove(slash, 1);
    }

    private static CtyEntity? LongestSlashEntity(string call)
    {
        CtyEntity? best = null;
        var bestLength = 0;
        var tie = false;
        foreach (var (key, candidate) in CtyService.Prefixes)
        {
            if (key.IndexOf('/') < 0) continue;
            var form = CallForm(key);
            if (form.Length == 0 || !call.StartsWith(form, StringComparison.OrdinalIgnoreCase))
                continue;
            if (form.Length > bestLength)
            {
                best = candidate;
                bestLength = form.Length;
                tie = false;
            }
            else if (form.Length == bestLength
                     && best != null
                     && !string.Equals(best.Prefix, candidate.Prefix, StringComparison.OrdinalIgnoreCase))
            {
                tie = true;
            }
        }
        return tie ? null : best;
    }
}

/// <summary>
/// Phone, CW, and digital for the DXCC glance. Not <c>QrvQsoTotals.ModeBucket</c>:
/// a mode that is in none of these sets fills no cell.
/// </summary>
internal static class DxccGlanceModes
{
    internal static readonly string[] Groups = ["phone", "cw", "digital"];

    private static readonly HashSet<string> Phone = new(StringComparer.OrdinalIgnoreCase)
    {
        "SSB", "USB", "LSB", "AM", "FM", "SAM", "NFM", "DSB", "PH", "PHONE", "VOICE",
    };

    private static readonly HashSet<string> Cw = new(StringComparer.OrdinalIgnoreCase)
    {
        "CW", "CWU", "CWL", "CWR", "CW-R",
    };

    private static readonly HashSet<string> Digital = new(StringComparer.OrdinalIgnoreCase)
    {
        "DIGU", "DIGL", "FT8", "FT4", "JS8", "Q65", "MSK144", "RTTY", "PSK31", "WSPR", "JT65", "JT9", "DIGI",
        "PSK", "PSK63", "PSK125",
        "FSK", "FSK-R", "FSKR",
        "DATA-U", "PKT-U", "USB-D", "USBD", "DATA-L", "PKT-L", "LSB-D", "LSBD",
        "JT65A", "JT65B", "JT65C", "FST4",
        "DIG", "DATA", "DRM", "FT2", "FST4W", "HELL", "QPSK", "OLIVIA", "CONTESTIA", "MFSK",
        "DOMINOEX", "THOR", "MT63", "PACKET", "VARA", "VARAC", "VARAHF",
        "Q65C", "Q65D", "Q65E", "MSKMS", "JS8CX2", "RTTYR",
    };

    internal static string? Group(string? mode)
    {
        if (string.IsNullOrWhiteSpace(mode)) return null;
        var token = mode.Trim();
        if (Phone.Contains(token)) return "phone";
        if (Cw.Contains(token)) return "cw";
        if (Digital.Contains(token)) return "digital";
        return null;
    }
}
