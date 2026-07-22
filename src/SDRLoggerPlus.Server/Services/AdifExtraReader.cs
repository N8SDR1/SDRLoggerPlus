using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Reads ADIF fields that landed in <see cref="Qso.AdifExtra"/> rather than on
/// a typed property.
///
/// Two awards depend on this. County data sits in <c>cnty</c> because ADIF
/// import never mapped the field, and satellite data sits in <c>SAT_NAME</c> /
/// <c>PROP_MODE</c> because there is no satellite property on Qso at all.
///
/// The lookup is case-insensitive by necessity, not caution: ADIF field names
/// are case-insensitive by spec, imported rows carry them lower-cased
/// ("cnty", "sat_name") while the spec and our own writers use upper, so an
/// exact-match lookup finds nothing on real logs.
/// </summary>
public static class AdifExtraReader
{
    public static string? Read(Qso qso, string name)
    {
        var extra = qso.AdifExtra;
        if (extra == null) return null;

        foreach (var element in extra.Elements)
        {
            if (!string.Equals(element.Name, name, StringComparison.OrdinalIgnoreCase)) continue;
            var value = element.Value;
            if (value == null || value.IsBsonNull) return null;
            // Import stringifies extras, but older rows may hold other types.
            return value.IsString ? value.AsString : value.ToString();
        }
        return null;
    }

    /// <summary>Read trimmed, treating blank as absent.</summary>
    public static string? ReadTrimmed(Qso qso, string name)
    {
        var value = Read(qso, name);
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
