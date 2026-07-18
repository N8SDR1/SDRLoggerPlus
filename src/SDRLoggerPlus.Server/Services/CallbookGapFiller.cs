using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Backfills a <see cref="CreateQsoRequest"/> from callbook data for QSOs that
/// arrive without an operator at the keyboard — currently WSJT-X / JTDX / MSHV
/// auto-logging.
///
/// Strictly gap-filling: a value the logging source supplied always wins and is
/// never replaced. WSJT-X sends the grid the station actually transmitted in the
/// FT8 exchange, which is authoritative for that QSO and beats a callbook entry
/// that may be stale or wrong for a portable/rover operation. The callbook only
/// supplies fields the source left empty (typically QTH and name, which the
/// WSJT-X protocol has no field for).
/// </summary>
public static class CallbookGapFiller
{
    /// <summary>
    /// Merge callbook values into <paramref name="request"/>, filling only the
    /// fields that are null/blank. Returns the request unchanged when the
    /// callbook has nothing to add.
    /// </summary>
    public static CreateQsoRequest Fill(
        CreateQsoRequest request,
        string? name = null,
        string? city = null,
        string? state = null,
        string? grid = null,
        string? country = null)
    {
        return request with
        {
            Name = Keep(request.Name) ?? Clean(name),
            Grid = Keep(request.Grid) ?? Clean(grid),
            Country = Keep(request.Country) ?? Clean(country),
            Qth = Keep(request.Qth) ?? ComposeQth(city, state),
        };
    }

    /// <summary>True when the request still has a field the callbook could fill.</summary>
    public static bool HasGap(CreateQsoRequest request) =>
        Keep(request.Name) is null
        || Keep(request.Grid) is null
        || Keep(request.Country) is null
        || Keep(request.Qth) is null;

    // "Green Bay, WI" from whichever parts the callbook returned; null when neither.
    private static string? ComposeQth(string? city, string? state)
    {
        var parts = new[] { Clean(city), Clean(state) }.Where(p => p is not null).ToArray();
        return parts.Length == 0 ? null : string.Join(", ", parts);
    }

    private static string? Keep(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;
    private static string? Clean(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
