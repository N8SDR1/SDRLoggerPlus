using System.Text.RegularExpressions;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Resolves a US state for WAS/5BWAS counting, ported from SDRLogger+:
/// state field (code or full name) → QTH extraction → Alaska/Hawaii entity
/// mapping. Whatever produced the state, the QSO's country must be
/// United States / Alaska / Hawaii — a "TX" pulled from a non-US QTH
/// must not count toward Texas.
/// </summary>
public static partial class UsStateResolver
{
    private static readonly HashSet<string> StateCodes = new(StringComparer.Ordinal)
    {
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
        "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
        "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
        "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    };

    private static readonly Dictionary<string, string> StateNames = new(StringComparer.Ordinal)
    {
        ["ALABAMA"] = "AL", ["ALASKA"] = "AK", ["ARIZONA"] = "AZ", ["ARKANSAS"] = "AR",
        ["CALIFORNIA"] = "CA", ["COLORADO"] = "CO", ["CONNECTICUT"] = "CT", ["DELAWARE"] = "DE",
        ["FLORIDA"] = "FL", ["GEORGIA"] = "GA", ["HAWAII"] = "HI", ["IDAHO"] = "ID",
        ["ILLINOIS"] = "IL", ["INDIANA"] = "IN", ["IOWA"] = "IA", ["KANSAS"] = "KS",
        ["KENTUCKY"] = "KY", ["LOUISIANA"] = "LA", ["MAINE"] = "ME", ["MARYLAND"] = "MD",
        ["MASSACHUSETTS"] = "MA", ["MICHIGAN"] = "MI", ["MINNESOTA"] = "MN", ["MISSISSIPPI"] = "MS",
        ["MISSOURI"] = "MO", ["MONTANA"] = "MT", ["NEBRASKA"] = "NE", ["NEVADA"] = "NV",
        ["NEW HAMPSHIRE"] = "NH", ["NEW JERSEY"] = "NJ", ["NEW MEXICO"] = "NM", ["NEW YORK"] = "NY",
        ["NORTH CAROLINA"] = "NC", ["NORTH DAKOTA"] = "ND", ["OHIO"] = "OH", ["OKLAHOMA"] = "OK",
        ["OREGON"] = "OR", ["PENNSYLVANIA"] = "PA", ["RHODE ISLAND"] = "RI", ["SOUTH CAROLINA"] = "SC",
        ["SOUTH DAKOTA"] = "SD", ["TENNESSEE"] = "TN", ["TEXAS"] = "TX", ["UTAH"] = "UT",
        ["VERMONT"] = "VT", ["VIRGINIA"] = "VA", ["WASHINGTON"] = "WA", ["WEST VIRGINIA"] = "WV",
        ["WISCONSIN"] = "WI", ["WYOMING"] = "WY",
    };

    private static readonly HashSet<string> UsEntities = new(StringComparer.OrdinalIgnoreCase)
    {
        "United States", "Alaska", "Hawaii",
    };

    [GeneratedRegex(@"[,\s/]+")]
    private static partial Regex QthTokenSplit();

    public static string? Resolve(string? stateField, string? qth, string? country)
    {
        if (country == null || !UsEntities.Contains(country))
            return null;

        var state = FromStateField(stateField) ?? FromQth(qth);
        if (state != null)
            return state;

        // Alaska and Hawaii are their own DXCC entities — auto-map them
        if (string.Equals(country, "Alaska", StringComparison.OrdinalIgnoreCase)) return "AK";
        if (string.Equals(country, "Hawaii", StringComparison.OrdinalIgnoreCase)) return "HI";
        return null;
    }

    private static string? FromStateField(string? stateField)
    {
        var raw = stateField?.Trim().ToUpperInvariant();
        if (string.IsNullOrEmpty(raw)) return null;
        if (StateCodes.Contains(raw)) return raw;
        return StateNames.TryGetValue(raw, out var code) ? code : null;
    }

    private static string? FromQth(string? qth)
    {
        if (string.IsNullOrWhiteSpace(qth)) return null;
        var upper = qth.Trim().ToUpperInvariant();

        foreach (var token in QthTokenSplit().Split(upper))
        {
            if (StateCodes.Contains(token.Trim()))
                return token.Trim();
        }

        foreach (var (name, code) in StateNames)
        {
            if (upper.Contains(name))
                return code;
        }
        return null;
    }
}
