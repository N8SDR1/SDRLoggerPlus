using System.Text.RegularExpressions;

namespace SDRLoggerPlus.Server.Services.Wsjtx;

/// <summary>
/// The station and grid extracted from a decoded FT8/FT4 message line.
/// <see cref="Callsign"/> is the TRANSMITTING station (the one we decoded);
/// <see cref="DxCall"/> is who they were calling (null for a CQ);
/// <see cref="Grid"/> is the 4-char Maidenhead locator when the message carries
/// one (CQ and grid-reply messages), else null.
/// </summary>
public record WsjtxDecodeParse(string? Callsign, string? DxCall, string? Grid, bool IsCq);

/// <summary>
/// Parses the free-form text of a WSJT-X Decode message into the transmitting
/// callsign + optional grid. FT8/FT4 messages follow a small grammar:
///   • "CQ [dir] CALL [GRID]"      — CALL is the transmitter (available to work)
///   • "TOCALL DECALL GRID|REPORT" — DECALL (2nd) is the transmitter
/// Directional CQ tokens (DX, NA, POTA, a directed frequency, …) sit between CQ
/// and the callsign and are skipped. Returns null for free text / telemetry that
/// carries no callsign.
/// </summary>
public static class WsjtxDecodeParser
{
    // A 4-char Maidenhead field+square, e.g. FN42. (Grids in FT8 messages are 4 chars.)
    private static readonly Regex GridRe = new(@"^[A-R]{2}[0-9]{2}$", RegexOptions.Compiled);
    // A signal report, e.g. -15, +03, R-07.
    private static readonly Regex ReportRe = new(@"^R?[+-][0-9]{2}$", RegexOptions.Compiled);
    // Roger / sign-off tokens that are neither callsign nor grid.
    private static readonly HashSet<string> Rogers = new(StringComparer.Ordinal)
    {
        "RR73", "RRR", "73", "RR", "TU", "R", "DX", "QRZ",
    };

    public static WsjtxDecodeParse? Parse(string? message)
    {
        if (string.IsNullOrWhiteSpace(message)) return null;

        // Uppercase, strip hashed-call angle brackets ("<K1ABC>" -> "K1ABC").
        var tokens = message.ToUpperInvariant()
            .Replace("<", "").Replace(">", "")
            .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length == 0) return null;

        if (tokens[0] == "CQ")
        {
            // Skip CQ and any directional tokens; the first callsign after is the caller.
            for (var i = 1; i < tokens.Length; i++)
            {
                if (!IsCallsign(tokens[i])) continue;
                var grid = i + 1 < tokens.Length && IsGrid(tokens[i + 1]) ? tokens[i + 1] : null;
                return new WsjtxDecodeParse(tokens[i], DxCall: null, Grid: grid, IsCq: true);
            }
            return null;
        }

        // Standard exchange: TOCALL DECALL [GRID|REPORT]. The 2nd call transmits.
        if (tokens.Length >= 2 && IsCallsign(tokens[0]) && IsCallsign(tokens[1]))
        {
            var grid = tokens.Length >= 3 && IsGrid(tokens[2]) ? tokens[2] : null;
            return new WsjtxDecodeParse(tokens[1], DxCall: tokens[0], Grid: grid, IsCq: false);
        }

        return null;
    }

    // "RR73" pattern-matches a Maidenhead grid (R,R,7,3) but in an FT8 message
    // it's a roger sign-off, so roger tokens are never treated as a grid.
    private static bool IsGrid(string t) => GridRe.IsMatch(t) && !Rogers.Contains(t);

    private static bool IsCallsign(string t)
    {
        if (Rogers.Contains(t) || IsGrid(t) || ReportRe.IsMatch(t)) return false;
        if (t.Length < 3) return false;
        // Only letters/digits/slash, and a real call has BOTH a letter and a digit
        // (this alone rejects grids like FN42? no — so IsGrid is checked above).
        var hasLetter = false;
        var hasDigit = false;
        foreach (var c in t)
        {
            if (c >= 'A' && c <= 'Z') hasLetter = true;
            else if (c >= '0' && c <= '9') hasDigit = true;
            else if (c != '/') return false;
        }
        return hasLetter && hasDigit;
    }
}
