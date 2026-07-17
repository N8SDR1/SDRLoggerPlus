using System.Globalization;
using System.Text;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Produces a Cabrillo 3.0 log for a contest session. Pure and I/O-free: given a
/// definition, session, QSOs, and station facts it returns the file text. The QSO
/// lines are built generically from the definition's sent/received exchange
/// fields (which include RST), so it works for any declarative contest without a
/// hand-authored column map.
/// </summary>
public static class CabrilloExporter
{
    public static string Generate(
        ContestDefinition def, ContestSession session, IReadOnlyList<Qso> qsos,
        string stationCallsign, string? gridSquare)
    {
        var me = session.MyExchange;
        var summary = ContestScoringEngine.Recompute(def, me, qsos.ToList());

        // Resolve the exchange fields for the operator's role: an in-state QSO-party
        // op sends its county, an out-of-state op sends its state, etc. Falls back to
        // the top-level fields for contests without a role split.
        var role = def.Roles?.GetValueOrDefault(session.Role);
        var sentFields = role?.SentExchange ?? def.SentExchange;
        var rcvdFields = role?.RcvdExchange ?? def.RcvdExchange;

        var sb = new StringBuilder();
        sb.AppendLine("START-OF-LOG: 3.0");
        sb.AppendLine($"CONTEST: {def.CabrilloName}");
        sb.AppendLine($"CALLSIGN: {stationCallsign.ToUpperInvariant()}");
        sb.AppendLine("CATEGORY-OPERATOR: SINGLE-OP");
        sb.AppendLine("CATEGORY-BAND: ALL");
        sb.AppendLine($"CATEGORY-MODE: {CategoryMode(def)}");
        sb.AppendLine($"CATEGORY-POWER: {(string.IsNullOrWhiteSpace(me.Power) ? "HIGH" : me.Power!.ToUpperInvariant())}");
        sb.AppendLine($"CLAIMED-SCORE: {summary.Score}");
        sb.AppendLine($"OPERATORS: {stationCallsign.ToUpperInvariant()}");
        if (!string.IsNullOrWhiteSpace(gridSquare))
            sb.AppendLine($"GRID-LOCATOR: {gridSquare.ToUpperInvariant()}");
        sb.AppendLine("CREATED-BY: SDRLoggerPlus");

        foreach (var qso in qsos)
            sb.AppendLine(QsoLine(sentFields, rcvdFields, me, qso, stationCallsign));

        sb.AppendLine("END-OF-LOG:");
        return sb.ToString();
    }

    private static string QsoLine(
        IReadOnlyList<ContestField> sentFields, IReadOnlyList<ContestField> rcvdFields,
        MyExchange me, Qso qso, string stationCallsign)
    {
        var freq = FreqKhz(qso);
        var mode = CabrilloMode(qso.Mode);
        var date = qso.QsoDate.ToUniversalTime().ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var time = Hhmm(qso.TimeOn);

        var sent = sentFields.Select(f => SentValue(f, me, qso));
        var rcvd = rcvdFields.Select(f => RcvdValue(f, qso));

        var tokens = new List<string>
        {
            "QSO:", freq, mode, date, time,
            stationCallsign.ToUpperInvariant().PadRight(13),
        };
        tokens.AddRange(sent.Select(v => v.PadRight(6)));
        tokens.Add((qso.Callsign ?? "").ToUpperInvariant().PadRight(13));
        tokens.AddRange(rcvd.Select(v => v.PadRight(6)));

        return string.Join(" ", tokens).TrimEnd();
    }

    private static string SentValue(ContestField f, MyExchange me, Qso qso)
    {
        // County is a plain Text field (no dedicated field type); emit it by key so
        // an in-area QSO-party operator sends its county rather than a blank token.
        if (f.Type == ContestFieldType.Text && f.Key.Equals("county", StringComparison.OrdinalIgnoreCase))
            return me.County ?? "";
        return f.Type switch
        {
            ContestFieldType.Rst => qso.RstSent ?? (IsCw(qso.Mode) ? "599" : "59"),
            ContestFieldType.Serial => qso.Contest?.SerialSent ?? "",
            ContestFieldType.Zone => me.CqZone?.ToString() ?? "",
            ContestFieldType.State => me.State ?? "",
            ContestFieldType.Section => me.Section ?? "",
            ContestFieldType.Name => me.Name ?? "",
            ContestFieldType.Grid => me.Grid ?? "",
            ContestFieldType.Power => me.Power ?? "",
            _ => "",
        };
    }

    private static string RcvdValue(ContestField f, Qso qso)
    {
        var c = qso.Contest;
        var typed = f.Type switch
        {
            ContestFieldType.Rst => qso.RstRcvd ?? (IsCw(qso.Mode) ? "599" : "59"),
            ContestFieldType.Serial => c?.SerialRcvd,
            ContestFieldType.Zone => c?.RcvdZone,
            ContestFieldType.State => c?.RcvdState,
            ContestFieldType.Section => c?.RcvdSection,
            ContestFieldType.Name => c?.RcvdName,
            ContestFieldType.Grid => c?.RcvdGrid,
            ContestFieldType.Power => c?.RcvdPower,
            _ => null,
        };
        // Fall back to the generic received-field store for non-typed exchange
        // fields (age, check, precedence, member#, IOTA ref, county, …).
        return typed ?? (c?.RcvdFields != null && c.RcvdFields.TryGetValue(f.Key, out var v) ? v : "");
    }

    private static string CategoryMode(ContestDefinition def)
    {
        if (def.Modes.Count != 1) return "MIXED";
        return CabrilloMode(def.Modes[0]) switch
        {
            "CW" => "CW",
            "PH" => "SSB",
            "RY" => "RTTY",
            _ => "MIXED",
        };
    }

    private static bool IsCw(string? mode) => (mode ?? "").ToUpperInvariant().StartsWith("CW");

    // Cabrillo mode tokens: CW / PH / RY / FM / DG.
    private static string CabrilloMode(string? mode)
    {
        var m = (mode ?? "").ToUpperInvariant();
        if (m.StartsWith("CW")) return "CW";
        if (m is "SSB" or "USB" or "LSB" or "PH" or "AM") return "PH";
        if (m is "RTTY" or "RY") return "RY";
        if (m == "FM" || m == "NFM") return "FM";
        return "DG"; // FT8/FT4/PSK/JT/DIGI/etc.
    }

    // Representative kHz per band when a QSO has no stored frequency.
    private static readonly Dictionary<string, int> BandKhz = new(StringComparer.OrdinalIgnoreCase)
    {
        ["160m"] = 1830, ["80m"] = 3530, ["40m"] = 7030, ["30m"] = 10120,
        ["20m"] = 14030, ["17m"] = 18080, ["15m"] = 21030, ["12m"] = 24900,
        ["10m"] = 28030, ["6m"] = 50100, ["2m"] = 144100,
    };

    private static string FreqKhz(Qso qso)
    {
        if (qso.Frequency is > 0)
            return ((int)Math.Round(qso.Frequency.Value)).ToString(CultureInfo.InvariantCulture);
        return BandKhz.TryGetValue(qso.Band ?? "", out var k) ? k.ToString(CultureInfo.InvariantCulture) : "0";
    }

    private static string Hhmm(string? timeOn)
    {
        var t = new string((timeOn ?? "").Where(char.IsDigit).ToArray());
        if (t.Length >= 4) return t[..4];
        return t.PadRight(4, '0');
    }
}
