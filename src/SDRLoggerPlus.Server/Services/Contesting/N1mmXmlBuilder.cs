using System.Globalization;
using System.Text;
using System.Xml;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Builds N1MM-compatible XML datagrams: <c>&lt;contactinfo&gt;</c> (one per QSO)
/// and <c>&lt;dynamicresults&gt;</c> (running score). These are consumed by
/// external scoreboards (contestonlinescore.com), SO2R helpers, and loggers like
/// DXLog. Pure and I/O-free so the payloads can be unit-tested exactly.
/// </summary>
public static class N1mmXmlBuilder
{
    public static string ContactInfo(ContestDefinition def, ContestSession session, Qso qso, string myCall)
    {
        var (sb, w) = Start();
        w.WriteStartElement("contactinfo");
        El(w, "app", "SDRLoggerPlus");
        El(w, "contestname", def.CabrilloName);
        El(w, "contestnr", "1");
        El(w, "timestamp", Timestamp(qso));
        El(w, "mycall", myCall.ToUpperInvariant());
        El(w, "band", BandMhz(qso).ToString(CultureInfo.InvariantCulture));
        El(w, "rxfreq", FreqTensOfHz(qso));
        El(w, "txfreq", FreqTensOfHz(qso));
        El(w, "operator", myCall.ToUpperInvariant());
        El(w, "mode", CabrilloMode(qso.Mode));
        El(w, "call", (qso.Callsign ?? "").ToUpperInvariant());
        El(w, "countryprefix", qso.Country ?? "");
        El(w, "wpxprefix", WpxPrefixExtractor.Extract(qso.Callsign ?? "") ?? "");
        El(w, "continent", qso.Continent ?? "");
        El(w, "snt", qso.RstSent ?? "");
        El(w, "sntnr", qso.Contest?.SerialSent ?? "");
        El(w, "rcv", qso.RstRcvd ?? "");
        El(w, "rcvnr", qso.Contest?.SerialRcvd ?? "");
        El(w, "gridsquare", qso.Contest?.RcvdGrid ?? qso.Grid ?? "");
        El(w, "section", qso.Contest?.RcvdSection ?? "");
        El(w, "name", qso.Contest?.RcvdName ?? "");
        El(w, "power", qso.Contest?.RcvdPower ?? "");
        El(w, "zone", qso.Contest?.RcvdZone ?? qso.Station?.CqZone?.ToString() ?? "");
        El(w, "points", (qso.Contest?.QsoPoints ?? 0).ToString(CultureInfo.InvariantCulture));
        El(w, "ismultiplier1", (qso.Contest?.Mults is { Count: > 0 }) ? "1" : "0");
        El(w, "radionr", "1");
        El(w, "isoriginal", "True");
        El(w, "isclaimedqso", qso.Contest?.IsDupe == true ? "0" : "1");
        w.WriteEndElement();
        return Finish(sb, w);
    }

    public static string DynamicResults(ContestDefinition def, ContestStateDto state, string myCall)
    {
        var (sb, w) = Start();
        w.WriteStartElement("dynamicresults");
        El(w, "contest", def.CabrilloName);
        El(w, "call", myCall.ToUpperInvariant());
        El(w, "ops", myCall.ToUpperInvariant());
        El(w, "qsos", state.Qsos.ToString(CultureInfo.InvariantCulture));
        El(w, "points", state.Points.ToString(CultureInfo.InvariantCulture));
        El(w, "mults", state.Multipliers.ToString(CultureInfo.InvariantCulture));
        El(w, "score", state.Score.ToString(CultureInfo.InvariantCulture));
        El(w, "timestamp", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        w.WriteEndElement();
        return Finish(sb, w);
    }

    // -- helpers ------------------------------------------------------------

    // Write to a UTF-8 stream so the XML declaration matches the bytes we send
    // over UDP/HTTP (writing to a StringBuilder would force encoding="utf-16").
    private static readonly UTF8Encoding Utf8 = new(encoderShouldEmitUTF8Identifier: false);

    private static (MemoryStream, XmlWriter) Start()
    {
        var ms = new MemoryStream();
        var w = XmlWriter.Create(ms, new XmlWriterSettings { Encoding = Utf8, OmitXmlDeclaration = false, Indent = false });
        return (ms, w);
    }

    private static string Finish(MemoryStream ms, XmlWriter w)
    {
        w.Flush();
        w.Close();
        return Utf8.GetString(ms.ToArray());
    }

    private static void El(XmlWriter w, string name, string value)
    {
        w.WriteStartElement(name);
        w.WriteString(value);
        w.WriteEndElement();
    }

    private static string Timestamp(Qso qso)
    {
        var t = new string((qso.TimeOn ?? "").Where(char.IsDigit).ToArray()).PadRight(6, '0');
        var hh = t[..2]; var mm = t.Substring(2, 2); var ss = t.Substring(4, 2);
        return $"{qso.QsoDate.ToUniversalTime():yyyy-MM-dd} {hh}:{mm}:{ss}";
    }

    private static int BandMhz(Qso qso)
    {
        if (qso.Frequency is > 0) return (int)(qso.Frequency.Value / 1000.0);
        var digits = new string((qso.Band ?? "").Where(char.IsDigit).ToArray());
        return int.TryParse(digits, out var m) ? m : 0;
    }

    // N1MM expresses frequency in tens of Hz. Qso.Frequency is kHz → ×100.
    private static string FreqTensOfHz(Qso qso)
        => qso.Frequency is > 0 ? ((long)(qso.Frequency.Value * 100)).ToString(CultureInfo.InvariantCulture) : "0";

    private static string CabrilloMode(string? mode)
    {
        var m = (mode ?? "").ToUpperInvariant();
        if (m.StartsWith("CW")) return "CW";
        if (m is "SSB" or "USB" or "LSB" or "PH" or "AM") return "USB";
        if (m is "RTTY" or "RY") return "RTTY";
        return m.Length > 0 ? m : "CW";
    }
}
