using System.Globalization;

namespace SDRLoggerPlus.Server.Services.Flex;

/// <summary>
/// Pure (I/O-free) helpers for the FlexRadio 6000 SmartSDR Ethernet API, so the wire
/// logic is unit-testable without a radio. Grounded in FlexRadio's official
/// smartsdr-api-docs wiki:
///   - Discovery: UDP broadcast, VITA-49 packet whose payload is the string
///     "model=%s serial=%s version=%s name=%s callsign=%s ip=%u.%u.%u.%u port=%u"
///     (spaces in name are '_'). We LOCATE the payload by scanning for "model=" rather
///     than trusting an exact VITA header offset — robust across firmware framing.
///   - Command:  "C<seq>|<command>\n"  → response "R<seq>|<hex>|<msg>"
///   - Status:   "S<handle>|<message>" (async object updates, incl. slices)
///   - Tune:     "slice t <rx> <MHz>"      Mode: "slice s <rx> mode=<MODE>"
///
/// HARDWARE-UNVERIFIED (no Flex on the bench): the slice-status FIELD NAMES
/// (RF_frequency / mode / active / in_use) are inferred from FlexLib convention, and
/// the discovery UDP port (docs say 4991 vs 4992) — both flagged where used. Verify
/// on a real 6000-series radio.
/// </summary>
public static class FlexProtocol
{
    public const int CommandPort = 4992; // TCP command/status socket

    /// <summary>A radio as announced by the discovery broadcast.</summary>
    public sealed record FlexRadioInfo(
        string Model, string Serial, string Version, string Name, string Callsign, string Ip, int Port);

    /// <summary>A parsed slice status snapshot.</summary>
    public sealed record FlexSliceStatus(
        int Index, long? FrequencyHz, string? Mode, bool Active, bool InUse);

    // -- discovery ----------------------------------------------------------

    /// <summary>
    /// Find the ASCII discovery payload ("model=…") anywhere in a received datagram,
    /// skipping the binary VITA-49 header. Returns null if no payload marker is found.
    /// </summary>
    public static string? FindDiscoveryPayload(byte[] datagram, int length)
    {
        if (datagram is null || length <= 0) return null;
        // Decode the printable region as ASCII and locate the "model=" marker.
        var text = System.Text.Encoding.ASCII.GetString(datagram, 0, Math.Min(length, datagram.Length));
        var idx = text.IndexOf("model=", StringComparison.Ordinal);
        if (idx < 0) return null;
        // Trim at the first NUL/control terminator after the marker.
        var end = text.IndexOf('\0', idx);
        return end < 0 ? text[idx..] : text[idx..end];
    }

    /// <summary>Parse the space-separated key=value discovery payload.</summary>
    public static FlexRadioInfo? ParseDiscoveryPayload(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload)) return null;
        var kv = ParseKeyValues(payload);
        if (!kv.TryGetValue("model", out var model) || !kv.TryGetValue("serial", out var serial))
            return null;
        kv.TryGetValue("ip", out var ip);
        var port = kv.TryGetValue("port", out var portStr) && int.TryParse(portStr, out var p) ? p : CommandPort;
        // name has spaces encoded as underscores for the packet — restore them.
        var name = kv.TryGetValue("name", out var n) ? n.Replace('_', ' ') : model;
        kv.TryGetValue("version", out var version);
        kv.TryGetValue("callsign", out var callsign);
        return new FlexRadioInfo(model, serial, version ?? "", name, callsign ?? "",
            ip ?? "", port <= 0 ? CommandPort : port);
    }

    /// <summary>radioId for a discovered/connected Flex — stable per serial.</summary>
    public static string RadioId(string serial) => $"flex-{serial}";

    // -- commands -----------------------------------------------------------

    /// <summary>"C&lt;seq&gt;|slice t &lt;rx&gt; &lt;MHz&gt;" — tune a slice. Frequency is MHz.</summary>
    public static string TuneCommand(int seq, int sliceRx, long frequencyHz)
        => $"C{seq}|slice t {sliceRx} {HzToMhz(frequencyHz)}";

    /// <summary>"C&lt;seq&gt;|slice s &lt;rx&gt; mode=&lt;MODE&gt;" — set a slice's mode.</summary>
    public static string ModeCommand(int seq, int sliceRx, string appMode)
        => $"C{seq}|slice s {sliceRx} mode={MapAppModeToFlex(appMode)}";

    /// <summary>"C&lt;seq&gt;|sub slice all" — subscribe to slice status updates.</summary>
    public static string SubscribeSlicesCommand(int seq) => $"C{seq}|sub slice all";

    /// <summary>MHz string with Hz precision (6 dp) for the tune command.</summary>
    public static string HzToMhz(long hz) => (hz / 1_000_000.0).ToString("0.000000", CultureInfo.InvariantCulture);

    // -- status parsing -----------------------------------------------------

    /// <summary>
    /// Parse a slice status body of the form "slice &lt;index&gt; key=value key=value …".
    /// <paramref name="statusBody"/> is the part AFTER "S&lt;handle&gt;|". Returns null if it
    /// isn't a slice status. FIELD NAMES are the FlexLib convention (unverified on HW).
    /// </summary>
    public static FlexSliceStatus? ParseSliceStatus(string? statusBody)
    {
        if (string.IsNullOrWhiteSpace(statusBody)) return null;
        var s = statusBody.Trim();
        if (!s.StartsWith("slice ", StringComparison.Ordinal)) return null;
        var rest = s["slice ".Length..];
        // First token is the slice index; the remainder is key=value pairs.
        var sp = rest.IndexOf(' ');
        var idxToken = sp < 0 ? rest : rest[..sp];
        if (!int.TryParse(idxToken, out var index)) return null;
        var kv = sp < 0 ? new Dictionary<string, string>() : ParseKeyValues(rest[(sp + 1)..]);

        long? freqHz = null;
        if (kv.TryGetValue("RF_frequency", out var rf) &&
            double.TryParse(rf, NumberStyles.Float, CultureInfo.InvariantCulture, out var mhz))
            freqHz = (long)Math.Round(mhz * 1_000_000.0);

        string? mode = kv.TryGetValue("mode", out var m) ? MapFlexModeToApp(m) : null;
        var active = kv.TryGetValue("active", out var a) && a == "1";
        var inUse = !kv.TryGetValue("in_use", out var iu) || iu == "1"; // absent ⇒ assume in use
        return new FlexSliceStatus(index, freqHz, mode, active, inUse);
    }

    // -- mode mapping -------------------------------------------------------

    /// <summary>App-normalized mode → Flex slice mode token.</summary>
    public static string MapAppModeToFlex(string? appMode) => (appMode ?? "").ToUpperInvariant() switch
    {
        "USB" => "USB",
        "LSB" => "LSB",
        "CW" or "CWU" or "CWL" => "CW",
        "AM" => "AM",
        "SAM" => "SAM",
        "FM" or "NFM" => "FM",
        "DIGU" or "FT8" or "FT4" or "DATA-U" or "USB-D" => "DIGU",
        "DIGL" or "DATA-L" or "LSB-D" => "DIGL",
        "RTTY" => "RTTY",
        "FDV" or "FREEDV" => "FDV",
        _ => (appMode ?? "USB").ToUpperInvariant(),
    };

    /// <summary>Flex slice mode token → app-normalized mode.</summary>
    public static string MapFlexModeToApp(string? flexMode) => (flexMode ?? "").ToUpperInvariant() switch
    {
        "USB" => "USB",
        "LSB" => "LSB",
        "CW" => "CW",
        "AM" => "AM",
        "SAM" => "SAM",
        "FM" => "FM",
        "DIGU" => "DIGU",
        "DIGL" => "DIGL",
        "RTTY" => "RTTY",
        "FDV" => "FDV",
        _ => (flexMode ?? "").ToUpperInvariant(),
    };

    // -- helpers ------------------------------------------------------------

    private static Dictionary<string, string> ParseKeyValues(string s)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var tok in s.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = tok.IndexOf('=');
            if (eq <= 0) continue;
            dict[tok[..eq]] = tok[(eq + 1)..];
        }
        return dict;
    }
}
