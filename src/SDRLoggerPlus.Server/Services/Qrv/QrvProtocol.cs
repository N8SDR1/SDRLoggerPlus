using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// QRV board wire format from qrv_client.md. Presence only: call, band, mode,
/// app "Logger", and the server-issued install key. Hz and notes are not encoded.
/// </summary>
public static class QrvProtocol
{
    public const string AppName = "Logger";
    public const string DefaultApiBase = "https://qrvsdr.cc";
    public const string DefaultConfiguredBase = "https://qrvsdr.cc/v1";

    public static readonly TimeSpan HeartbeatPeriod = TimeSpan.FromSeconds(25);
    public static readonly TimeSpan MinPostInterval = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan BandModeDebounce = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan RosterPeriod = TimeSpan.FromSeconds(25);
    public static readonly TimeSpan MinRosterInterval = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan RosterWatchTtl = TimeSpan.FromSeconds(45);
    public static readonly TimeSpan HttpTimeout = TimeSpan.FromSeconds(12);

    private static readonly Regex CallPattern = new(
        "^[A-Z0-9]{3,11}(/[A-Z0-9]{1,5})?$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    // Exact tokens qrv_norm_mode() stores, plus the two aliases Logger is told to POST
    // (CW and DIG). The server rewrites those; clients must not invent anything else.
    private static readonly HashSet<string> SendableModes = new(StringComparer.Ordinal)
    {
        "LSB", "USB", "CWL", "CWU", "DSB", "AM", "SAM", "FM", "DIGU", "DIGL", "SPEC", "DRM",
        "SSB", "NFM", "PSK31", "JT65", "JT9", "DIGI", "DATA", "VARAC", "PACKET", "RTTYR",
        "FT8", "FT4", "FT2",
        "JT65A", "JT65B", "JT65C",
        "Q65", "Q65C", "Q65D", "Q65E",
        "MSK144", "MSKMS",
        "JS8", "JS8CX2",
        "WSPR", "FST4", "FST4W",
        "HELL", "RTTY", "PSK", "QPSK",
        "OLIVIA", "CONTESTIA", "MFSK", "DOMINOEX", "THOR", "MT63",
        "VARA", "VARAHF",
        "CW", "DIG",
    };

    private static readonly (long Lo, long Hi, string Name)[] HamBands =
    {
        (1_800_000, 2_000_000, "160m"),
        (3_500_000, 4_000_000, "80m"),
        (5_330_000, 5_407_000, "60m"),
        (7_000_000, 7_300_000, "40m"),
        (10_100_000, 10_150_000, "30m"),
        (14_000_000, 14_350_000, "20m"),
        (18_068_000, 18_168_000, "17m"),
        (21_000_000, 21_450_000, "15m"),
        (24_890_000, 24_990_000, "12m"),
        (28_000_000, 29_700_000, "10m"),
        (50_000_000, 54_000_000, "6m"),
        (144_000_000, 148_000_000, "2m"),
        (420_000_000, 450_000_000, "70cm"),
    };

    public static string? NormalizeCall(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        var call = raw.Trim().ToUpperInvariant().Replace(" ", "");
        if (call.Length is < 3 or > 16) return null;
        if (!CallPattern.IsMatch(call)) return null;
        var slash = call.IndexOf('/');
        var baseCall = slash < 0 ? call : call[..slash];
        var hasLetter = false;
        var hasDigit = false;
        foreach (var c in baseCall)
        {
            if (char.IsLetter(c)) hasLetter = true;
            if (char.IsDigit(c)) hasDigit = true;
        }
        return hasLetter && hasDigit ? call : null;
    }

    public static string NormalizeApiBase(string? raw)
    {
        var baseUrl = (raw ?? "").Trim();
        while (baseUrl.EndsWith('/'))
            baseUrl = baseUrl[..^1];
        if (baseUrl.EndsWith("/v1", StringComparison.OrdinalIgnoreCase))
            baseUrl = baseUrl[..^3];
        while (baseUrl.EndsWith('/'))
            baseUrl = baseUrl[..^1];
        if (baseUrl.Length == 0
            || (!baseUrl.StartsWith("https://", StringComparison.OrdinalIgnoreCase)
                && !baseUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase)))
        {
            return DefaultApiBase;
        }
        return baseUrl;
    }

    public static string EndpointUrl(string? apiBase, string phpFile) =>
        NormalizeApiBase(apiBase) + "/v1/" + phpFile;

    /// <summary>Ham band first, then GEN for 1 Hz through 54 MHz. Null when there is no honest band.</summary>
    public static string? BandForHz(long hz)
    {
        if (hz <= 0) return null;
        foreach (var (lo, hi, name) in HamBands)
        {
            if (hz >= lo && hz <= hi) return name;
        }
        if (hz >= 1 && hz <= 54_000_000) return "GEN";
        return null;
    }

    /// <summary>
    /// Logger Mode select (and the rig names Logger already folds into it) → the token to POST.
    /// Null when the board has no token. Never substitutes DIGU for an unknown mode.
    /// </summary>
    public static string? ModeForLogger(string? mode)
    {
        if (string.IsNullOrWhiteSpace(mode)) return null;
        var upper = mode.Trim().ToUpperInvariant();

        // Hyphenated rig names whose stripped form is not the contract token.
        switch (upper)
        {
            case "CW-R":
            case "CWR":
                return "CWL";
            case "FSK-R":
            case "FSKR":
                return "RTTYR";
            case "FSK":
                return "RTTY";
            case "DATA-U":
            case "PKT-U":
            case "USB-D":
            case "USBD":
                return "DIGU";
            case "DATA-L":
            case "PKT-L":
            case "LSB-D":
            case "LSBD":
                return "DIGL";
        }

        var token = new string(upper.Where(static c => c is >= 'A' and <= 'Z' or >= '0' and <= '9').ToArray());
        if (token.Length == 0) return null;

        if (token == "VARAHFSTD") token = "VARAHF";
        else if (token == "JS8CX2EXPERIMENTS") token = "JS8CX2";

        if (token.Length is < 1 or > 12) return null;
        return SendableModes.Contains(token) ? token : null;
    }

    public static string EncodeHeartbeat(string call, string band, string mode, string? installKey)
    {
        var obj = new JsonObject
        {
            ["call"] = call,
            ["app"] = AppName,
            ["band"] = band,
            ["mode"] = mode,
        };
        if (!string.IsNullOrEmpty(installKey))
            obj["install_key"] = installKey;
        return obj.ToJsonString();
    }

    public static string EncodeQrt(string call, string installKey)
    {
        var obj = new JsonObject
        {
            ["call"] = call,
            ["app"] = AppName,
            ["install_key"] = installKey,
        };
        return obj.ToJsonString();
    }

    public static QrvWriteResult ParseWrite(string? body, int status)
    {
        if (string.IsNullOrWhiteSpace(body))
            return new QrvWriteResult(false, status, status > 0 ? $"http_{status}" : "QRV unavailable", null, 180);

        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return new QrvWriteResult(false, status, status > 0 ? $"http_{status}" : "invalid_json", null, 180);

            var root = doc.RootElement;
            var ok = root.TryGetProperty("ok", out var okEl) && okEl.ValueKind == JsonValueKind.True;
            var error = ReadString(root, "error");
            var key = ReadString(root, "install_key");
            var stale = 180;
            if (root.TryGetProperty("stale_s", out var staleEl) && staleEl.TryGetInt32(out var staleValue))
                stale = staleValue;
            if (!ok && string.IsNullOrEmpty(error))
                error = status > 0 ? $"http_{status}" : "QRV unavailable";
            return new QrvWriteResult(ok, status, error, key, stale);
        }
        catch (JsonException)
        {
            return new QrvWriteResult(false, status, status > 0 ? $"http_{status}" : "invalid_json", null, 180);
        }
    }

    public static QrvRosterResult ParseRoster(string? body, int status)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return new QrvRosterResult(
                false, status, status > 0 ? $"http_{status}" : "QRV unavailable", 180, Array.Empty<QrvRosterEntry>());
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return new QrvRosterResult(
                    false, status, status > 0 ? $"http_{status}" : "invalid_json", 180, Array.Empty<QrvRosterEntry>());
            }

            var root = doc.RootElement;
            var ok = root.TryGetProperty("ok", out var okEl) && okEl.ValueKind == JsonValueKind.True;
            var error = ReadString(root, "error");
            var stale = 180;
            if (root.TryGetProperty("stale_s", out var staleEl) && staleEl.TryGetInt32(out var staleValue))
                stale = staleValue;

            var entries = new List<QrvRosterEntry>();
            if (root.TryGetProperty("entries", out var arr) && arr.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in arr.EnumerateArray())
                {
                    if (item.ValueKind != JsonValueKind.Object) continue;
                    long? hz = null;
                    if (item.TryGetProperty("hz", out var hzEl) && hzEl.ValueKind == JsonValueKind.Number && hzEl.TryGetInt64(out var hzValue))
                        hz = hzValue;
                    entries.Add(new QrvRosterEntry(
                        ReadString(item, "call") ?? "",
                        ReadString(item, "app") ?? "",
                        ReadString(item, "band") ?? "",
                        ReadString(item, "mode") ?? "",
                        ReadString(item, "last_seen"),
                        hz,
                        ReadString(item, "note")));
                }
            }

            if (!ok && string.IsNullOrEmpty(error))
                error = status > 0 ? $"http_{status}" : "QRV unavailable";
            return new QrvRosterResult(ok, status, error, stale, entries);
        }
        catch (JsonException)
        {
            return new QrvRosterResult(
                false, status, status > 0 ? $"http_{status}" : "invalid_json", 180, Array.Empty<QrvRosterEntry>());
        }
    }

    public static bool IsInstallKey(string? key)
    {
        if (key is not { Length: 64 }) return false;
        foreach (var c in key)
        {
            if (c is not (>= '0' and <= '9' or >= 'a' and <= 'f' or >= 'A' and <= 'F'))
                return false;
        }
        return true;
    }

    public static string? KeyPrefix(string? key)
    {
        if (string.IsNullOrEmpty(key)) return null;
        return key.Length <= 8 ? key : key[..8] + "...";
    }

    private static string? ReadString(JsonElement obj, string name)
    {
        if (!obj.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String)
            return null;
        return value.GetString();
    }
}

public sealed record QrvWriteResult(bool Ok, int Status, string? Error, string? InstallKey, int StaleSeconds);

public sealed record QrvRosterEntry(
    string Call,
    string App,
    string Band,
    string Mode,
    string? LastSeen,
    long? Hz,
    string? Note);

public sealed record QrvRosterResult(
    bool Ok,
    int Status,
    string? Error,
    int StaleSeconds,
    IReadOnlyList<QrvRosterEntry> Entries);

public sealed record QrvKeyUpdate(string? InstallKey, string? BoundCall);

public sealed class QrvStatusSnapshot
{
    public bool Enabled { get; init; }
    public bool OnAir { get; init; }
    public string Phase { get; init; } = "off";
    public string? Call { get; init; }
    public string? Band { get; init; }
    public string? Mode { get; init; }
    public string? Error { get; init; }
    public string? Detail { get; init; }
    public string? InstallKeyPrefix { get; init; }
    public IReadOnlyList<QrvRosterEntry> Roster { get; init; } = Array.Empty<QrvRosterEntry>();
}
