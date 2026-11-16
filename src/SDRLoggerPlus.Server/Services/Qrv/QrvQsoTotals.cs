using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// QSO-totals rules from docs/qrv-qso-totals.md. Replacement snapshots only:
/// call, app "Logger", the presence install key, and week/month/year
/// voice/cw/digital counts. No ADIF, no per-QSO rows, no frequencies.
/// </summary>
public static class QrvQsoTotals
{
    public const int MaxCount = 100_000;
    public const string SourceLyra = "Lyra";
    public const string SourceStd = "STD";
    public const string SourceLogger = "Logger";

    public static readonly TimeSpan RefreshPeriod = TimeSpan.FromMinutes(3);
    public static readonly TimeSpan MinPostInterval = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan StoreFailedRetry = TimeSpan.FromSeconds(30);

    private static readonly HashSet<string> VoiceModes = new(StringComparer.Ordinal)
    {
        "LSB", "USB", "AM", "SAM", "FM", "NFM", "DSB", "SSB",
    };

    private static readonly HashSet<string> CwModes = new(StringComparer.Ordinal)
    {
        "CW", "CWU", "CWL", "CWR",
    };

    /// <summary>
    /// What one live TCI session has announced. A {LOG} macro is not part of this.
    /// </summary>
    public readonly record struct TciSession(string? Protocol, bool ComboLinked, bool StdIdentified)
    {
        public static TciSession None => new(null, false, false);
    }

    public readonly record struct ModeCounts(int Voice, int Cw, int Digital)
    {
        public static ModeCounts Zero => new(0, 0, 0);

        public int Total => Voice + Cw + Digital;
    }

    public readonly record struct Snapshot(ModeCounts Week, ModeCounts Month, ModeCounts Year)
    {
        public static Snapshot Zero => new(ModeCounts.Zero, ModeCounts.Zero, ModeCounts.Zero);
    }

    /// <summary>
    /// When the TCI session identifies itself, the saved label is still Lyra or STD.
    /// Otherwise the label is Logger. Either label counts. Combo link is not required
    /// for the contact to count. Lyra's hello is <c>protocol:Lyra</c>.
    /// <c>protocol:ExpertSDR3</c> is labeled Lyra only while Combo link is on.
    /// STD is labeled STD only when that session has identified STD and the logging
    /// program is STD (or the hello itself is <c>protocol:STD</c>). A hand log is not
    /// guessed to be STD, and a {LOG} macro does not create a source.
    /// </summary>
    public static string Classify(TciSession session, string? loggingProgramId)
    {
        var protocol = session.Protocol?.Trim();
        var lyraHello = EqualsName(protocol, SourceLyra);
        var expert = EqualsName(protocol, "ExpertSDR3");
        var stdHello = EqualsName(protocol, SourceStd);
        var lyra = lyraHello || (expert && session.ComboLinked);
        var std = session.StdIdentified || stdHello;

        if (IsStdProgram(loggingProgramId))
            return std ? SourceStd : SourceLogger;

        if (lyra) return SourceLyra;
        if (stdHello) return SourceStd;
        return SourceLogger;
    }

    /// <summary>
    /// STD has identified itself on this TCI session. Lyra's combo / {LOG}
    /// commands are not an identity. Accepted forms are the same hello grammar
    /// as Lyra (<c>protocol:STD</c>) and a client announcement
    /// (<c>std:</c>, <c>app:STD</c>, <c>client:STD</c>).
    /// </summary>
    public static bool IsStdIdentity(string? command, IReadOnlyList<string>? args)
    {
        if (string.IsNullOrWhiteSpace(command)) return false;
        if (command.Equals("std", StringComparison.OrdinalIgnoreCase)) return true;
        if (args == null || args.Count == 0 || string.IsNullOrWhiteSpace(args[0])) return false;
        if (!args[0].Trim().Equals(SourceStd, StringComparison.OrdinalIgnoreCase)) return false;
        return command.Equals("protocol", StringComparison.OrdinalIgnoreCase)
            || command.Equals("app", StringComparison.OrdinalIgnoreCase)
            || command.Equals("client", StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsCountedContact(Qso qso)
    {
        if (qso.RadioSource is not (SourceLyra or SourceStd or SourceLogger)) return false;
        return !IsContest(qso);
    }

    public static bool IsContest(Qso qso)
    {
        var contest = qso.Contest;
        if (contest == null) return false;
        return !string.IsNullOrWhiteSpace(contest.ContestId)
            || !string.IsNullOrWhiteSpace(contest.SessionId);
    }

    /// <summary>Voice, CW, or digital. Counted QSOs with any other mode are digital.</summary>
    public static string ModeBucket(string? mode)
    {
        var token = NormalizeMode(mode);
        if (VoiceModes.Contains(token)) return "voice";
        if (CwModes.Contains(token)) return "cw";
        return "digital";
    }

    public static DateTime WeekStartUtc(DateTime utcNow)
    {
        var now = AsUtc(utcNow);
        var date = DateOnly.FromDateTime(now);
        var monday = ISOWeek.ToDateTime(ISOWeek.GetYear(date), ISOWeek.GetWeekOfYear(date), DayOfWeek.Monday);
        return DateTime.SpecifyKind(monday, DateTimeKind.Utc);
    }

    public static DateTime MonthStartUtc(DateTime utcNow)
    {
        var now = AsUtc(utcNow);
        return new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc);
    }

    public static DateTime YearStartUtc(DateTime utcNow)
    {
        var now = AsUtc(utcNow);
        return new DateTime(now.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }

    public static Snapshot Count(IEnumerable<Qso> qsos, DateTime utcNow)
    {
        var now = AsUtc(utcNow);
        var weekStart = WeekStartUtc(now);
        var monthStart = MonthStartUtc(now);
        var yearStart = YearStartUtc(now);
        var week = new int[3];
        var month = new int[3];
        var year = new int[3];

        foreach (var qso in qsos)
        {
            if (!IsCountedContact(qso)) continue;
            var when = AsUtc(qso.QsoDate);
            if (when > now) continue;
            var bucket = BucketIndex(qso.Mode);
            if (when >= yearStart) year[bucket]++;
            if (when >= monthStart) month[bucket]++;
            if (when >= weekStart) week[bucket]++;
        }

        return new Snapshot(Clamp(week), Clamp(month), Clamp(year));
    }

    public static string Encode(string call, string installKey, Snapshot snapshot)
    {
        var obj = new JsonObject
        {
            ["call"] = call,
            ["app"] = QrvProtocol.AppName,
            ["install_key"] = installKey,
            ["week"] = CountsObject(snapshot.Week),
            ["month"] = CountsObject(snapshot.Month),
            ["year"] = CountsObject(snapshot.Year),
        };
        return obj.ToJsonString();
    }

    public static QrvTotalsWriteResult ParseWrite(string? body, int status)
    {
        if (string.IsNullOrWhiteSpace(body))
            return new QrvTotalsWriteResult(false, status, status > 0 ? $"http_{status}" : "QRV unavailable", null, null, null);

        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return new QrvTotalsWriteResult(false, status, status > 0 ? $"http_{status}" : "invalid_json", null, null, null);

            var root = doc.RootElement;
            var ok = root.TryGetProperty("ok", out var okEl) && okEl.ValueKind == JsonValueKind.True;
            var error = ReadString(root, "error");
            string? week = null, month = null, year = null;
            if (root.TryGetProperty("periods", out var periods) && periods.ValueKind == JsonValueKind.Object)
            {
                week = ReadString(periods, "week");
                month = ReadString(periods, "month");
                year = ReadString(periods, "year");
            }
            if (!ok && string.IsNullOrEmpty(error))
                error = status > 0 ? $"http_{status}" : "QRV unavailable";
            return new QrvTotalsWriteResult(ok, status, error, week, month, year);
        }
        catch (JsonException)
        {
            return new QrvTotalsWriteResult(false, status, status > 0 ? $"http_{status}" : "invalid_json", null, null, null);
        }
    }

    private static JsonObject CountsObject(ModeCounts counts) => new()
    {
        ["voice"] = counts.Voice,
        ["cw"] = counts.Cw,
        ["digital"] = counts.Digital,
    };

    private static ModeCounts Clamp(int[] buckets) =>
        new(Clamp(buckets[0]), Clamp(buckets[1]), Clamp(buckets[2]));

    private static int Clamp(int value)
    {
        if (value < 0) return 0;
        return value > MaxCount ? MaxCount : value;
    }

    private static int BucketIndex(string? mode) => ModeBucket(mode) switch
    {
        "voice" => 0,
        "cw" => 1,
        _ => 2,
    };

    internal static string NormalizeMode(string? mode)
    {
        if (string.IsNullOrWhiteSpace(mode)) return "";
        var upper = mode.Trim().ToUpperInvariant();
        if (upper is "CW-R" or "CWR") return "CWR";
        var token = new string(upper.Where(static c => c is >= 'A' and <= 'Z' or >= '0' and <= '9').ToArray());
        return token == "CWR" ? "CWR" : token;
    }

    private static bool IsStdProgram(string? id) =>
        !string.IsNullOrWhiteSpace(id) && id.Trim().Equals(SourceStd, StringComparison.OrdinalIgnoreCase);

    private static bool EqualsName(string? value, string name) =>
        value != null && value.Equals(name, StringComparison.OrdinalIgnoreCase);

    private static DateTime AsUtc(DateTime value) =>
        value.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(value, DateTimeKind.Utc)
            : value.ToUniversalTime();

    private static string? ReadString(JsonElement obj, string name)
    {
        if (!obj.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String)
            return null;
        return value.GetString();
    }
}

public sealed record QrvTotalsWriteResult(
    bool Ok,
    int Status,
    string? Error,
    string? WeekPeriod,
    string? MonthPeriod,
    string? YearPeriod);
