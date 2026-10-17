using System.Text;
using System.Text.RegularExpressions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Logging;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Club Log real-time QSO upload (SDRLogger+ port).
///
/// Uses realtime.php for per-QSO uploads ONLY — putlogs.php is for bulk ADIF
/// imports, and calling it repeatedly for single QSOs triggers automatic IP
/// blocking by Club Log. On HTTP 403 (or "Login rejected") a blocked flag stops
/// ALL further uploads until the user re-saves credentials (one-strike rule —
/// repeated 403s from one IP get firewalled by Club Log).
/// API details: https://clublog.freshdesk.com/support/solutions/articles/54906
/// </summary>
public class ClubLogService
{
    private readonly ISettingsService _settingsService;
    private readonly HttpClient _http;
    private readonly ILogger<ClubLogService> _logger;
    private readonly Qsl.QslBlockStateStore? _blockStore;
    private volatile bool _blocked;

    public ClubLogService(ISettingsService settingsService, HttpClient http, ILogger<ClubLogService> logger,
        Qsl.QslBlockStateStore? blockStore = null)
    {
        _settingsService = settingsService;
        _http = http;
        _logger = logger;
        _blockStore = blockStore;

        // The block must survive a restart. It used to be process state only,
        // so restarting the app re-armed uploads against credentials Club Log
        // had already rejected — and repeating rejected POSTs is precisely what
        // gets an IP firewalled.
        if (_blockStore?.Load().ClubLogBlockedUtc is { } blockedAt)
        {
            _blocked = true;
            _logger.LogWarning(
                "Club Log uploads remain disabled from a previous authentication failure at {BlockedUtc}. " +
                "Re-save Club Log credentials in Settings to clear it.", blockedAt);
        }
    }

    public bool IsBlocked => _blocked;

    /// <summary>Clears the auth-failure block (called when credentials are re-saved).</summary>
    public void ResetBlock()
    {
        _blocked = false;
        _blockStore?.Save(new Qsl.QslBlockState());
    }

    /// <summary>Trips the one-strike block and persists it.</summary>
    private void Block(string reason)
    {
        _blocked = true;
        _blockStore?.Save(new Qsl.QslBlockState
        {
            ClubLogBlockedUtc = DateTime.UtcNow,
            ClubLogReason = reason,
        });
    }

    /// <summary>Single-record ADIF string for realtime.php.</summary>
    public static string BuildAdif(Qso qso, string stationCallsign)
    {
        static string Field(string name, string? value) =>
            string.IsNullOrEmpty(value) ? "" : $"<{name}:{value.Length}>{value}";

        var sb = new StringBuilder();
        sb.Append(Field("CALL", qso.Callsign));
        sb.Append(Field("STATION_CALLSIGN", stationCallsign));
        // QSO_DATE + TIME_ON are UTC, derived from one QsoDate instant so they can't drift.
        var utc = qso.QsoDate.ToUniversalTime();
        sb.Append(Field("QSO_DATE", utc.ToString("yyyyMMdd")));
        sb.Append(Field("TIME_ON", utc.ToString("HHmmss")));
        sb.Append(Field("BAND", qso.Band));
        sb.Append(Field("MODE", qso.Mode));
        // Qso.Frequency is kHz; ADIF FREQ is MHz.
        sb.Append(Field("FREQ", qso.Frequency is > 0
            ? (qso.Frequency.Value / 1000.0).ToString("F6", System.Globalization.CultureInfo.InvariantCulture) : null));
        sb.Append(Field("RST_SENT", qso.RstSent));
        sb.Append(Field("RST_RCVD", qso.RstRcvd));
        sb.Append("<EOR>");
        return sb.ToString();
    }

    /// <summary>
    /// Verifies credentials via getlotwstate.php — a safe read-only endpoint
    /// (200 = valid, 403 = refused). NEVER test via realtime.php: repeated
    /// failed POSTs trigger Club Log's reactive IP firewall. A successful test
    /// clears the one-strike block.
    /// </summary>
    /// <param name="draft">
    /// Values currently in the settings form. When set, these are what gets
    /// tested — not the last saved copy — so the reply matches what the
    /// operator just typed. Null uses the saved settings.
    /// </param>
    public async Task<(bool Ok, string Message)> TestAsync(ClubLogSettings? draft = null, CancellationToken ct = default)
    {
        var saved = await _settingsService.GetSettingsAsync();
        var settings = draft ?? saved.ClubLog;
        var apiKey = settings.ApiKey?.Trim();
        var email = settings.Email?.Trim();
        var password = settings.Password?.Trim();
        if (string.IsNullOrWhiteSpace(apiKey))
            return (false, "Enter your Club Log application key first");
        if (string.IsNullOrWhiteSpace(email) || string.IsNullOrWhiteSpace(password))
            return (false, "Enter your Club Log email and application password first");

        var callsign = !string.IsNullOrWhiteSpace(settings.Callsign) ? settings.Callsign!.Trim() : saved.Station?.Callsign ?? "";
        if (string.IsNullOrWhiteSpace(callsign))
            return (false, "Set your callsign first");

        try
        {
            var url = $"https://clublog.org/getlotwstate.php?api={Uri.EscapeDataString(apiKey)}" +
                      $"&email={Uri.EscapeDataString(email)}&password={Uri.EscapeDataString(password)}" +
                      $"&callsign={Uri.EscapeDataString(callsign)}";
            var response = await _http.GetAsync(url, ct);
            if (response.IsSuccessStatusCode)
            {
                ResetBlock();
                return (true, $"Club Log credentials verified for {callsign}.");
            }

            var body = await response.Content.ReadAsStringAsync(ct);
            var message = FormatRejection((int)response.StatusCode, body, password, apiKey);
            _logger.LogInformation("Club Log credential test failed: {Message}", message);
            return (false, message);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (false, $"Club Log test failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Club Log's body is the actual reason when they send one (callsign not
    /// on the account, login rejected, invalid API key). A stock "403 Forbidden"
    /// page from their proxy has no reason, so say what to check instead of
    /// repeating that page.
    /// </summary>
    internal static string FormatRejection(int statusCode, string? body, string? password, string? apiKey)
    {
        var detail = SummarizeBody(body);
        detail = SecretScrubber.Redact(detail, password, apiKey) ?? "";
        detail = SecretScrubber.Head(detail, 180).Trim();
        if (string.IsNullOrEmpty(detail) || IsGenericStatusPage(detail))
            return $"Club Log refused the login (HTTP {statusCode}). Use an application password from Club Log → Settings → App Passwords (not the website sign-in password), a callsign on that account, and the application key issued for this program.";
        return $"Club Log rejected the request (HTTP {statusCode}): {detail}";
    }

    private static bool IsGenericStatusPage(string detail)
    {
        var generic = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "400", "401", "403", "404", "500", "forbidden", "unauthorized",
            "not", "found", "error", "http", "nginx", "apache", "cloudflare",
        };
        var tokens = detail.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return tokens.Length > 0 && tokens.All(t => generic.Contains(t.TrimEnd('.', ':', '-')));
    }

    private static string SummarizeBody(string? body)
    {
        if (string.IsNullOrWhiteSpace(body)) return "";
        var text = Regex.Replace(body, "<[^>]+>", " ");
        return Regex.Replace(text, @"\s+", " ").Trim();
    }

    /// <summary>Uploads one QSO. Returns (success, errorMessage).</summary>
    public async Task<QslUploadResult> UploadQsoAsync(Qso qso, CancellationToken ct = default)
    {
        if (_blocked)
            return QslUploadResult.Fail(QslFailureKind.Auth,
                "Club Log uploads disabled — authentication failed previously; re-save credentials in Settings");

        var settings = (await _settingsService.GetSettingsAsync()).ClubLog;
        if (!settings.Enabled)
            return QslUploadResult.NotConfigured("Club Log upload not enabled");
        if (string.IsNullOrWhiteSpace(settings.ApiKey))
            return QslUploadResult.NotConfigured("Club Log application key not configured");
        if (string.IsNullOrWhiteSpace(settings.Email) || string.IsNullOrWhiteSpace(settings.Password))
            return QslUploadResult.NotConfigured("Club Log credentials not configured");

        var station = (await _settingsService.GetSettingsAsync()).Station;
        var callsign = !string.IsNullOrWhiteSpace(settings.Callsign) ? settings.Callsign! : station.Callsign ?? "";
        if (string.IsNullOrWhiteSpace(callsign))
            return QslUploadResult.NotConfigured("No callsign configured");

        try
        {
            var adif = BuildAdif(qso, callsign);
            _logger.LogInformation("Club Log: uploading {Call} {Band} {Mode}", qso.Callsign, qso.Band, qso.Mode);

            var response = await _http.PostAsync("https://clublog.org/realtime.php",
                new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["api"] = settings.ApiKey!.Trim(),
                    ["email"] = settings.Email!,
                    ["password"] = settings.Password!,
                    ["callsign"] = callsign,
                    ["adif"] = adif,
                }), ct);

            var body = (await response.Content.ReadAsStringAsync(ct)).Trim();
            // Classify against the raw body and report the redacted one. Masking
            // before classification would let a credential that happens to
            // contain a marker word change how the response is interpreted.
            var safeBody = SecretScrubber.Redact(body, settings.Password, settings.ApiKey)!;
            _logger.LogDebug("Club Log: HTTP {Status} — {Body}", (int)response.StatusCode, SecretScrubber.Head(safeBody, 200));

            if (response.StatusCode == System.Net.HttpStatusCode.Forbidden || body.StartsWith("Login rejected"))
            {
                // One-strike: stop everything until credentials change, and
                // remember it across restarts.
                Block($"HTTP {(int)response.StatusCode}: {SecretScrubber.Head(safeBody, 120)}");
                _logger.LogWarning("Club Log: 403 — all uploads disabled to prevent IP firewall ban");
                return QslUploadResult.Fail(QslFailureKind.Auth,
                    "Club Log: authentication failed (403) — uploads disabled to prevent an IP ban. Re-check credentials in Settings.");
            }
            if (response.StatusCode == System.Net.HttpStatusCode.BadRequest)
                return QslUploadResult.Fail(QslFailureKind.Rejected,
                    $"Club Log: QSO rejected — {SecretScrubber.Head(safeBody, 200)}");
            if ((int)response.StatusCode >= 500)
                return QslUploadResult.Fail(QslFailureKind.Temporary, "Club Log: server error — try again later");

            if (Regex.IsMatch(body, @"\bOK\b") || Regex.IsMatch(body, @"\bDupe\b") || body.Contains("Updated QSO"))
                return QslUploadResult.Success();

            // An unrecognised body is classified Rejected, not Temporary. The
            // POST may well have been accepted, and re-sending something Club
            // Log already acted on is exactly the repetition its IP firewall
            // watches for. A human should look instead.
            return QslUploadResult.Fail(QslFailureKind.Rejected,
                $"Club Log: unexpected response — {SecretScrubber.Head(safeBody, 200)}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Club Log upload failed");
            return QslUploadResult.Fail(QslFailureKind.Temporary, $"Club Log upload failed: {ex.Message}");
        }
    }
}
