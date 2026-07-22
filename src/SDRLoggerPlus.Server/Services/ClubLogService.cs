using System.Text;
using System.Text.RegularExpressions;
using SDRLoggerPlus.Contracts.Models;

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
        sb.Append(Field("QSO_DATE", qso.QsoDate.ToString("yyyyMMdd")));
        sb.Append(Field("TIME_ON", qso.TimeOn?.Replace(":", "") is { Length: > 6 } t ? t[..6] : qso.TimeOn?.Replace(":", "")));
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
    /// (200 = valid, 403 = invalid). NEVER test via realtime.php: repeated
    /// failed POSTs trigger Club Log's reactive IP firewall. A successful test
    /// clears the one-strike block.
    /// </summary>
    public async Task<(bool Ok, string Message)> TestAsync(CancellationToken ct = default)
    {
        var settings = (await _settingsService.GetSettingsAsync()).ClubLog;
        if (string.IsNullOrWhiteSpace(settings.ApiKey))
            return (false, "Enter your Club Log application key first");
        if (string.IsNullOrWhiteSpace(settings.Email) || string.IsNullOrWhiteSpace(settings.Password))
            return (false, "Enter your Club Log email and password first");

        var station = (await _settingsService.GetSettingsAsync()).Station;
        var callsign = !string.IsNullOrWhiteSpace(settings.Callsign) ? settings.Callsign! : station.Callsign ?? "";
        if (string.IsNullOrWhiteSpace(callsign))
            return (false, "Set your callsign first");

        try
        {
            var url = $"https://clublog.org/getlotwstate.php?api={Uri.EscapeDataString(settings.ApiKey!.Trim())}" +
                      $"&email={Uri.EscapeDataString(settings.Email!)}&password={Uri.EscapeDataString(settings.Password!)}" +
                      $"&callsign={Uri.EscapeDataString(callsign)}";
            var response = await _http.GetAsync(url, ct);
            if (response.IsSuccessStatusCode)
            {
                ResetBlock();
                return (true, $"Club Log credentials verified for {callsign}.");
            }
            return (false, $"Club Log rejected the credentials (HTTP {(int)response.StatusCode}).");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (false, $"Club Log test failed: {ex.Message}");
        }
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
            _logger.LogDebug("Club Log: HTTP {Status} — {Body}", (int)response.StatusCode, body.Length > 200 ? body[..200] : body);

            if (response.StatusCode == System.Net.HttpStatusCode.Forbidden || body.StartsWith("Login rejected"))
            {
                // One-strike: stop everything until credentials change, and
                // remember it across restarts.
                Block($"HTTP {(int)response.StatusCode}: {(body.Length > 120 ? body[..120] : body)}");
                _logger.LogWarning("Club Log: 403 — all uploads disabled to prevent IP firewall ban");
                return QslUploadResult.Fail(QslFailureKind.Auth,
                    "Club Log: authentication failed (403) — uploads disabled to prevent an IP ban. Re-check credentials in Settings.");
            }
            if (response.StatusCode == System.Net.HttpStatusCode.BadRequest)
                return QslUploadResult.Fail(QslFailureKind.Rejected,
                    $"Club Log: QSO rejected — {(body.Length > 200 ? body[..200] : body)}");
            if ((int)response.StatusCode >= 500)
                return QslUploadResult.Fail(QslFailureKind.Temporary, "Club Log: server error — try again later");

            if (Regex.IsMatch(body, @"\bOK\b") || Regex.IsMatch(body, @"\bDupe\b") || body.Contains("Updated QSO"))
                return QslUploadResult.Success();

            // An unrecognised body is classified Rejected, not Temporary. The
            // POST may well have been accepted, and re-sending something Club
            // Log already acted on is exactly the repetition its IP firewall
            // watches for. A human should look instead.
            return QslUploadResult.Fail(QslFailureKind.Rejected,
                $"Club Log: unexpected response — {(body.Length > 200 ? body[..200] : body)}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Club Log upload failed");
            return QslUploadResult.Fail(QslFailureKind.Temporary, $"Club Log upload failed: {ex.Message}");
        }
    }
}
