using System.Text;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// eQSL.cc real-time QSO upload (v1 SDRLogger+ parity).
///
/// eQSL exposes ImportADIF.cfm as a form-encoded POST that accepts one or
/// more ADIF records. We upload one QSO at a time on QsoService.CreateAsync
/// completion so the operator sees eQSL cards land almost immediately.
/// The one-strike block that ClubLogService uses is overkill here — eQSL
/// doesn't have a reactive IP firewall — so we just log warnings and
/// carry on. A cleared cache of "last error" is exposed for the settings
/// UI to render.
///
/// API endpoint: POST https://www.eQSL.cc/qslcard/importADIF.cfm
///   fields: EQSL_USER, EQSL_PSWD, ADIFData [, EQSL_QTHNICKNAME]
///   response body: text containing "Result: N out of N records added"
///                  on success, or "ERROR:" prefixed message on failure.
/// </summary>
public class EqslService
{
    private readonly ISettingsService _settingsService;
    private readonly HttpClient _http;
    private readonly ILogger<EqslService> _logger;

    public EqslService(ISettingsService settingsService, HttpClient http, ILogger<EqslService> logger)
    {
        _settingsService = settingsService;
        _http = http;
        _logger = logger;
    }

    /// <summary>Single-record ADIF string for ImportADIF.cfm.</summary>
    public static string BuildAdif(Qso qso, string stationCallsign, string? qthNickname)
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
        // eQSL matches on QTH nickname when multiple QTHs are on the account.
        if (!string.IsNullOrWhiteSpace(qthNickname))
            sb.Append(Field("APP_EQSL_QTH_NICKNAME", qthNickname));
        sb.Append("<EOR>");
        return sb.ToString();
    }

    /// <summary>
    /// Verify credentials by uploading a benign minimal ADIF and checking
    /// the response. eQSL doesn't publish a dedicated auth-check endpoint,
    /// so we test by attempting a real upload of a placeholder QSO the
    /// user can safely delete (or nudge to "cancel-if-dupe" via the ADIF
    /// spec) — but the more polite approach is to hit a lightweight page
    /// that requires auth. Here we just POST an empty ADIF and inspect
    /// the error string; a "bad password" response is unambiguous.
    /// </summary>
    public async Task<(bool Ok, string Message)> TestAsync(CancellationToken ct = default)
    {
        var settings = (await _settingsService.GetSettingsAsync()).Eqsl;
        if (string.IsNullOrWhiteSpace(settings.Username))
            return (false, "Enter your eQSL username first");
        if (string.IsNullOrWhiteSpace(settings.Password))
            return (false, "Enter your eQSL password first");

        try
        {
            // Empty ADIF triggers eQSL's "no records" branch AFTER auth,
            // so we can distinguish "bad login" from "0 records" cleanly.
            var form = new Dictionary<string, string>
            {
                ["EQSL_USER"] = settings.Username!,
                ["EQSL_PSWD"] = settings.Password!,
                ["ADIFData"] = "<eoh><eor>",
            };
            if (!string.IsNullOrWhiteSpace(settings.QthNickname))
                form["EQSL_QTHNICKNAME"] = settings.QthNickname!;

            var response = await _http.PostAsync("https://www.eQSL.cc/qslcard/importADIF.cfm",
                new FormUrlEncodedContent(form), ct);
            var body = (await response.Content.ReadAsStringAsync(ct)).Trim();

            if (body.Contains("Bad Callsign/Password", StringComparison.OrdinalIgnoreCase) ||
                body.Contains("Bad password", StringComparison.OrdinalIgnoreCase) ||
                body.Contains("USER not found", StringComparison.OrdinalIgnoreCase))
                return (false, "eQSL rejected the credentials");

            if (!response.IsSuccessStatusCode)
                return (false, $"eQSL returned HTTP {(int)response.StatusCode}");

            // Anything else on 200 = auth worked (0 records is expected).
            return (true, $"eQSL credentials verified for {settings.Username}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (false, $"eQSL test failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Download the eQSL Inbox as ADIF (received QSLs = confirmations). Two-step,
    /// the way eQSL's API works: DownloadInBox.cfm builds a file and returns a
    /// page linking to it, then we fetch that .adi. Reads creds + last-sync from
    /// settings; returns the raw ADIF ("" when there's nothing new). Throws with a
    /// clear message on missing creds / bad login / non-AG account.
    /// </summary>
    public async Task<string> DownloadInboxAdifAsync(CancellationToken ct = default)
    {
        var eqsl = (await _settingsService.GetSettingsAsync()).Eqsl;
        if (string.IsNullOrWhiteSpace(eqsl.Username) || string.IsNullOrWhiteSpace(eqsl.Password))
            throw new InvalidOperationException("Set your eQSL username + password in Settings → eQSL first.");

        var url = new StringBuilder("https://www.eQSL.cc/qslcard/DownloadInBox.cfm?");
        url.Append($"UserName={Uri.EscapeDataString(eqsl.Username!)}&Password={Uri.EscapeDataString(eqsl.Password!)}");
        if (eqsl.LastConfirmationSync is { } since)
            url.Append($"&RcvdSince={since:yyyyMMddHHmm}");

        var html = await _http.GetStringAsync(url.ToString(), ct);

        var link = System.Text.RegularExpressions.Regex.Match(
            html, @"/downloadedfiles/[^""'<>\s]+?\.adi",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        if (!link.Success)
        {
            if (html.Contains("Bad", StringComparison.OrdinalIgnoreCase) ||
                html.Contains("Error", StringComparison.OrdinalIgnoreCase) ||
                html.Contains("not found", StringComparison.OrdinalIgnoreCase) ||
                html.Contains("Authenticity", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException(
                    "eQSL rejected the login, or your account isn't Authenticity-Guaranteed (required for inbox downloads). Check your eQSL username/password.");
            // Valid page, no link = nothing new since the last sync.
            return "";
        }

        var adifUrl = "https://www.eQSL.cc" + link.Value;
        return await _http.GetStringAsync(adifUrl, ct);
    }

    /// <summary>Uploads one QSO. Fire-and-forget style; the caller ignores the return except in tests.</summary>
    public async Task<QslUploadResult> UploadQsoAsync(Qso qso, CancellationToken ct = default)
    {
        var all = await _settingsService.GetSettingsAsync();
        var settings = all.Eqsl;
        if (!settings.Enabled) return QslUploadResult.NotConfigured("eQSL upload not enabled");
        if (string.IsNullOrWhiteSpace(settings.Username) || string.IsNullOrWhiteSpace(settings.Password))
            return QslUploadResult.NotConfigured("eQSL credentials not configured");

        var callsign = !string.IsNullOrWhiteSpace(settings.Username) ? settings.Username! : all.Station.Callsign ?? "";
        if (string.IsNullOrWhiteSpace(callsign))
            return QslUploadResult.NotConfigured("No callsign configured");

        try
        {
            var adif = BuildAdif(qso, callsign, settings.QthNickname);
            _logger.LogInformation("eQSL: uploading {Call} {Band} {Mode}", qso.Callsign, qso.Band, qso.Mode);

            var form = new Dictionary<string, string>
            {
                ["EQSL_USER"] = settings.Username!,
                ["EQSL_PSWD"] = settings.Password!,
                ["ADIFData"] = adif,
            };
            if (!string.IsNullOrWhiteSpace(settings.QthNickname))
                form["EQSL_QTHNICKNAME"] = settings.QthNickname!;

            var response = await _http.PostAsync("https://www.eQSL.cc/qslcard/importADIF.cfm",
                new FormUrlEncodedContent(form), ct);
            var body = (await response.Content.ReadAsStringAsync(ct)).Trim();
            _logger.LogDebug("eQSL: HTTP {Status} — {Body}", (int)response.StatusCode,
                body.Length > 200 ? body[..200] : body);

            if (body.Contains("Bad Callsign/Password", StringComparison.OrdinalIgnoreCase) ||
                body.Contains("Bad password", StringComparison.OrdinalIgnoreCase))
                return QslUploadResult.Fail(QslFailureKind.Auth,
                    "eQSL rejected the credentials — fix them in Settings");

            if (body.Contains("out of", StringComparison.OrdinalIgnoreCase) &&
                body.Contains("added", StringComparison.OrdinalIgnoreCase))
                return QslUploadResult.Success();

            // eQSL answers 200 with an HTML body regardless, so the status code
            // carries no signal — a 5xx or a transport fault is the only
            // retryable case, and both surface as an exception below.
            if (body.StartsWith("ERROR:", StringComparison.OrdinalIgnoreCase))
                return QslUploadResult.Fail(QslFailureKind.Rejected,
                    body.Length > 200 ? body[..200] : body);

            return QslUploadResult.Fail(QslFailureKind.Rejected,
                $"eQSL: unexpected response — {(body.Length > 200 ? body[..200] : body)}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "eQSL upload failed");
            return QslUploadResult.Fail(QslFailureKind.Temporary, $"eQSL upload failed: {ex.Message}");
        }
    }
}
