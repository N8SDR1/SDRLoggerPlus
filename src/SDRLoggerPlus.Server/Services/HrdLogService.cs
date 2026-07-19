using System.Text;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// HRDLog.net real-time QSO upload. Posts a single-QSO ADIF record to HRDLog's
/// NewEntry robot endpoint, authenticated by the account callsign plus the
/// per-account upload code (hrdlog.net → My Account → Online Log).
/// API: https://www.hrdlog.net/API.aspx
///
/// Success detection is deliberately lenient: a 2xx response is treated as
/// accepted (the raw body is logged), with an explicit failure only when the
/// body clearly signals rejection. The exact NewEntry response format should be
/// confirmed against a live upload before tightening this.
/// </summary>
public class HrdLogService
{
    private const string Endpoint = "https://robot.hrdlog.net/NewEntry.aspx";
    private const string AppName = "SDRLoggerPlus";

    private readonly ISettingsService _settingsService;
    private readonly HttpClient _http;
    private readonly ILogger<HrdLogService> _logger;

    public HrdLogService(ISettingsService settingsService, HttpClient http, ILogger<HrdLogService> logger)
    {
        _settingsService = settingsService;
        _http = http;
        _logger = logger;
    }

    /// <summary>Single-record ADIF string for the NewEntry endpoint.</summary>
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

    /// <summary>Uploads one QSO. Returns (success, errorMessage).</summary>
    public async Task<(bool Ok, string? Error)> UploadQsoAsync(Qso qso, CancellationToken ct = default)
    {
        var settings = (await _settingsService.GetSettingsAsync()).HrdLog;
        if (!settings.Enabled)
            return (false, "HRDLog upload not enabled");
        if (string.IsNullOrWhiteSpace(settings.UploadCode))
            return (false, "HRDLog upload code not configured");

        var station = (await _settingsService.GetSettingsAsync()).Station;
        var callsign = !string.IsNullOrWhiteSpace(settings.Callsign) ? settings.Callsign! : station.Callsign ?? "";
        if (string.IsNullOrWhiteSpace(callsign))
            return (false, "No callsign configured");

        try
        {
            var adif = BuildAdif(qso, callsign);
            _logger.LogInformation("HRDLog: uploading {Call} {Band} {Mode}", qso.Callsign, qso.Band, qso.Mode);

            var response = await _http.PostAsync(Endpoint,
                new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["Callsign"] = callsign,
                    ["Code"] = settings.UploadCode!.Trim(),
                    ["App"] = AppName,
                    ["ADIFData"] = adif,
                }), ct);

            var body = (await response.Content.ReadAsStringAsync(ct)).Trim();
            _logger.LogInformation("HRDLog: HTTP {Status} — {Body}", (int)response.StatusCode, body.Length > 300 ? body[..300] : body);

            if (!response.IsSuccessStatusCode)
                return (false, $"HRDLog: server returned HTTP {(int)response.StatusCode}");

            // Lenient: only fail when the body clearly signals rejection. These
            // words are very unlikely to appear in a success acknowledgement.
            foreach (var marker in new[] { "invalid", "denied", "rejected", "not authorized", "unauthorized" })
            {
                if (body.Contains(marker, StringComparison.OrdinalIgnoreCase))
                    return (false, $"HRDLog rejected the QSO — {(body.Length > 200 ? body[..200] : body)}");
            }

            return (true, null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "HRDLog upload failed");
            return (false, $"HRDLog upload failed: {ex.Message}");
        }
    }
}
