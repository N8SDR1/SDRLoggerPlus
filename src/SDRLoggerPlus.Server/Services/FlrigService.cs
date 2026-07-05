using System.Text;
using System.Xml.Linq;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// W1HKJ flrig XML-RPC rig-control integration, ported from v1.x SDRLogger+.
/// flrig is a lightweight rig-control bridge that exposes rig.get_vfoA,
/// rig.set_vfoA, rig.get_mode, rig.set_mode etc. over XML-RPC (default port
/// 12345). We poll it every 1.5 s while enabled and broadcast the freq/mode
/// via SignalR using the same RadioStateChangedEvent Hamlib/TCI use — the
/// UI treats it as "just another rig" once connected.
///
/// The XML-RPC client below is a minimal HttpClient+XDocument implementation.
/// flrig's XML-RPC surface is tiny (six methods total) so we don't pull in
/// a general-purpose XML-RPC NuGet dependency for it.
/// </summary>
public class FlrigService : BackgroundService
{
    /// <summary>Fixed RadioId — flrig only exposes one connected rig at a time.</summary>
    public const string FlrigRadioId = "flrig";

    private readonly IHubContext<LogHub, ILogHubClient> _hubContext;
    private readonly ISettingsRepository _settingsRepository;
    private readonly ILogger<FlrigService> _logger;
    private readonly IHttpClientFactory _httpClientFactory;

    private RadioConnectionState _state = RadioConnectionState.Disconnected;
    private long _lastFreqHz;
    private string _lastMode = "";
    private string _rigModel = "";
    private string _detectedDigitalUsb = "DATA-U";
    private string _detectedDigitalLsb = "DATA-L";

    // Mode name candidates flrig may report on different rigs, checked in
    // priority order during connection to figure out what to send back for
    // the app's normalized "DIGU"/"DIGL" modes.
    private static readonly string[] DigitalUsbCandidates = { "USB-D", "DATA-U", "PKT-U", "DIGU" };
    private static readonly string[] DigitalLsbCandidates = { "LSB-D", "DATA-L", "PKT-L", "DIGL" };

    // flrig mode string → app's normalized mode string. Covers the common rigs
    // (Icom digital passthrough, Kenwood/Yaesu DATA-U/-L, PKT-U/-L on some).
    private static readonly Dictionary<string, string> ModeIn = new(StringComparer.OrdinalIgnoreCase)
    {
        ["USB"] = "USB", ["LSB"] = "LSB",
        ["CW"] = "CWU", ["CW-R"] = "CWL", ["CWR"] = "CWL",
        ["FM"] = "FM", ["NFM"] = "NFM", ["AM"] = "AM", ["SAM"] = "SAM",
        ["RTTY"] = "RTTY", ["RTTYR"] = "RTTY", ["RTTY-R"] = "RTTY",
        // Digital passthrough — Kenwood/Yaesu
        ["DATA-U"] = "DIGU", ["DATA-L"] = "DIGL",
        ["PKT-U"] = "DIGU", ["PKT-L"] = "DIGL",
        ["DIGU"] = "DIGU", ["DIGL"] = "DIGL",
        // Icom / IC-9100 digital passthrough
        ["USB-D"] = "DIGU", ["LSB-D"] = "DIGL",
        ["USBD"] = "DIGU", ["LSBD"] = "DIGL",
        // Direct WSJT-X / friends passthrough (rare on flrig side)
        ["FT8"] = "FT8", ["FT4"] = "FT4", ["JS8"] = "JS8", ["WSPR"] = "WSPR",
        ["JT65"] = "JT65", ["JT9"] = "JT9",
    };

    // App's normalized mode → flrig mode string for outgoing set_mode calls.
    // Digital passthrough modes are overridden at send-time by the
    // auto-detected _detectedDigitalUsb / _detectedDigitalLsb so the right
    // name is sent for whichever rig is actually attached.
    private static readonly Dictionary<string, string> ModeOut = new(StringComparer.OrdinalIgnoreCase)
    {
        ["USB"] = "USB", ["LSB"] = "LSB",
        ["CWU"] = "CW", ["CWL"] = "CW-R", ["CW"] = "CW",
        ["FM"] = "FM", ["NFM"] = "FM", ["AM"] = "AM", ["SAM"] = "AM",
        ["RTTY"] = "RTTY", ["RTTY-R"] = "RTTY-R", ["RTTYR"] = "RTTY-R",
        // Placeholders — overridden at send-time by digital-mode detection.
        ["DIGU"] = "DATA-U", ["DIGL"] = "DATA-L",
        ["FT8"] = "DATA-U", ["FT4"] = "DATA-U", ["JS8"] = "DATA-U",
        ["WSPR"] = "DATA-U", ["JT65"] = "DATA-U", ["JT9"] = "DATA-U",
        ["DIGI"] = "DATA-U", ["PSK31"] = "DATA-U",
    };

    private static readonly HashSet<string> DigitalUsbAppModes = new(StringComparer.OrdinalIgnoreCase)
    {
        "FT8", "FT4", "JS8", "WSPR", "JT65", "JT9", "DIGI", "PSK31", "DIGU",
        "DATA-U", "PKT-U", "MSK144", "Q65", "FST4", "FST4W", "VARAC", "OLIVIA",
        "HELL", "PACKET", "DATA",
    };
    private static readonly HashSet<string> DigitalLsbAppModes = new(StringComparer.OrdinalIgnoreCase)
    {
        "DIGL", "DATA-L", "PKT-L",
    };

    public FlrigService(
        IHubContext<LogHub, ILogHubClient> hubContext,
        ISettingsRepository settingsRepository,
        IHttpClientFactory httpClientFactory,
        ILogger<FlrigService> logger)
    {
        _hubContext = hubContext;
        _settingsRepository = settingsRepository;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public bool IsConnected => _state == RadioConnectionState.Connected;
    public long LastFrequencyHz => _lastFreqHz;
    public string LastMode => _lastMode;
    public string RigModel => _rigModel;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Give the rest of the service graph a beat to come up before the
        // first XML-RPC poke.
        try { await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken); }
        catch (OperationCanceledException) { return; }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await _settingsRepository.GetAsync();
                var cfg = settings?.Radio?.Flrig;
                if (cfg == null || !cfg.Enabled)
                {
                    if (_state != RadioConnectionState.Disconnected)
                    {
                        _logger.LogInformation("flrig disabled — disconnecting");
                        await SetStateAsync(RadioConnectionState.Disconnected);
                    }
                    await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
                    continue;
                }

                await PollOnceAsync(cfg, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "flrig poller error");
                await SetStateAsync(RadioConnectionState.Error, ex.Message);
            }

            try { await Task.Delay(TimeSpan.FromMilliseconds(1500), stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    // ── Public control surface (called from LogHub) ──────────────────────

    /// <summary>
    /// Try to set the rig frequency. Returns true if flrig accepted the
    /// call. Uses XML-RPC's &lt;double&gt; type — flrig's set_vfoA requires
    /// that; sending an integer or string silently fails on some builds.
    /// </summary>
    public async Task<bool> SetFrequencyAsync(long frequencyHz, CancellationToken ct = default)
    {
        var settings = await _settingsRepository.GetAsync();
        var cfg = settings?.Radio?.Flrig;
        if (cfg == null || !cfg.Enabled) return false;
        try
        {
            await XmlRpcCallAsync(cfg, "rig.set_vfoA",
                new XElement("value", new XElement("double", frequencyHz.ToString("F0", System.Globalization.CultureInfo.InvariantCulture))),
                ct);
            _lastFreqHz = frequencyHz;
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "flrig set_vfoA {Hz} failed", frequencyHz);
            return false;
        }
    }

    /// <summary>
    /// Set the rig mode. Handles the app's normalized mode names
    /// (USB/LSB/CWU/CWL/DIGU/DIGL/FT8/etc.) and translates to the right
    /// flrig string for whatever rig is attached, respecting the user's
    /// DigitalMode / RttyMode overrides.
    /// </summary>
    public async Task<bool> SetModeAsync(string appMode, CancellationToken ct = default)
    {
        var settings = await _settingsRepository.GetAsync();
        var cfg = settings?.Radio?.Flrig;
        if (cfg == null || !cfg.Enabled) return false;
        if (string.IsNullOrWhiteSpace(appMode)) return false;

        var flrigMode = MapAppModeToFlrig(appMode, cfg);
        if (string.IsNullOrWhiteSpace(flrigMode)) return false;

        try
        {
            await XmlRpcCallAsync(cfg, "rig.set_mode",
                new XElement("value", new XElement("string", flrigMode)),
                ct);
            _lastMode = appMode.ToUpperInvariant();
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "flrig set_mode {Mode} → {FlrigMode} failed", appMode, flrigMode);
            return false;
        }
    }

    // ── Polling ──────────────────────────────────────────────────────────

    private async Task PollOnceAsync(FlrigSettings cfg, CancellationToken ct)
    {
        // rig.get_vfoA
        string? freqStr;
        try
        {
            freqStr = await XmlRpcCallReturningStringAsync(cfg, "rig.get_vfoA", null, ct);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("flrig connect failed: {Msg}", ex.Message);
            await SetStateAsync(RadioConnectionState.Disconnected);
            return;
        }
        if (freqStr == null) return;

        long freqHz = 0;
        if (long.TryParse(freqStr, System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var parsedHz))
        {
            freqHz = parsedHz;
        }

        // rig.get_mode
        var modeRaw = await XmlRpcCallReturningStringAsync(cfg, "rig.get_mode", null, ct) ?? "";
        var normalizedMode = MapFlrigModeToApp(modeRaw);

        // First successful poll after a disconnect — auto-detect digital mode
        // names and grab the rig model. Both are best-effort; older flrig
        // builds may not implement get_modes / get_xcvr.
        if (_state != RadioConnectionState.Connected)
        {
            await DetectDigitalModesAsync(cfg, ct);
            try
            {
                var xcvr = await XmlRpcCallReturningStringAsync(cfg, "rig.get_xcvr", null, ct);
                _rigModel = xcvr?.Trim() ?? "";
            }
            catch { _rigModel = ""; }

            await _hubContext.BroadcastRadioDiscovered(new RadioDiscoveredEvent(
                Id: FlrigRadioId,
                Type: RadioType.Flrig,
                Model: string.IsNullOrEmpty(_rigModel) ? "flrig" : _rigModel,
                IpAddress: cfg.Host,
                Port: cfg.Port,
                Nickname: null));
            await SetStateAsync(RadioConnectionState.Connected);
        }

        if (freqHz != _lastFreqHz || normalizedMode != _lastMode)
        {
            _lastFreqHz = freqHz;
            _lastMode = normalizedMode;
            await _hubContext.BroadcastRadioStateChanged(new RadioStateChangedEvent(
                RadioId: FlrigRadioId,
                FrequencyHz: freqHz,
                Mode: normalizedMode,
                IsTransmitting: false,
                Band: BandFromHz(freqHz),
                SliceOrInstance: null));
        }
    }

    private async Task DetectDigitalModesAsync(FlrigSettings cfg, CancellationToken ct)
    {
        try
        {
            var modesRaw = await XmlRpcCallReturningStringAsync(cfg, "rig.get_modes", null, ct) ?? "";
            if (string.IsNullOrWhiteSpace(modesRaw)) return;
            var avail = new HashSet<string>(
                modesRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
                StringComparer.OrdinalIgnoreCase);
            foreach (var candidate in DigitalUsbCandidates)
            {
                if (avail.Contains(candidate)) { _detectedDigitalUsb = candidate; break; }
            }
            foreach (var candidate in DigitalLsbCandidates)
            {
                if (avail.Contains(candidate)) { _detectedDigitalLsb = candidate; break; }
            }
            _logger.LogInformation("flrig digital modes detected — USB: {Usb}  LSB: {Lsb}",
                _detectedDigitalUsb, _detectedDigitalLsb);
        }
        catch
        {
            // Older flrig builds may not expose get_modes — keep defaults.
        }
    }

    // ── Mode name translation ────────────────────────────────────────────

    private static string MapFlrigModeToApp(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "";
        var key = raw.Trim().ToUpperInvariant();
        return ModeIn.TryGetValue(key, out var mapped) ? mapped : key;
    }

    private string MapAppModeToFlrig(string appMode, FlrigSettings cfg)
    {
        var upper = appMode.Trim().ToUpperInvariant();

        // RTTY special-case — user override wins, else send flrig's native
        // "RTTY" (which is the correct native-radio-RTTY mode). This mirrors
        // v1.x behavior for AFSK RTTY-via-fldigi setups.
        if (upper == "RTTY" || upper == "RTTY-R" || upper == "RTTYR")
        {
            if (!string.IsNullOrWhiteSpace(cfg.RttyMode))
                return cfg.RttyMode!.Trim();
            return upper == "RTTY" ? "RTTY" : "RTTY-R";
        }

        // Digital passthrough — user override wins, else use the digital
        // mode name we auto-detected on connect.
        if (DigitalUsbAppModes.Contains(upper))
        {
            if (!string.IsNullOrWhiteSpace(cfg.DigitalMode))
                return cfg.DigitalMode!.Trim();
            return _detectedDigitalUsb;
        }
        if (DigitalLsbAppModes.Contains(upper))
        {
            if (!string.IsNullOrWhiteSpace(cfg.DigitalMode))
                return cfg.DigitalMode!.Trim();
            return _detectedDigitalLsb;
        }

        return ModeOut.TryGetValue(upper, out var mapped) ? mapped : upper;
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private async Task SetStateAsync(RadioConnectionState newState, string? errorMessage = null)
    {
        if (_state == newState) return;
        _state = newState;
        await _hubContext.BroadcastRadioConnectionStateChanged(
            new RadioConnectionStateChangedEvent(FlrigRadioId, newState, errorMessage));
    }

    private static string BandFromHz(long hz)
    {
        // Coarse HF/6m/2m/70cm band string — matches the strings the app's
        // other rig services return so the frontend maps cleanly.
        double mhz = hz / 1_000_000.0;
        return mhz switch
        {
            >= 0.135 and <= 0.138  => "2200m",
            >= 0.472 and <= 0.479  => "630m",
            >= 1.800 and <= 2.000  => "160m",
            >= 3.500 and <= 4.000  => "80m",
            >= 5.330 and <= 5.410  => "60m",
            >= 7.000 and <= 7.300  => "40m",
            >= 10.100 and <= 10.150 => "30m",
            >= 14.000 and <= 14.350 => "20m",
            >= 18.068 and <= 18.168 => "17m",
            >= 21.000 and <= 21.450 => "15m",
            >= 24.890 and <= 24.990 => "12m",
            >= 28.000 and <= 29.700 => "10m",
            >= 50.000 and <= 54.000 => "6m",
            >= 144.000 and <= 148.000 => "2m",
            >= 222.000 and <= 225.000 => "1.25m",
            >= 420.000 and <= 450.000 => "70cm",
            _ => "?",
        };
    }

    // ── Minimal XML-RPC client ──────────────────────────────────────────
    //
    // flrig's XML-RPC surface is tiny (six calls in the whole app). Rather
    // than pull in a general XML-RPC NuGet package, we build the request
    // envelope by hand and parse the response with XDocument. Requests
    // that take a single param (set_vfoA / set_mode) pass an XElement for
    // that <value>; get_* calls pass null.

    private async Task XmlRpcCallAsync(FlrigSettings cfg, string method, XElement? paramValue, CancellationToken ct)
    {
        await XmlRpcCallReturningStringAsync(cfg, method, paramValue, ct);
    }

    private async Task<string?> XmlRpcCallReturningStringAsync(FlrigSettings cfg, string method, XElement? paramValue, CancellationToken ct)
    {
        var http = _httpClientFactory.CreateClient();
        http.Timeout = TimeSpan.FromSeconds(4);

        var paramsElem = new XElement("params");
        if (paramValue != null)
        {
            paramsElem.Add(new XElement("param", paramValue));
        }
        var envelope = new XDocument(
            new XDeclaration("1.0", null, null),
            new XElement("methodCall",
                new XElement("methodName", method),
                paramsElem));
        var body = envelope.Declaration + envelope.ToString();

        var url = $"http://{cfg.Host}:{cfg.Port}/RPC2";
        using var content = new StringContent(body, Encoding.UTF8, "text/xml");
        using var resp = await http.PostAsync(url, content, ct);
        if (!resp.IsSuccessStatusCode)
            throw new InvalidOperationException($"flrig XML-RPC {method}: HTTP {(int)resp.StatusCode}");

        var respXml = await resp.Content.ReadAsStringAsync(ct);
        var doc = XDocument.Parse(respXml);

        // Fault response: <methodResponse><fault><value><struct>...</struct></value></fault></methodResponse>
        var fault = doc.Root?.Element("fault");
        if (fault != null)
        {
            var faultText = fault.Descendants("string").FirstOrDefault()?.Value ?? "flrig fault";
            throw new InvalidOperationException($"flrig XML-RPC {method}: {faultText}");
        }

        // Success response: <methodResponse><params><param><value>...</value></param></params></methodResponse>
        var valueElem = doc.Root?.Element("params")?.Element("param")?.Element("value");
        if (valueElem == null) return null;

        // Pick out the typed child (<string>/<int>/<i4>/<double>/etc.) or,
        // per XML-RPC's default-string convention, the raw text.
        var typed = valueElem.Elements().FirstOrDefault();
        return (typed?.Value ?? valueElem.Value).Trim();
    }
}
