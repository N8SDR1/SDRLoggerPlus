using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Logging;

namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// Logger's QRV presence client (qrv_client.md §10.3 and §11).
/// Opt-in, default off. Heartbeats while a radio is actually tuned.
/// QRT on disable, radio disconnect, callsign change, and shutdown.
/// Hz and notes are never sent. Discord is never called.
/// </summary>
public sealed class QrvPresenceClient
{
    private readonly HttpClient _http;
    private readonly ILogger<QrvPresenceClient>? _logger;
    private readonly Func<DateTimeOffset> _clock;
    private readonly object _gate = new();
    private readonly SemaphoreSlim _tick = new(1, 1);

    private bool _enabled;
    private string _apiBase = QrvProtocol.DefaultApiBase;
    private string _stationCall = "";
    private string? _installKey;
    private string? _boundCall;
    private string? _loggingModeRaw;
    private string? _radioModeRaw;
    private string? _preferredRadioId;
    private bool _radioConnected;
    private long _frequencyHz;
    private bool _onAir;
    private bool _hold;
    private bool _postedThisSession;
    private bool _heartbeatInFlight;
    private bool _qrtAfterFlight;
    private bool _rosterWatching;
    private bool _rosterInFlight;
    private string? _error;
    private string? _rosterError;
    private string? _publishedCall;
    private string? _publishedBand;
    private string? _publishedMode;
    private string? _observedBand = "";
    private string? _observedMode = "";
    private string? _modeForHold;
    private string? _lastSentBand;
    private string? _lastSentMode;
    private DateTimeOffset _lastPostUtc = DateTimeOffset.MinValue;
    private DateTimeOffset? _debounceUntil;
    private DateTimeOffset _lastRosterUtc = DateTimeOffset.MinValue;
    private DateTimeOffset _rosterWatchUtc = DateTimeOffset.MinValue;
    private QueuedQrt? _queuedQrt;
    private QrvKeyUpdate? _pendingKey;
    private IReadOnlyList<QrvRosterEntry> _roster = Array.Empty<QrvRosterEntry>();

    public QrvPresenceClient(HttpClient http, ILogger<QrvPresenceClient>? logger = null, Func<DateTimeOffset>? clock = null)
    {
        _http = http;
        _logger = logger;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
    }

    public string? PreferredRadioId
    {
        get { lock (_gate) return _preferredRadioId; }
    }

    /// <summary>
    /// Call and install key the presence heartbeat is using. Totals reuse this
    /// key and do not invent another. Call is already normalized.
    /// </summary>
    public QrvCredentials Credentials()
    {
        lock (_gate) return new QrvCredentials(_apiBase, _stationCall, _installKey);
    }

    public void ApplySettings(bool enabled, string? baseUrl, string? stationCall, string? storedKey, string? storedBoundCall)
    {
        lock (_gate)
        {
            if (_pendingKey == null && string.IsNullOrEmpty(_installKey) && QrvProtocol.IsInstallKey(storedKey))
            {
                _installKey = storedKey!.Trim().ToLowerInvariant();
                var bound = QrvProtocol.NormalizeCall(storedBoundCall);
                if (!string.IsNullOrEmpty(bound))
                    _boundCall = bound;
            }

            var call = QrvProtocol.NormalizeCall(stationCall) ?? "";
            if (!string.IsNullOrEmpty(_stationCall) && _stationCall != call && !string.IsNullOrEmpty(_installKey))
            {
                var qrtCall = !string.IsNullOrEmpty(_boundCall) ? _boundCall : _stationCall;
                _queuedQrt = new QueuedQrt(qrtCall, _installKey);
                if (_heartbeatInFlight)
                    _qrtAfterFlight = true;
                _installKey = null;
                _boundCall = null;
                _pendingKey = new QrvKeyUpdate(null, null);
                _onAir = false;
                _postedThisSession = false;
                _hold = false;
                _debounceUntil = null;
            }
            _stationCall = call;
            _apiBase = QrvProtocol.NormalizeApiBase(baseUrl);

            if (_enabled && !enabled)
            {
                _hold = false;
                _debounceUntil = null;
                _error = null;
                _rosterError = null;
                ArmQrt();
            }
            else if (!_enabled && enabled)
            {
                _hold = false;
                _error = null;
            }
            _enabled = enabled;
            NoteModeHold();
        }
    }

    public void ReportLoggingMode(string? mode, string? radioId)
    {
        lock (_gate)
        {
            _loggingModeRaw = string.IsNullOrWhiteSpace(mode) ? null : mode.Trim();
            if (!string.IsNullOrWhiteSpace(radioId))
                _preferredRadioId = radioId.Trim();
            NoteModeHold();
            NoteDebounce();
            if (_onAir && !PhysicallyQrv())
                ArmQrt();
        }
    }

    public void SetRadio(bool connected, long frequencyHz, string? mode)
    {
        lock (_gate)
        {
            if (connected && !_radioConnected)
                _hold = false;
            _radioConnected = connected;
            _frequencyHz = connected ? frequencyHz : 0;
            _radioModeRaw = string.IsNullOrWhiteSpace(mode) ? null : mode.Trim();
            NoteModeHold();
            NoteDebounce();
            if (_onAir && !PhysicallyQrv())
                ArmQrt();
            else if (_postedThisSession && !PhysicallyQrv() && _enabled)
                ArmQrt();
        }
    }

    public void TouchRosterWatch()
    {
        lock (_gate)
        {
            _rosterWatching = true;
            _rosterWatchUtc = _clock();
        }
    }

    public void SetRosterWatching(bool on)
    {
        lock (_gate)
        {
            _rosterWatching = on;
            if (on) _rosterWatchUtc = _clock();
        }
    }

    public QrvKeyUpdate? PeekKeyUpdate()
    {
        lock (_gate) return _pendingKey;
    }

    public void AcknowledgeKeyUpdate(QrvKeyUpdate update)
    {
        lock (_gate)
        {
            if (_pendingKey == update)
                _pendingKey = null;
        }
    }

    public QrvStatusSnapshot Status()
    {
        lock (_gate) return SnapshotLocked();
    }

    public async Task TickAsync(CancellationToken cancellationToken)
    {
        await _tick.WaitAsync(cancellationToken);
        try
        {
            await PublishPendingAsync(cancellationToken);
        }
        finally
        {
            _tick.Release();
        }
    }

    public async Task ShutdownAsync(CancellationToken cancellationToken)
    {
        await _tick.WaitAsync(cancellationToken);
        try
        {
            QueuedQrt? qrt;
            string apiBase;
            lock (_gate)
            {
                _enabled = false;
                _debounceUntil = null;
                _onAir = false;
                if (!string.IsNullOrEmpty(_installKey) && _postedThisSession && _queuedQrt == null)
                {
                    var call = !string.IsNullOrEmpty(_boundCall) ? _boundCall : _stationCall;
                    if (!string.IsNullOrEmpty(call))
                        _queuedQrt = new QueuedQrt(call, _installKey);
                }
                qrt = TakeQrt();
                apiBase = _apiBase;
            }
            if (qrt != null)
                await SendQrtAsync(qrt, apiBase, cancellationToken);
        }
        finally
        {
            _tick.Release();
        }
    }

    private async Task PublishPendingAsync(CancellationToken cancellationToken)
    {
        QueuedQrt? qrt;
        PlannedHeartbeat? heartbeat;
        bool roster;
        string apiBase;
        lock (_gate)
        {
            qrt = TakeQrt();
            heartbeat = PlanHeartbeat();
            roster = PlanRoster();
            apiBase = _apiBase;
        }

        if (qrt != null)
            await SendQrtAsync(qrt, apiBase, cancellationToken);
        if (heartbeat != null)
            await SendHeartbeatAsync(heartbeat, cancellationToken);

        lock (_gate)
        {
            qrt = TakeQrt();
            apiBase = _apiBase;
        }
        if (qrt != null)
            await SendQrtAsync(qrt, apiBase, cancellationToken);
        if (roster)
            await FetchRosterAsync(apiBase, cancellationToken);
    }

    private PlannedHeartbeat? PlanHeartbeat()
    {
        if (!_enabled || _hold || _heartbeatInFlight || !PhysicallyQrv())
            return null;

        var now = _clock();
        var first = _lastPostUtc == DateTimeOffset.MinValue;
        var since = first ? TimeSpan.MaxValue : now - _lastPostUtc;
        var debounceDue = _debounceUntil is DateTimeOffset due && now >= due && since >= QrvProtocol.MinPostInterval;
        var periodicDue = since >= QrvProtocol.HeartbeatPeriod;
        if (!first && !debounceDue && !periodicDue)
            return null;

        var call = QrvProtocol.NormalizeCall(_stationCall)!;
        var band = QrvProtocol.BandForHz(_frequencyHz)!;
        var mode = EffectiveMode()!;
        _lastPostUtc = now;
        _debounceUntil = null;
        _lastSentBand = band;
        _lastSentMode = mode;
        _heartbeatInFlight = true;
        _postedThisSession = true;
        return new PlannedHeartbeat(call, band, mode, _installKey, _apiBase);
    }

    private bool PlanRoster()
    {
        if (_rosterInFlight) return false;
        var now = _clock();
        if (_rosterWatching && now - _rosterWatchUtc > QrvProtocol.RosterWatchTtl)
            _rosterWatching = false;
        if (!_rosterWatching) return false;
        if (_lastRosterUtc != DateTimeOffset.MinValue)
        {
            var since = now - _lastRosterUtc;
            if (since < QrvProtocol.MinRosterInterval || since < QrvProtocol.RosterPeriod)
                return false;
        }
        _lastRosterUtc = now;
        _rosterInFlight = true;
        return true;
    }

    private QueuedQrt? TakeQrt()
    {
        var qrt = _queuedQrt;
        _queuedQrt = null;
        return qrt;
    }

    private async Task SendHeartbeatAsync(PlannedHeartbeat heartbeat, CancellationToken cancellationToken)
    {
        var url = QrvProtocol.EndpointUrl(heartbeat.ApiBase, "heartbeat.php");
        var json = QrvProtocol.EncodeHeartbeat(heartbeat.Call, heartbeat.Band, heartbeat.Mode, heartbeat.InstallKey);
        _logger?.LogInformation("QRV heartbeat {Call} {Band} {Mode}", heartbeat.Call, heartbeat.Band, heartbeat.Mode);
        var (status, body) = await PostAsync(url, json, cancellationToken);

        lock (_gate)
        {
            _heartbeatInFlight = false;
            if (status <= 0)
            {
                _error = "QRV unavailable";
                MaybeQrtAfterFlight();
                return;
            }

            var parsed = QrvProtocol.ParseWrite(body, status);
            if (parsed.Ok)
            {
                _error = null;
                _publishedCall = heartbeat.Call;
                _publishedBand = heartbeat.Band;
                _publishedMode = heartbeat.Mode;
                _onAir = true;
                RememberIssuedKey(parsed.InstallKey, heartbeat.Call);
                if (_qrtAfterFlight || !_enabled || !PhysicallyQrv())
                {
                    _qrtAfterFlight = false;
                    _onAir = false;
                    QueueQrt();
                }
                return;
            }

            _error = string.IsNullOrEmpty(parsed.Error) ? "QRV unavailable" : parsed.Error;
            if (ShouldHold(status, parsed.Error))
            {
                _hold = true;
                _onAir = false;
                _logger?.LogWarning("QRV heartbeat rejected {Status} {Error}", status, _error);
            }
            else if (status == 429 || parsed.Error == "rate_limited")
            {
                _logger?.LogInformation("QRV heartbeat rate limited");
            }
            MaybeQrtAfterFlight();
        }
    }

    private async Task SendQrtAsync(QueuedQrt qrt, string apiBase, CancellationToken cancellationToken)
    {
        var url = QrvProtocol.EndpointUrl(apiBase, "qrt.php");
        var json = QrvProtocol.EncodeQrt(qrt.Call, qrt.InstallKey);
        _logger?.LogInformation("QRV QRT {Call} key {KeyPrefix}", qrt.Call, QrvProtocol.KeyPrefix(qrt.InstallKey));
        var (status, body) = await PostAsync(url, json, cancellationToken);
        lock (_gate)
        {
            _postedThisSession = false;
            _onAir = false;
            if (status <= 0)
            {
                _error = "QRV unavailable";
                return;
            }
            var parsed = QrvProtocol.ParseWrite(body, status);
            if (!parsed.Ok && !string.IsNullOrEmpty(parsed.Error))
                _error = parsed.Error;
        }
    }

    private async Task FetchRosterAsync(string apiBase, CancellationToken cancellationToken)
    {
        var url = QrvProtocol.EndpointUrl(apiBase, "roster.php");
        int status;
        string body;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            using var response = await _http.SendAsync(request, cancellationToken);
            status = (int)response.StatusCode;
            body = await response.Content.ReadAsStringAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            status = 0;
            body = "";
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            _logger?.LogWarning(ex, "QRV roster request failed");
            status = 0;
            body = "";
        }

        lock (_gate)
        {
            _rosterInFlight = false;
            if (status <= 0)
            {
                _rosterError = "QRV unavailable";
                return;
            }
            var parsed = QrvProtocol.ParseRoster(body, status);
            if (parsed.Ok)
            {
                _roster = parsed.Entries;
                _rosterError = null;
                return;
            }
            // Keep the last good roster. A failed GET must not blank the board.
            _rosterError = string.IsNullOrEmpty(parsed.Error) ? "QRV unavailable" : parsed.Error;
        }
    }

    private void MaybeQrtAfterFlight()
    {
        if (!_qrtAfterFlight) return;
        _qrtAfterFlight = false;
        _onAir = false;
        QueueQrt();
    }

    private void RememberIssuedKey(string? issued, string call)
    {
        if (!QrvProtocol.IsInstallKey(issued)) return;
        var key = issued!.Trim().ToLowerInvariant();
        if (_installKey == key && _boundCall == call) return;
        _installKey = key;
        _boundCall = call;
        _pendingKey = new QrvKeyUpdate(key, call);
        _logger?.LogInformation("QRV install key stored {KeyPrefix}", QrvProtocol.KeyPrefix(key));
    }

    private void ArmQrt()
    {
        if (_heartbeatInFlight)
        {
            _qrtAfterFlight = true;
            _onAir = false;
            return;
        }
        if (!_postedThisSession && !_onAir)
            return;
        QueueQrt();
        _onAir = false;
    }

    private void QueueQrt()
    {
        if (_queuedQrt != null || string.IsNullOrEmpty(_installKey)) return;
        var call = !string.IsNullOrEmpty(_boundCall) ? _boundCall : _stationCall;
        if (string.IsNullOrEmpty(call)) return;
        _queuedQrt = new QueuedQrt(call, _installKey);
    }

    private bool PhysicallyQrv()
    {
        return _enabled
            && _radioConnected
            && _frequencyHz > 0
            && !string.IsNullOrEmpty(_stationCall)
            && QrvProtocol.BandForHz(_frequencyHz) != null
            && EffectiveMode() != null;
    }

    private string? EffectiveMode()
    {
        var logged = QrvProtocol.ModeForLogger(_loggingModeRaw);
        if (!string.IsNullOrEmpty(logged)) return logged;
        return QrvProtocol.ModeForLogger(_radioModeRaw);
    }

    private void NoteModeHold()
    {
        var mode = EffectiveMode();
        if (mode == _modeForHold) return;
        _modeForHold = mode;
        _hold = false;
    }

    private void NoteDebounce()
    {
        var band = QrvProtocol.BandForHz(_frequencyHz) ?? "";
        var mode = EffectiveMode() ?? "";
        var changed = band != (_observedBand ?? "") || mode != (_observedMode ?? "");
        _observedBand = band;
        _observedMode = mode;
        if (!changed || !_onAir) return;
        if (band.Length == 0 && mode.Length == 0) return;
        _debounceUntil = _clock().Add(QrvProtocol.BandModeDebounce);
    }

    private QrvStatusSnapshot SnapshotLocked()
    {
        var visibleError = _error ?? _rosterError;
        var phase = !_enabled
            ? "off"
            : !string.IsNullOrEmpty(visibleError)
                ? "error"
                : _onAir
                    ? "on-air"
                    : "waiting";
        return new QrvStatusSnapshot
        {
            Enabled = _enabled,
            OnAir = _onAir,
            Phase = phase,
            Call = _publishedCall ?? (string.IsNullOrEmpty(_stationCall) ? null : _stationCall),
            Band = _publishedBand ?? QrvProtocol.BandForHz(_frequencyHz),
            Mode = _publishedMode ?? EffectiveMode(),
            Error = _error ?? _rosterError,
            Detail = DetailLocked(),
            InstallKeyPrefix = QrvProtocol.KeyPrefix(_installKey),
            Roster = _roster,
        };
    }

    private string DetailLocked()
    {
        if (!_enabled) return "Presence is off.";
        if (_hold && !string.IsNullOrEmpty(_error))
            return "QRV rejected the last update. Change mode, or turn QRV off and on, to try again.";
        if (string.IsNullOrEmpty(_stationCall))
            return "Set a station callsign to publish.";
        if (!_radioConnected)
            return "Radio is not connected.";
        if (_frequencyHz <= 0)
            return "No frequency from the radio.";
        if (QrvProtocol.BandForHz(_frequencyHz) == null)
            return "This frequency is not a QRV band.";
        if (EffectiveMode() == null)
            return "QRV has no token for this mode.";
        if (!string.IsNullOrEmpty(_error))
            return "QRV unavailable.";
        return _onAir ? "On the air." : "Waiting to publish.";
    }

    private async Task<(int Status, string Body)> PostAsync(string url, string json, CancellationToken cancellationToken)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            var content = new StringContent(json, Encoding.UTF8);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
            request.Content = content;
            using var response = await _http.SendAsync(request, cancellationToken);
            var body = response.Content == null ? "" : await response.Content.ReadAsStringAsync(cancellationToken);
            return ((int)response.StatusCode, body);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return (0, "");
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            _logger?.LogWarning(ex, "QRV request failed");
            return (0, "");
        }
    }

    private static bool ShouldHold(int status, string? error) =>
        status is 400 or 401 or 403
        || error is "bad_key" or "call_mismatch" or "forbidden" or "invalid_fields" or "invalid_hz";

    private sealed record QueuedQrt(string Call, string InstallKey);

    private sealed record PlannedHeartbeat(string Call, string Band, string Mode, string? InstallKey, string ApiBase);
}
