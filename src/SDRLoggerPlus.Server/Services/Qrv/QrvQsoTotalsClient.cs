using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Logging;

namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// Posts QSO-total snapshots to qso.php. Opt-in, default off, separate from
/// presence. Reuses the presence install key. Sends nothing while the switch
/// is off. One zero snapshot when the operator turns it off: counts of 0 and
/// <c>last</c> of "" for week, month, and year.
/// </summary>
public sealed class QrvQsoTotalsClient
{
    private readonly HttpClient _http;
    private readonly ILogger<QrvQsoTotalsClient>? _logger;
    private readonly Func<DateTimeOffset> _clock;
    private readonly object _gate = new();
    private readonly SemaphoreSlim _tick = new(1, 1);

    private bool _enabled;
    private string _apiBase = QrvProtocol.DefaultApiBase;
    private string _call = "";
    private string? _installKey;
    private QrvQsoTotals.Snapshot _counts = QrvQsoTotals.Snapshot.Zero;
    private bool _dirty;
    private bool _zeroPending;
    private bool _inFlight;
    private string? _hold;
    private string? _error;
    private string? _rejectedJson;
    private string? _periods;
    private DateTimeOffset _nextUtc = DateTimeOffset.MinValue;
    private DateTimeOffset _lastOkUtc = DateTimeOffset.MinValue;

    public QrvQsoTotalsClient(HttpClient http, ILogger<QrvQsoTotalsClient>? logger = null, Func<DateTimeOffset>? clock = null)
    {
        _http = http;
        _logger = logger;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
    }

    public void ApplySettings(bool enabled, string? baseUrl, string? stationCall, string? installKey)
    {
        lock (_gate)
        {
            var call = QrvProtocol.NormalizeCall(stationCall) ?? "";
            var key = QrvProtocol.IsInstallKey(installKey) ? installKey!.Trim().ToLowerInvariant() : null;
            var keyChanged = key != _installKey;
            var callChanged = call != _call;
            _apiBase = QrvProtocol.NormalizeApiBase(baseUrl);
            _call = call;
            _installKey = key;

            if (_hold == "bad_key" && keyChanged && key != null)
                _hold = null;
            if (_hold is "call_mismatch" or "invalid_fields" && (keyChanged || callChanged))
                _hold = null;

            if (_enabled && !enabled)
            {
                _enabled = false;
                _dirty = false;
                _error = null;
                if (_hold == "forbidden")
                {
                    _zeroPending = false;
                }
                else
                {
                    _hold = null;
                    _zeroPending = key != null && call.Length > 0;
                }
            }
            else if (!_enabled && enabled)
            {
                _enabled = true;
                _zeroPending = false;
                // A ban stays stopped until the process restarts.
                if (_hold != "forbidden")
                {
                    _error = null;
                    _dirty = true;
                }
            }

            if (_enabled && (keyChanged || callChanged))
                _dirty = true;
        }
    }

    public void SetCounts(QrvQsoTotals.Snapshot counts)
    {
        lock (_gate)
        {
            if (counts.Equals(_counts)) return;
            _counts = counts;
            if (_enabled) _dirty = true;
        }
    }

    public QrvTotalsStatus Status()
    {
        lock (_gate) return SnapshotLocked();
    }

    public async Task TickAsync(CancellationToken cancellationToken)
    {
        await _tick.WaitAsync(cancellationToken);
        try
        {
            if (!TryPlan(out var plan)) return;
            await SendAsync(plan, cancellationToken);
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
            if (!TryPlan(out var plan)) return;
            await SendAsync(plan, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // App is exiting. A missed zero ages off the board on the server's next edit.
        }
        finally
        {
            _tick.Release();
        }
    }

    private bool TryPlan(out PlannedPost plan)
    {
        lock (_gate)
        {
            plan = default;
            var now = _clock();
            if (_inFlight || now < _nextUtc) return false;

            if (!_enabled)
            {
                if (!_zeroPending || _installKey == null || _call.Length == 0)
                    return false;
                plan = new PlannedPost(Encode(QrvQsoTotals.Snapshot.Zero), true);
                _inFlight = true;
                _nextUtc = now.Add(QrvQsoTotals.MinPostInterval);
                return true;
            }

            if (_hold is "forbidden" or "bad_key" or "call_mismatch") return false;
            if (_installKey == null || _call.Length == 0) return false;

            var periodic = _lastOkUtc != DateTimeOffset.MinValue
                && now - _lastOkUtc >= QrvQsoTotals.RefreshPeriod;
            if (!_dirty && !periodic) return false;

            var json = Encode(_counts);
            if (json == _rejectedJson)
            {
                _dirty = false;
                return false;
            }

            _hold = null;

            plan = new PlannedPost(json, false);
            _inFlight = true;
            _nextUtc = now.Add(QrvQsoTotals.MinPostInterval);
            return true;
        }
    }

    private string Encode(QrvQsoTotals.Snapshot counts) =>
        QrvQsoTotals.Encode(_call, _installKey!, counts);

    private async Task SendAsync(PlannedPost plan, CancellationToken cancellationToken)
    {
        var url = QrvProtocol.EndpointUrl(_apiBase, "qso.php");
        _logger?.LogInformation(
            "QRV QSO totals {Call} key {KeyPrefix} zeros {Zeros}",
            _call, QrvProtocol.KeyPrefix(_installKey), plan.Zeros);
        var (status, body) = await PostAsync(url, plan.Json, cancellationToken);

        lock (_gate)
        {
            _inFlight = false;
            var parsed = QrvTotalsWriteResultOrUnavailable(body, status);
            if (parsed.Ok)
            {
                _error = null;
                _rejectedJson = null;
                _lastOkUtc = _clock();
                _periods = JoinPeriods(parsed);
                if (plan.Zeros)
                    _zeroPending = false;
                else if (plan.Json == Encode(_counts))
                    _dirty = false;
                else
                    _dirty = true;
                return;
            }

            _error = parsed.Error;
            if (status == 429 || parsed.Error == "rate_limited")
            {
                _logger?.LogInformation("QRV QSO totals rate limited");
                return;
            }

            if (status == 500 || parsed.Error == "store_failed" || status <= 0)
            {
                _nextUtc = _clock().Add(QrvQsoTotals.StoreFailedRetry);
                _logger?.LogWarning("QRV QSO totals will retry {Status} {Error}", status, _error);
                return;
            }

            if (status == 400 || parsed.Error == "invalid_fields")
            {
                _rejectedJson = plan.Json;
                _dirty = false;
                if (plan.Zeros) _zeroPending = false;
                _hold = "invalid_fields";
                _logger?.LogWarning("QRV QSO totals rejected {Status} {Error}", status, _error);
                return;
            }

            if (status == 401 || parsed.Error == "bad_key")
            {
                _hold = "bad_key";
                _dirty = false;
                if (plan.Zeros) _zeroPending = false;
                _logger?.LogWarning("QRV QSO totals bad install key");
                return;
            }

            if (status == 403 && parsed.Error == "call_mismatch")
            {
                _hold = "call_mismatch";
                _dirty = false;
                if (plan.Zeros) _zeroPending = false;
                _logger?.LogWarning("QRV QSO totals call does not match the install key");
                return;
            }

            if (status == 403 || parsed.Error == "forbidden")
            {
                _hold = "forbidden";
                _dirty = false;
                _zeroPending = false;
                _logger?.LogWarning("QRV QSO totals forbidden — stopped");
                return;
            }

            _nextUtc = _clock().Add(QrvQsoTotals.StoreFailedRetry);
        }
    }

    private static QrvTotalsWriteResult QrvTotalsWriteResultOrUnavailable(string body, int status) =>
        QrvQsoTotals.ParseWrite(body, status);

    private static string? JoinPeriods(QrvTotalsWriteResult parsed)
    {
        if (parsed.WeekPeriod == null && parsed.MonthPeriod == null && parsed.YearPeriod == null)
            return null;
        return $"{parsed.WeekPeriod} {parsed.MonthPeriod} {parsed.YearPeriod}".Trim();
    }

    private QrvTotalsStatus SnapshotLocked()
    {
        string phase;
        if (!_enabled && !_zeroPending)
            phase = "off";
        else if (_hold == "forbidden")
            phase = "stopped";
        else if (_enabled && _installKey == null)
            phase = "needs-key";
        else if (_enabled && _call.Length == 0)
            phase = "needs-call";
        else if (!string.IsNullOrEmpty(_error) || _hold != null)
            phase = "error";
        else if (_lastOkUtc != DateTimeOffset.MinValue && !_dirty && !_zeroPending)
            phase = "posted";
        else
            phase = "waiting";

        return new QrvTotalsStatus
        {
            Enabled = _enabled,
            Phase = phase,
            Detail = DetailLocked(phase),
            Error = _error,
            WeekVoice = _counts.Week.Voice,
            WeekCw = _counts.Week.Cw,
            WeekDigital = _counts.Week.Digital,
        };
    }

    private string DetailLocked(string phase) => phase switch
    {
        "off" => "QSO totals are off. Nothing is sent.",
        "needs-key" => "A presence heartbeat has to succeed once before totals are accepted. The install key is not invented here.",
        "needs-call" => "Set the station callsign the presence heartbeat uses.",
        "stopped" => "QRV refused this install. Totals are stopped.",
        "error" when _hold == "bad_key" => "The install key was rejected. Run a presence heartbeat. A new key is not invented here.",
        "error" when _hold == "call_mismatch" => "That install key is bound to a different callsign. Totals use the presence heartbeat's call.",
        "error" when _hold == "invalid_fields" => "QRV rejected the totals payload. It will not be sent again unchanged.",
        "error" => "QRV totals are unavailable. The latest snapshot will be sent later.",
        "posted" => _periods == null
            ? "Totals posted."
            : $"Totals posted ({_periods}).",
        _ => _zeroPending
            ? "Sending zeros so this call leaves the board."
            : "Waiting to post the latest totals.",
    };

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
            _logger?.LogWarning(ex, "QRV QSO totals request failed");
            return (0, "");
        }
    }

    private readonly record struct PlannedPost(string Json, bool Zeros);
}

public sealed class QrvTotalsStatus
{
    public bool Enabled { get; init; }
    public string Phase { get; init; } = "off";
    public string? Detail { get; init; }
    public string? Error { get; init; }
    public int WeekVoice { get; init; }
    public int WeekCw { get; init; }
    public int WeekDigital { get; init; }
}
