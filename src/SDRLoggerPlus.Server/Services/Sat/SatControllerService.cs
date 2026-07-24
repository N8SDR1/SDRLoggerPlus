using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using MongoDB.Bson;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services.Sat;

public record SatPassQso(string SatName, string Callsign, string Grid, string Mode, DateTime TimeUtc);
public record SatEvent(DateTime TimeUtc, string Kind, string Detail);

public record SatState(
    bool Active,
    string Status,            // idle | tracking | aos | los
    string? Serial,
    string? Firmware,
    string? Satellite,
    string? CatalogNumber,
    string? Transponder,
    string? UplinkFreq,
    string? UplinkMode,
    string? DownlinkFreq,
    string? DownlinkMode,
    string? AosAzimuth,
    string? LosAzimuth,
    DateTime? AosTimeUtc,
    DateTime? LastHeardUtc,
    List<SatPassQso> PassQsos,
    List<SatEvent> Events,
    SatMapInfo? Map,
    string? Error,
    // v1.x SAT-panel parity fields — the CSN /track poll already returns
    // these; we now stash them on the state so the frontend can render
    // Az/El, Range, Max El, and Time-to-AOS/LOS the way the old SDRLogger+
    // Satellite Status panel did.
    double? AzDeg,
    double? ElDeg,
    double? RangeKm,
    double? MaxElDeg,
    double? TimeToAosSec,
    double? TimeToLosSec,
    // Live Doppler-corrected uplink/downlink (Hz) for the ACTIVE transponder,
    // from the /track poll — the display follows these while the nominal
    // UplinkFreq/DownlinkFreq above are what a logged QSO records.
    string? UplinkFreqLive,
    string? DownlinkFreqLive,
    // Satellite altitude + footprint diameter (km) straight from the /track feed.
    double? AltitudeKm,
    double? FootprintKm,
    // Extra CSN /track telemetry surfaced in the Satellite Status panel:
    // per-leg Doppler (Hz), receiver signal (dBm), the antenna ROTOR look-angle
    // (distinct from the satellite's), and the satellite sub-point (deg).
    double? DopplerUpHz,
    double? DopplerDownHz,
    double? Rssi,
    double? AntAzDeg,
    double? AntElDeg,
    double? SubLatDeg,
    double? SubLonDeg);

public record SatMapInfo(double Lat, double Lon, double AltKm, double FootprintRadiusKm);

/// <summary>
/// CSN Technologies S.A.T. controller integration (SDRLogger+ port).
/// While activated: listens for "SAT,..." UDP broadcasts (port 9932), the
/// controller's ADIF-over-UDP QSO push (port 1100), and polls the
/// controller's /track endpoint for live az/el/range. SAT QSOs are
/// auto-logged with SAT_NAME / PROP_MODE / FREQ_RX preserved as ADIF extras
/// so LoTW satellite credit survives export. Sockets are bound only while
/// active so the ports stay free for other tools (ported behavior).
/// </summary>
public class SatControllerService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IHubContext<LogHub, ILogHubClient> _hubContext;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<SatControllerService> _logger;

    private volatile bool _active;
    // True only when the auto-activate logic turned us on, so a manual Activate is
    // never auto-deactivated out from under the operator.
    private volatile bool _autoActivated;
    // Controller's tracking state from /track: 1 = tracking a pass, 0 = idle.
    private volatile int _trackMode;
    private UdpClient? _satSocket;
    private UdpClient? _adifSocket;
    private readonly object _stateLock = new();
    private readonly Dictionary<string, DateTime> _qsoDedupe = new();

    private string _status = "idle";
    private string? _serial, _firmware, _satellite, _catno, _transponder;
    private string? _upFreq, _upMode, _downFreq, _downMode, _aosAz, _losAz, _error;
    // Live Doppler-corrected active-transponder freqs (Hz) from the /track poll.
    private string? _upFreqLive, _downFreqLive;
    private DateTime? _aosTime, _lastHeard;
    private readonly List<SatPassQso> _passQsos = new();
    private readonly Queue<SatEvent> _events = new();
    private SatMapInfo? _map;
    // Live look-angle + pass-timing fields sourced from the CSN /track poll.
    // Max-elevation is remembered across the pass (the controller only
    // reports current El) so the operator can see how high the bird will
    // ultimately go — matches v1.x SDRLogger+ Satellite Status panel.
    private double? _azDeg, _elDeg, _rangeKm, _maxElDeg, _ttAosSec, _ttLosSec;
    private double? _altKm, _footprintKm;
    private double? _dopUpHz, _dopDownHz, _rssi, _antAz, _antEl, _subLat, _subLon;

    public SatControllerService(
        IServiceProvider serviceProvider,
        IHubContext<LogHub, ILogHubClient> hubContext,
        IHttpClientFactory httpClientFactory,
        ILogger<SatControllerService> logger)
    {
        _serviceProvider = serviceProvider;
        _hubContext = hubContext;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public bool IsActive => _active;

    public async Task SetActiveAsync(bool active)
    {
        _active = active;
        // Any explicit call (i.e. the operator's Activate/Deactivate button) hands
        // ownership back to them; the auto path re-flags itself right after calling this.
        _autoActivated = false;
        if (!active)
        {
            CloseSockets();
            lock (_stateLock)
            {
                _status = "idle";
                _aosTime = null;
                // Clear pass-scoped live-tracking data so a stale look-angle
                // doesn't linger in the UI after the operator deactivates.
                _azDeg = _elDeg = _rangeKm = _maxElDeg = _ttAosSec = _ttLosSec = null;
                _altKm = _footprintKm = null;
                _dopUpHz = _dopDownHz = _rssi = _antAz = _antEl = _subLat = _subLon = null;
                _upFreqLive = _downFreqLive = null;
            }
        }
        await BroadcastStateAsync();
    }

    public SatState GetState()
    {
        lock (_stateLock)
        {
            return new SatState(_active, _status, _serial, _firmware, _satellite, _catno,
                _transponder, _upFreq, _upMode, _downFreq, _downMode, _aosAz, _losAz,
                _aosTime, _lastHeard, _passQsos.ToList(), _events.ToList(), _map, _error,
                _azDeg, _elDeg, _rangeKm, _maxElDeg, _ttAosSec, _ttLosSec,
                _upFreqLive, _downFreqLive, _altKm, _footprintKm,
                _dopUpHz, _dopDownHz, _rssi, _antAz, _antEl, _subLat, _subLon);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("S.A.T. controller service starting (inactive until enabled)");

        var lastTrackPoll = DateTime.MinValue;
        var lastPassProbe = DateTime.MinValue;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await ReadSettingsAsync();
                if (!_active || !settings.Sat.Enabled)
                {
                    CloseSockets();
                    // Follow-the-controller: while idle, optionally probe /track over HTTP
                    // (no UDP listeners bound, so the ports stay free) and activate when the
                    // controller starts a pass — or one is inside the AOS lead window.
                    if (settings.Sat.Enabled && settings.Sat.AutoActivate &&
                        !string.IsNullOrWhiteSpace(settings.Sat.ControllerIp) &&
                        DateTime.UtcNow - lastPassProbe > TimeSpan.FromSeconds(10))
                    {
                        lastPassProbe = DateTime.UtcNow;
                        if (await IsPassImminentAsync(settings.Sat.ControllerIp, settings.Sat.AutoActivateLeadSeconds, stoppingToken))
                        {
                            _logger.LogInformation("S.A.T. auto-activating — controller has a pass in progress or imminent");
                            await SetActiveAsync(true);
                            _autoActivated = true;
                        }
                    }
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    continue;
                }

                // Auto-deactivate after the pass — but only if WE activated. A manual
                // Activate stays on until the operator turns it off.
                if (_autoActivated && settings.Sat.AutoActivate && _trackMode == 0 && !IsAosWithin(settings.Sat.AutoActivateLeadSeconds))
                {
                    _logger.LogInformation("S.A.T. auto-deactivating — pass complete");
                    _autoActivated = false;
                    await SetActiveAsync(false);
                    continue;
                }

                EnsureSockets(settings.Sat);
                var receiveTasks = new List<Task>();
                if (_satSocket != null) receiveTasks.Add(ReceiveSatAsync(_satSocket, stoppingToken));
                if (_adifSocket != null) receiveTasks.Add(ReceiveAdifAsync(_adifSocket, stoppingToken));

                // Poll /track every 2 s while active
                if (DateTime.UtcNow - lastTrackPoll > TimeSpan.FromSeconds(2) &&
                    !string.IsNullOrWhiteSpace(settings.Sat.ControllerIp))
                {
                    lastTrackPoll = DateTime.UtcNow;
                    receiveTasks.Add(PollTrackAsync(settings.Sat.ControllerIp, settings, stoppingToken));
                }

                if (receiveTasks.Count == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                    continue;
                }
                await Task.WhenAny(Task.WhenAll(receiveTasks), Task.Delay(TimeSpan.FromSeconds(2), stoppingToken));
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "S.A.T. service loop error");
                CloseSockets();
                try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }
        CloseSockets();
    }

    private void EnsureSockets(SatSettings settings)
    {
        if (_satSocket == null)
        {
            try
            {
                _satSocket = BindUdp(settings.UdpPort);
                _logger.LogInformation("S.A.T. UDP listener active on port {Port}", settings.UdpPort);
                _error = null;
            }
            catch (Exception ex)
            {
                _error = $"UDP {settings.UdpPort}: {ex.Message}";
            }
        }
        if (_adifSocket == null)
        {
            try
            {
                _adifSocket = BindUdp(settings.AdifPort);
                _logger.LogInformation("S.A.T. ADIF listener active on port {Port}", settings.AdifPort);
            }
            catch (Exception ex)
            {
                _error = $"ADIF {settings.AdifPort}: {ex.Message}";
            }
        }
    }

    private static UdpClient BindUdp(int port)
    {
        var client = new UdpClient();
        client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        client.Client.Bind(new IPEndPoint(IPAddress.Any, port));
        return client;
    }

    private void CloseSockets()
    {
        _satSocket?.Dispose();
        _satSocket = null;
        _adifSocket?.Dispose();
        _adifSocket = null;
    }

    private async Task ReceiveSatAsync(UdpClient socket, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            var result = await socket.ReceiveAsync(cts.Token);
            var text = Encoding.UTF8.GetString(result.Buffer).Trim();
            await HandleSatMessageAsync(text);
        }
        catch (OperationCanceledException) { }
    }

    private async Task ReceiveAdifAsync(UdpClient socket, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            var result = await socket.ReceiveAsync(cts.Token);
            var text = Encoding.UTF8.GetString(result.Buffer).Trim();
            await HandleAdifRecordAsync(text);
        }
        catch (OperationCanceledException) { }
    }

    internal async Task HandleSatMessageAsync(string text)
    {
        var message = SatMessageParser.Parse(text);
        if (message == null) return;
        var now = DateTime.UtcNow;

        lock (_stateLock)
        {
            _lastHeard = now;
            switch (message)
            {
                case SatBoot boot:
                    _serial = boot.Serial;
                    _firmware = boot.Firmware;
                    _status = "idle";
                    AddEvent(now, "boot", $"S.A.T. v{boot.Firmware} (SN: {boot.Serial})");
                    break;
                case SatStartTrack track:
                    _status = "tracking";
                    _satellite = track.Satellite;
                    _catno = track.CatalogNumber;
                    _passQsos.Clear();
                    _transponder = _upFreq = _upMode = _downFreq = _downMode = null;
                    _aosAz = _losAz = null;
                    _aosTime = null;
                    AddEvent(now, "track", $"Tracking {track.Satellite} ({track.CatalogNumber})");
                    break;
                case SatAos aos:
                    _status = "aos";
                    _aosAz = aos.AzimuthDeg;
                    _aosTime = now;
                    AddEvent(now, "aos", $"AOS at {aos.AzimuthDeg}°");
                    break;
                case SatLos los:
                    _status = "los";
                    _losAz = los.AzimuthDeg;
                    _aosTime = null;
                    AddEvent(now, "los", $"LOS at {los.AzimuthDeg}°");
                    break;
                case SatTransponder xpdr:
                    _transponder = xpdr.Name;
                    _upFreq = xpdr.UplinkFreq;
                    _upMode = xpdr.UplinkMode;
                    _downFreq = xpdr.DownlinkFreq;
                    _downMode = xpdr.DownlinkMode;
                    AddEvent(now, "transponder", $"{xpdr.Name}: ↑{xpdr.UplinkFreq} ↓{xpdr.DownlinkFreq}");
                    break;
                case SatStop:
                    _status = "idle";
                    _aosTime = null;
                    AddEvent(now, "stop", "Tracking stopped");
                    break;
            }
        }

        if (message is SatQso qso)
        {
            lock (_stateLock)
            {
                _passQsos.Add(new SatPassQso(qso.SatName, qso.Callsign, qso.Grid, qso.Mode, now));
                AddEvent(now, "qso", $"QSO: {qso.Callsign} on {qso.SatName}");
            }
            await AutoLogSatQsoAsync(qso, now);
        }

        await BroadcastStateAsync();
    }

    internal async Task HandleAdifRecordAsync(string text)
    {
        if (string.IsNullOrWhiteSpace(text)) return;
        using var scope = _serviceProvider.CreateScope();
        var adifService = scope.ServiceProvider.GetRequiredService<IAdifService>();

        foreach (var qso in adifService.ParseAdif(text))
        {
            if (string.IsNullOrWhiteSpace(qso.Callsign)) continue;
            if (!TryDedupe(qso.Callsign, qso.AdifExtra?.GetValue("SAT_NAME", "")?.AsString ?? "")) continue;

            // Ensure satellite QSOs carry PROP_MODE for LoTW credit
            qso.AdifExtra ??= new BsonDocument();
            if (!qso.AdifExtra.Contains("PROP_MODE")) qso.AdifExtra["PROP_MODE"] = "SAT";

            await PersistAndBroadcastAsync(scope.ServiceProvider, qso);
            lock (_stateLock)
            {
                _lastHeard = DateTime.UtcNow;
                _passQsos.Add(new SatPassQso(
                    qso.AdifExtra.GetValue("SAT_NAME", "")?.AsString ?? "",
                    qso.Callsign, qso.Grid ?? "", qso.Mode, DateTime.UtcNow));
                AddEvent(DateTime.UtcNow, "qso", $"QSO (ADIF): {qso.Callsign}");
            }
        }
        await BroadcastStateAsync();
    }

    private async Task AutoLogSatQsoAsync(SatQso satQso, DateTime now)
    {
        if (!TryDedupe(satQso.Callsign, satQso.SatName)) return;

        double? downMhz = double.TryParse(satQso.DownlinkHz, out var dHz) ? dHz / 1e6 : null;
        double? upMhz = double.TryParse(satQso.UplinkHz, out var uHz) ? uHz / 1e6 : null;
        var band = downMhz != null ? BandHelper.GetBandFromMhz(downMhz.Value) : "";
        var (country, continent, _) = CtyService.GetEntityFromCallsign(satQso.Callsign);

        var qso = new Qso
        {
            Callsign = satQso.Callsign,
            QsoDate = now,
            TimeOn = now.ToString("HHmm"),
            Band = band,
            Mode = satQso.Mode,
            // Qso.Frequency is kHz; band/FREQ_RX stay in MHz.
            Frequency = downMhz * 1000.0,
            RstSent = satQso.RstSent,
            RstRcvd = satQso.RstReceived,
            Comment = satQso.Comment,
            Country = country,
            Continent = continent,
            Station = new StationInfo { Name = satQso.Name, Grid = satQso.Grid, Country = country },
            AdifExtra = new BsonDocument
            {
                ["SAT_NAME"] = satQso.SatName,
                ["PROP_MODE"] = "SAT",
                ["FREQ_RX"] = upMhz?.ToString("F6") ?? "",
            },
        };

        using var scope = _serviceProvider.CreateScope();
        await PersistAndBroadcastAsync(scope.ServiceProvider, qso);
        _logger.LogInformation("S.A.T. auto-logged QSO with {Call} on {Sat}", satQso.Callsign, satQso.SatName);
    }

    /// <summary>
    /// Persist via repository (not IQsoService.CreateAsync) so ADIF extras
    /// like SAT_NAME/PROP_MODE survive — then mirror CreateAsync's side
    /// effects (spot-status cache update + QsoLogged broadcast).
    /// </summary>
    private async Task PersistAndBroadcastAsync(IServiceProvider scopedProvider, Qso qso)
    {
        var repository = scopedProvider.GetRequiredService<IQsoRepository>();
        var spotStatus = scopedProvider.GetService<ISpotStatusService>();

        var created = await repository.CreateAsync(qso);
        spotStatus?.OnQsoLogged(created.Callsign, created.Country, created.Band, created.Mode, created.Station?.Grid);
        await _hubContext.BroadcastQso(new QsoLoggedEvent(
            created.Id, created.Callsign, created.QsoDate, created.TimeOn,
            created.Band, created.Mode, created.Frequency,
            created.RstSent, created.RstRcvd, created.Station?.Grid));
    }

    private bool TryDedupe(string callsign, string satName)
    {
        var key = $"{callsign.ToUpperInvariant()}|{satName}|{DateTime.UtcNow:yyyyMMddHHmm}";
        lock (_stateLock)
        {
            foreach (var stale in _qsoDedupe.Where(kv => DateTime.UtcNow - kv.Value > TimeSpan.FromMinutes(5))
                         .Select(kv => kv.Key).ToList())
                _qsoDedupe.Remove(stale);
            if (_qsoDedupe.ContainsKey(key)) return false;
            _qsoDedupe[key] = DateTime.UtcNow;
            return true;
        }
    }

    /// <summary>True when the last /track poll shows AOS inside the lead window.</summary>
    private bool IsAosWithin(int leadSeconds)
        => _ttAosSec is > 0 && _ttAosSec <= leadSeconds;

    /// <summary>
    /// Lightweight idle probe: is the controller tracking a pass right now (mode==1), or
    /// is AOS inside the lead window? HTTP only — deliberately binds no UDP sockets, so
    /// running this while "inactive" still leaves the listener ports free.
    /// </summary>
    private async Task<bool> IsPassImminentAsync(string controllerIp, int leadSeconds, CancellationToken ct)
    {
        try
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(3);
            var json = await client.GetStringAsync($"http://{controllerIp}/track", ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("mode", out var m) && m.ValueKind == JsonValueKind.Number && m.GetInt32() == 1)
                return true;
            if (root.TryGetProperty("ttaos", out var t) && t.ValueKind == JsonValueKind.Number)
            {
                var ttaos = t.GetDouble();
                return ttaos > 0 && ttaos <= leadSeconds;
            }
            return false;
        }
        catch (Exception ex)
        {
            // Controller off/unreachable while idle is normal — stay inactive quietly.
            _logger.LogDebug(ex, "S.A.T. pass probe failed");
            return false;
        }
    }

    internal async Task PollTrackAsync(string controllerIp, UserSettings settings, CancellationToken ct)
    {
        try
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(2);
            var json = await client.GetStringAsync($"http://{controllerIp}/track", ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            // Controller's own tracking flag — drives auto-deactivate after a pass.
            if (root.TryGetProperty("mode", out var trackMode) && trackMode.ValueKind == JsonValueKind.Number)
                _trackMode = trackMode.GetInt32();

            lock (_stateLock)
            {
                _lastHeard = DateTime.UtcNow;
                var satname = root.TryGetProperty("satname", out var sn) ? sn.GetString()?.Trim() : null;
                if (!string.IsNullOrEmpty(satname))
                {
                    _satellite = satname;
                    if (root.TryGetProperty("catno", out var cn)) _catno = cn.ToString();
                }
                if (root.TryGetProperty("aosAZ", out var aosAz) && aosAz.ValueKind == JsonValueKind.Number)
                    _aosAz = aosAz.GetDouble().ToString("F1");
                if (root.TryGetProperty("losAZ", out var losAz) && losAz.ValueKind == JsonValueKind.Number)
                    _losAz = losAz.GetDouble().ToString("F1");
                if (root.TryGetProperty("fw", out var fw)) _firmware = fw.ToString();

                // Pass status from time-to-AOS / time-to-LOS
                var ttaos = root.TryGetProperty("ttaos", out var ta) && ta.ValueKind == JsonValueKind.Number ? ta.GetDouble() : -1;
                var ttlos = root.TryGetProperty("ttlos", out var tl) && tl.ValueKind == JsonValueKind.Number ? tl.GetDouble() : -1;
                _ttAosSec = ttaos >= 0 ? ttaos : null;
                _ttLosSec = ttlos >= 0 ? ttlos : null;
                // Max-El also comes from /track (the CSN box computes it
                // per-pass); we fall back to remembering the highest _elDeg
                // we've seen if the controller doesn't report it directly.
                if (root.TryGetProperty("maxEL", out var mxEl) && mxEl.ValueKind == JsonValueKind.Number)
                    _maxElDeg = mxEl.GetDouble();
                if (!string.IsNullOrEmpty(satname) && ttaos == 0 && ttlos > 0)
                {
                    _status = "aos";
                    _aosTime ??= DateTime.UtcNow;
                }
                else if (!string.IsNullOrEmpty(satname))
                {
                    _status = "tracking";
                    if (ttaos > 0) _aosTime = null;
                }
                else
                {
                    _status = "idle";
                }

                // Sub-point + footprint from station look-angle and slant range.
                // Also stash the raw look-angle numbers on the state so the
                // Satellite Status panel can show them (v1.x parity).
                // CSN /track exposes the *satellite* look-angle as satAZ/satEL and
                // the slant range as rng. The bare az/el fields are the ANTENNA
                // ROTOR position (0/0 with no rotor slewing), and there is no
                // "range" field — reading those left Az/El stuck at 0 and Range
                // blank. Prefer the satellite fields, falling back to the rotor
                // ones only if a firmware doesn't provide them.
                var az = Num(root, "satAZ") ?? Num(root, "az");
                var el = Num(root, "satEL") ?? Num(root, "el");
                var rangeKm = Num(root, "rng") ?? Num(root, "range");
                _azDeg = az;
                _elDeg = el;
                _rangeKm = rangeKm;
                _altKm = Num(root, "satAlt");
                _footprintKm = Num(root, "satFootprint");
                _rssi = Num(root, "rssi");
                // Antenna ROTOR look-angle (az/el) — distinct from the satellite's
                // satAZ/satEL above; lets the operator see where the dish is pointed.
                _antAz = Num(root, "az");
                _antEl = Num(root, "el");
                // Sub-point: the controller reports satLat/satLon in RADIANS.
                var slat = Num(root, "satLat");
                var slon = Num(root, "satLon");
                _subLat = slat.HasValue ? slat.Value * 180.0 / Math.PI : null;
                _subLon = slon.HasValue ? slon.Value * 180.0 / Math.PI : null;

                // Live Doppler-corrected freqs for the ACTIVE transponder. The box
                // applies Doppler (+ the operator's passband offset) only to the
                // selected transponder; match it by name, falling back to whichever
                // entry is currently carrying Doppler. Nominal freqs stay in
                // _upFreq/_downFreq (what a QSO logs); these drive the live display.
                // The box applies Doppler/offset only to the SELECTED transponder,
                // so that's the reliable "active" signal (name match is a fallback
                // near TCA where Doppler momentarily crosses zero). From the active
                // entry we populate BOTH the nominal transponder info (what a QSO
                // logs — carried on every /track poll, so it survives a restart with
                // no UDP TRANSPONDER frame and follows transponder switches) and the
                // live Doppler-corrected freqs (what the display follows).
                _upFreqLive = _downFreqLive = null;
                _dopUpHz = _dopDownHz = null;
                if (root.TryGetProperty("freq", out var freqArr) && freqArr.ValueKind == JsonValueKind.Array)
                {
                    JsonElement? active = null;
                    foreach (var f in freqArr.EnumerateArray())
                        if ((Num(f, "dop_up") ?? 0) != 0 || (Num(f, "dop_down") ?? 0) != 0 ||
                            (Num(f, "off_up") ?? 0) != 0 || (Num(f, "off_down") ?? 0) != 0) { active = f; break; }
                    if (active is null && !string.IsNullOrEmpty(_transponder))
                        foreach (var f in freqArr.EnumerateArray())
                        {
                            var descr = f.TryGetProperty("descr", out var de) ? de.GetString()?.Trim() : null;
                            if (string.Equals(descr, _transponder!.Trim(), StringComparison.OrdinalIgnoreCase)) { active = f; break; }
                        }
                    if (active is { } a)
                    {
                        var upNom = Num(a, "upFreq") ?? 0;
                        var dnNom = Num(a, "downFreq") ?? 0;
                        var name = a.TryGetProperty("descr", out var dd) ? dd.GetString()?.Trim() : null;
                        if (!string.IsNullOrEmpty(name)) _transponder = name;
                        if (upNom > 0) _upFreq = ((long)Math.Round(upNom)).ToString();
                        if (dnNom > 0) _downFreq = ((long)Math.Round(dnNom)).ToString();
                        var um = a.TryGetProperty("upMode", out var umv) ? umv.GetString()?.Trim() : null;
                        var dm = a.TryGetProperty("downMode", out var dmv) ? dmv.GetString()?.Trim() : null;
                        if (!string.IsNullOrEmpty(um)) _upMode = um;
                        if (!string.IsNullOrEmpty(dm)) _downMode = dm;
                        _dopUpHz = Num(a, "dop_up");
                        _dopDownHz = Num(a, "dop_down");
                        var up = upNom + (_dopUpHz ?? 0) + (Num(a, "off_up") ?? 0);
                        var dn = dnNom + (_dopDownHz ?? 0) + (Num(a, "off_down") ?? 0);
                        if (up > 0) _upFreqLive = ((long)Math.Round(up)).ToString();
                        if (dn > 0) _downFreqLive = ((long)Math.Round(dn)).ToString();
                    }
                }
                // If the controller doesn't push maxEL, track the highest El
                // we've observed this pass ourselves. Reset happens in
                // SetActiveAsync(false) below.
                if (el is > 0 && (_maxElDeg == null || el.Value > _maxElDeg))
                    _maxElDeg = el.Value;
                var station = settings.Station;
                var loc = station.Latitude != null && station.Longitude != null
                    ? (station.Latitude.Value, station.Longitude.Value)
                    : Weather.GeoMath.GridToLatLon(station.GridSquare);
                if (loc != null && az != null && el != null && rangeKm is > 0)
                {
                    var sub = SatGeometry.SubpointFromLook(loc.Value.Item1, loc.Value.Item2, az.Value, el.Value, rangeKm.Value);
                    if (sub != null)
                        _map = new SatMapInfo(sub.Value.Lat, sub.Value.Lon, sub.Value.AltKm,
                            Math.Round(SatGeometry.FootprintRadiusKm(sub.Value.AltKm), 1));
                }
            }
            await BroadcastStateAsync();
        }
        catch (Exception ex)
        {
            _logger.LogDebug("S.A.T. /track poll error: {Error}", ex.Message);
        }
    }

    private static double? Num(JsonElement root, string name) =>
        root.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetDouble() : null;

    private void AddEvent(DateTime time, string kind, string detail)
    {
        _events.Enqueue(new SatEvent(time, kind, detail));
        while (_events.Count > 50) _events.Dequeue();
    }

    private async Task BroadcastStateAsync()
    {
        try
        {
            await _hubContext.Clients.All.OnSatState(GetState());
        }
        catch
        {
            // Broadcasting must never break the listener loop
        }
    }

    private async Task<UserSettings> ReadSettingsAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        return await settingsService.GetSettingsAsync();
    }
}
