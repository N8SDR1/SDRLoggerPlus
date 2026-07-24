using System.Net;
using System.Net.Sockets;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services.Wsjtx;

public record WsjtxClientInfo(string Id, string? Version, DateTime LastHeardUtc);

/// <summary>Fields the frontend echoes back to answer a decoded CQ (Reply, type 4).</summary>
public record WsjtxReplyRequest(
    int Source,
    string ClientId,
    uint Time,
    int Snr,
    double DeltaTime,
    uint DeltaFreq,
    string? Mode,
    string? Message,
    bool LowConfidence);

public record WsjtxStatus(
    int Source,
    bool Listening,
    int Port,
    string? MulticastAddress,
    string? Error,
    List<WsjtxClientInfo> Clients,
    string? LastQsoCall,
    DateTime? LastQsoAtUtc);

/// <summary>
/// Listens on the WSJT-X/JTDX UDP protocol and auto-logs completed QSOs
/// (QSO Logged, message type 5) through IQsoService — so the new QSO raises
/// the same SignalR events and spot-status updates as a manual entry.
///
/// Two independent sources are supported (e.g. WSJT-X on one port and JTDX on
/// another). Each has its own socket, client table and last-QSO state, but they
/// share one dedupe so the same QSO heard on both is only logged once. Each
/// source is settings-gated: its socket is only bound while enabled, and
/// rebinds when its port/multicast changes.
/// </summary>
public class WsjtxService : BackgroundService
{
    private const int DecodeBufferSize = 200;

    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<WsjtxService> _logger;
    private readonly IHubContext<LogHub, ILogHubClient> _hubContext;
    private readonly ISpotStatusService _spotStatus;
    private readonly WsjtxDedupe _dedupe = new();

    private readonly Listener _primary = new(1);
    private readonly Listener _secondary = new(2);

    // Most-recent decodes so a freshly-opened Decodes panel can backfill via
    // GET /api/wsjtx/decodes instead of waiting a cycle. Newest last.
    private readonly LinkedList<WsjtxDecodeEvent> _recentDecodes = new();
    private readonly object _decodesLock = new();

    public WsjtxService(
        IServiceProvider serviceProvider,
        ILogger<WsjtxService> logger,
        IHubContext<LogHub, ILogHubClient> hubContext,
        ISpotStatusService spotStatus)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _hubContext = hubContext;
        _spotStatus = spotStatus;
    }

    /// <summary>Most-recent decodes (oldest first) for panel backfill.</summary>
    public IReadOnlyList<WsjtxDecodeEvent> GetRecentDecodes()
    {
        lock (_decodesLock) return _recentDecodes.ToList();
    }

    /// <summary>
    /// Send a Reply ("call this station") back to the decoder that produced a
    /// decode, telling it to answer that CQ. Works with WSJT-X/JTDX; MSHV honours
    /// it only if it implements inbound Reply. Returns false if that source isn't
    /// bound or we haven't seen where its datagrams come from yet.
    /// </summary>
    public async Task<bool> SendReplyAsync(WsjtxReplyRequest req)
    {
        var l = req.Source == 2 ? _secondary : _primary;
        IPEndPoint? ep;
        lock (l.StateLock) ep = l.LastRemoteEndpoint;
        var udp = l.Udp;
        if (udp == null || ep == null)
        {
            _logger.LogWarning("WSJT-X source {N}: cannot send Reply — {Reason}",
                l.Source, udp == null ? "not listening" : "no decoder endpoint seen yet");
            return false;
        }

        var bytes = WsjtxMessageWriter.BuildReply(
            req.ClientId, req.Time, req.Snr, req.DeltaTime, req.DeltaFreq,
            req.Mode, req.Message, req.LowConfidence);
        await udp.SendAsync(bytes, bytes.Length, ep);
        _logger.LogInformation("WSJT-X source {N}: sent Reply \"{Msg}\" to {Ep}", l.Source, req.Message, ep);
        return true;
    }

    /// <summary>One UDP source: its own socket, client table and last-QSO state.</summary>
    private sealed class Listener(int source)
    {
        public int Source { get; } = source;
        public UdpClient? Udp;
        public bool CurrentEnabled;
        public int CurrentPort;
        public string? CurrentMulticast;
        public string? LastError;
        public readonly Dictionary<string, WsjtxClientInfo> Clients = new();
        public string? LastQsoCall;
        public DateTime? LastQsoAtUtc;
        // Latest dial frequency reported by a Status message; decodes carry only
        // the audio offset, so we add this to reconstruct the real RF frequency.
        public ulong LastDialFreqHz;
        // Latest mode reported by a Status message ("FT8", "FT4", …). Decodes carry
        // only a one-character mode code, so Status is the authoritative source.
        public string? LastMode;
        // Where this decoder's datagrams came from — the target for outbound
        // Reply ("call this station") messages.
        public IPEndPoint? LastRemoteEndpoint;
        // Last DX call seen in a Status message; we populate the log entry only
        // when it changes (Status arrives many times per second).
        public string? LastDxCall;
        public readonly object StateLock = new();

        public WsjtxStatus Snapshot()
        {
            lock (StateLock)
            {
                return new WsjtxStatus(
                    Source,
                    Udp != null,
                    CurrentPort,
                    CurrentMulticast,
                    LastError,
                    Clients.Values.OrderBy(c => c.Id).ToList(),
                    LastQsoCall,
                    LastQsoAtUtc);
            }
        }
    }

    /// <summary>Primary source status — retained for callers that expect a single status.</summary>
    public WsjtxStatus GetStatus() => _primary.Snapshot();

    /// <summary>Status for every source (primary first, then secondary).</summary>
    public IReadOnlyList<WsjtxStatus> GetStatuses() => new[] { _primary.Snapshot(), _secondary.Snapshot() };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("WSJT-X service starting");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await ReadSettingsAsync();

                Reconcile(_primary, settings.Enabled, settings.Port, settings.MulticastAddress);
                var s2 = settings.Source2;
                Reconcile(_secondary, s2?.Enabled ?? false, s2?.Port ?? 0, s2?.MulticastAddress);

                await ReceiveWindowAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "WSJT-X service error, restarting in 5s");
                StopListener(_primary);
                StopListener(_secondary);
                try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }

        StopListener(_primary);
        StopListener(_secondary);
        _logger.LogInformation("WSJT-X service stopped");
    }

    /// <summary>Bring a source's socket in line with its settings; self-heals a failed bind next cycle.</summary>
    private void Reconcile(Listener l, bool enabled, int port, string? multicast)
    {
        if (!enabled)
        {
            // Unconditional: a source can be socketless but still carry state
            // worth clearing — e.g. enabled-but-failing has Udp == null with
            // LastError set, and disabling it must drop that error too.
            StopListener(l);
            l.CurrentEnabled = false;
            return;
        }

        var changed = port != l.CurrentPort || multicast != l.CurrentMulticast;
        if (l.Udp == null || changed)
        {
            StopListener(l);
            l.CurrentPort = port;
            l.CurrentMulticast = multicast;
            l.CurrentEnabled = true;
            StartListener(l, port, multicast);
        }
    }

    private void StartListener(Listener l, int port, string? multicastAddress)
    {
        try
        {
            var udp = new UdpClient();
            udp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            udp.Client.Bind(new IPEndPoint(IPAddress.Any, port));

            if (!string.IsNullOrWhiteSpace(multicastAddress) &&
                IPAddress.TryParse(multicastAddress, out var group))
            {
                udp.JoinMulticastGroup(group);
                _logger.LogInformation("WSJT-X source {N} on port {Port}, multicast group {Group}", l.Source, port, group);
            }
            else
            {
                _logger.LogInformation("WSJT-X source {N} on port {Port} (unicast)", l.Source, port);
            }
            l.Udp = udp;
            l.LastError = null;
        }
        catch (Exception ex)
        {
            // Record the error and leave the socket unbound; the next reconcile
            // cycle retries. A failed source never takes down the other.
            l.LastError = ex.Message;
            l.Udp = null;
            _logger.LogWarning("WSJT-X source {N} bind failed on port {Port}: {Error}", l.Source, port, ex.Message);
        }
    }

    private static void StopListener(Listener l)
    {
        l.Udp?.Dispose();
        l.Udp = null;
        // A stopped socket has no live clients and no current error. Without
        // this, a disabled source's snapshot keeps advertising its old decoders
        // (with an ever-aging LastHeardUtc) and its last bind error forever —
        // and anything judging link health from the status would cry wolf.
        // Clients repopulate from heartbeats (~15s apart) after a rebind.
        lock (l.StateLock)
        {
            l.Clients.Clear();
        }
        l.LastError = null;
    }

    /// <summary>Pump every bound socket for a 5-second window, then re-check settings.</summary>
    private async Task ReceiveWindowAsync(CancellationToken stoppingToken)
    {
        using var loopCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        loopCts.CancelAfter(TimeSpan.FromSeconds(5));

        var tasks = new List<Task>(2);
        if (_primary.Udp != null) tasks.Add(PumpAsync(_primary, loopCts.Token));
        if (_secondary.Udp != null) tasks.Add(PumpAsync(_secondary, loopCts.Token));

        if (tasks.Count == 0)
        {
            // Nothing bound (both disabled or failing) — idle until the next cycle.
            try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
            catch (OperationCanceledException) { }
            return;
        }

        await Task.WhenAll(tasks);
    }

    private async Task PumpAsync(Listener l, CancellationToken ct)
    {
        var udp = l.Udp;
        if (udp == null) return;

        while (!ct.IsCancellationRequested)
        {
            UdpReceiveResult result;
            try
            {
                result = await udp.ReceiveAsync(ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                // Socket faulted — surface it and drop out so the next cycle rebinds.
                l.LastError = ex.Message;
                return;
            }

            if (result.RemoteEndPoint is IPEndPoint ep)
            {
                lock (l.StateLock) l.LastRemoteEndpoint = ep;
            }

            try
            {
                await HandleDatagramAsync(l, result.Buffer);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "WSJT-X datagram handling failed");
            }
        }
    }

    /// <summary>Test / default entry point — attributes datagrams to the primary source.</summary>
    internal Task HandleDatagramAsync(byte[] buffer) => HandleDatagramAsync(_primary, buffer);

    /// <summary>Test entry point — reconcile a source against explicit settings.</summary>
    internal void Reconcile(int source, bool enabled, int port = 0, string? multicast = null)
        => Reconcile(source == 2 ? _secondary : _primary, enabled, port, multicast);

    private async Task HandleDatagramAsync(Listener l, byte[] buffer)
    {
        // A datagram arrived, so the socket is demonstrably receiving — any
        // recorded error is over. Without this, one transient receive fault
        // (e.g. Windows surfacing an ICMP port-unreachable as WSAECONNRESET
        // after an outbound Reply) would flag the source as failed forever,
        // because only a successful re-bind ever cleared LastError.
        l.LastError = null;

        var message = WsjtxMessageReader.Parse(buffer);
        switch (message)
        {
            case WsjtxHeartbeat hb:
                lock (l.StateLock)
                {
                    l.Clients[hb.Id] = new WsjtxClientInfo(hb.Id, hb.Version, DateTime.UtcNow);
                }
                break;

            case WsjtxClose close:
                lock (l.StateLock)
                {
                    l.Clients.Remove(close.Id);
                }
                break;

            case WsjtxStatusMessage status:
                if (status.DialFrequencyHz > 0)
                {
                    lock (l.StateLock) l.LastDialFreqHz = status.DialFrequencyHz;
                }
                if (!string.IsNullOrWhiteSpace(status.Mode))
                {
                    lock (l.StateLock) l.LastMode = status.Mode.Trim();
                }
                await HandleDxCallAsync(l, status);
                break;

            case WsjtxDecode decode:
                await HandleDecodeAsync(l, decode);
                break;

            case WsjtxQsoLogged qso:
                await LogQsoAsync(l, qso);
                break;
        }
    }

    /// <summary>
    /// Turn a raw decode into an enriched, needed-status-stamped event and push
    /// it to the Decodes panel. Only genuinely new, on-air decodes that resolve
    /// to a callsign are surfaced. Enrichment (cty.dat) and needed-status
    /// (SpotStatusService) are the same engines the DX-cluster pipeline uses.
    /// </summary>
    private async Task HandleDecodeAsync(Listener l, WsjtxDecode decode)
    {
        if (!decode.New || decode.OffAir) return;

        var parsed = WsjtxDecodeParser.Parse(decode.Message);
        if (parsed?.Callsign is not { Length: > 0 } call) return;

        ulong dialHz;
        string? statusMode;
        lock (l.StateLock)
        {
            dialHz = l.LastDialFreqHz;
            statusMode = l.LastMode;
        }
        // Decode carries the audio offset only; add the tracked dial frequency.
        var freqHz = dialHz > 0 ? dialHz + decode.DeltaFrequencyHz : 0UL;
        // …and a one-character mode code, not a mode name. Resolve it to something a
        // worked-before lookup can actually match.
        var mode = ResolveDecodeMode(statusMode, decode.Mode);
        var freqKhz = freqHz > 0 ? freqHz / 1000.0 : 0.0;
        var band = freqHz > 0 ? BandHelper.GetBand((long)freqHz) : null;

        // cty.dat entity resolution — synchronous, offline, no rate limit
        // (FT8 produces many decodes per cycle).
        string? country = null, continent = null;
        int? cqZone = null;
        try
        {
            (country, continent, cqZone) = CtyService.GetEntityFromCallsign(call);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "cty.dat lookup failed for decode {Call}", call);
        }

        // Needed-status vs the operator's log (band-aware when we have the freq).
        string? spotStatus = null, zoneStatus = null, gridStatus = null;
        if (freqKhz > 0)
        {
            try { spotStatus = _spotStatus.GetSpotStatus(call, country, freqKhz, mode); }
            catch (Exception ex) { _logger.LogDebug(ex, "GetSpotStatus failed for {Call}", call); }
            try { (_, zoneStatus) = _spotStatus.GetZoneStatus(call, freqKhz); }
            catch (Exception ex) { _logger.LogDebug(ex, "GetZoneStatus failed for {Call}", call); }
        }
        // Grid status works even without a dial frequency — a never-worked grid
        // is "newGrid" regardless of band.
        if (parsed.Grid is { Length: > 0 })
        {
            try { gridStatus = _spotStatus.GetGridStatus(parsed.Grid, freqKhz); }
            catch (Exception ex) { _logger.LogDebug(ex, "GetGridStatus failed for {Call}", call); }
        }

        var evt = new WsjtxDecodeEvent(
            Source: l.Source,
            ClientId: decode.Id,
            Callsign: call,
            DxCall: parsed.DxCall,
            Grid: parsed.Grid,
            Mode: mode,
            Snr: decode.Snr,
            DeltaTimeSeconds: decode.DeltaTimeSeconds,
            AudioOffsetHz: decode.DeltaFrequencyHz,
            FrequencyHz: freqHz,
            Band: band,
            Country: country,
            Continent: continent,
            CqZone: cqZone,
            IsCq: parsed.IsCq,
            SpotStatus: spotStatus,
            ZoneStatus: zoneStatus,
            GridStatus: gridStatus,
            DecodedAtUtc: DateTime.UtcNow,
            TimeMsSinceMidnight: decode.TimeMsSinceMidnight,
            RawMessage: decode.Message,
            LowConfidence: decode.LowConfidence);

        lock (_decodesLock)
        {
            _recentDecodes.AddLast(evt);
            while (_recentDecodes.Count > DecodeBufferSize) _recentDecodes.RemoveFirst();
        }

        await _hubContext.Clients.All.OnWsjtxDecode(evt);
    }

    /// <summary>
    /// Decode messages carry a one-character mode CODE ("~", "+", …), not a mode name,
    /// so passing it straight through produced worked-before keys like "USA:20m:~" that
    /// could never match a logged "USA:20m:FT8" — every already-worked station showed as
    /// un-worked in the Decodes panel.
    ///
    /// The Status message carries the real mode name, so prefer it. Failing that, map the
    /// codes we are sure of, and return null rather than guess — an unknown mode is
    /// already handled as "no verdict" downstream, which is honest; a wrong one is not.
    /// Forks that send a full name in the decode (rather than a code) are passed through.
    /// </summary>
    private static string? ResolveDecodeMode(string? statusMode, string? decodeMode)
    {
        if (!string.IsNullOrWhiteSpace(statusMode)) return statusMode.Trim();

        var code = decodeMode?.Trim();
        if (string.IsNullOrEmpty(code)) return null;
        if (code.Length > 1) return code;   // already a name, not a code

        return code switch
        {
            "~" => "FT8",
            "+" => "FT4",
            "#" => "JT65",
            "@" => "JT9",
            "&" => "MSK144",
            _ => null,
        };
    }

    /// <summary>
    /// When WSJT-X's DX Call changes (you double-clicked a CQ, or a station
    /// answered your CQ), push it through the same spot-selected pipeline a
    /// cluster click uses — populating the Log Entry callsign, firing the QRZ
    /// lookup (→ QRZ Profile), and placing the station on the map. Does NOT
    /// retune the rig (the decoder owns it) and doesn't log the QSO (WSJT-X
    /// auto-logs that separately at 73).
    /// </summary>
    private async Task HandleDxCallAsync(Listener l, WsjtxStatusMessage status)
    {
        var dxCall = status.DxCall?.Trim();
        if (string.IsNullOrEmpty(dxCall)) return;

        ulong dialHz;
        bool changed;
        lock (l.StateLock)
        {
            changed = !string.Equals(dxCall, l.LastDxCall, StringComparison.OrdinalIgnoreCase);
            if (changed) l.LastDxCall = dxCall;
            dialHz = l.LastDialFreqHz;
        }
        if (!changed) return;

        try
        {
            var freqKhz = dialHz > 0 ? dialHz / 1000.0 : 0.0;
            await _hubContext.BroadcastSpotSelected(new SpotSelectedEvent(dxCall, freqKhz, status.Mode, null));
            _logger.LogInformation("WSJT-X source {N} DX call → {Call}: populated log entry", l.Source, dxCall);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "WSJT-X DX-call populate failed for {Call}", dxCall);
        }
    }

    private async Task LogQsoAsync(Listener l, WsjtxQsoLogged qso)
    {
        var request = WsjtxQsoMapper.ToCreateRequest(qso);
        if (request == null) return;

        if (!_dedupe.TryAdd(request.Callsign, qso.DateTimeOff))
        {
            _logger.LogDebug("WSJT-X duplicate QSO suppressed: {Call}", request.Callsign);
            return;
        }

        using var scope = _serviceProvider.CreateScope();

        // WSJT-X has no protocol field for QTH and often sends no name, so an
        // auto-logged QSO would otherwise be thinner than a hand-entered one.
        // Backfill from the callbook, but only the gaps — the grid WSJT-X sent
        // is what the station actually transmitted and always wins.
        request = await FillGapsFromCallbookAsync(scope.ServiceProvider, request);

        var qsoService = scope.ServiceProvider.GetRequiredService<IQsoService>();
        var created = await qsoService.CreateAsync(request);

        lock (l.StateLock)
        {
            l.LastQsoCall = created.Callsign;
            l.LastQsoAtUtc = DateTime.UtcNow;
        }
        _logger.LogInformation("WSJT-X source {N} auto-logged {Mode} QSO with {Call} on {Band}",
            l.Source, request.Mode, created.Callsign, request.Band);
    }

    // Look the call up (QRZ first, HamQTH backfilling whatever QRZ lacked — the
    // same precedence the manual lookup path uses) and fill only the request's
    // empty fields. Best-effort: a missing/unconfigured callbook or a failed
    // lookup must never block the auto-log, so all failures return the request
    // untouched.
    private async Task<CreateQsoRequest> FillGapsFromCallbookAsync(
        IServiceProvider services, CreateQsoRequest request)
    {
        if (!CallbookGapFiller.HasGap(request)) return request;

        try
        {
            var qrz = services.GetService<IQrzService>();
            var info = qrz is null ? null : await qrz.LookupCallsignAsync(request.Callsign);

            HamQthCallsignInfo? hq = null;
            if (info is null || string.IsNullOrWhiteSpace(info.City) || string.IsNullOrWhiteSpace(info.Grid))
            {
                var hamQth = services.GetService<IHamQthService>();
                if (hamQth is not null)
                    hq = await hamQth.LookupCallsignAsync(request.Callsign);
            }

            if (info is null && hq is null) return request;

            var name = info is not null
                ? JoinName(info.FirstName, info.Name)
                : JoinName(hq?.FirstName, hq?.Name);

            return CallbookGapFiller.Fill(
                request,
                name: name,
                city: info?.City ?? hq?.City,
                state: info?.State ?? hq?.State,
                grid: info?.Grid ?? hq?.Grid,
                country: info?.Country ?? hq?.Country);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "WSJT-X callbook gap-fill failed for {Call}", request.Callsign);
            return request;
        }
    }

    private static string? JoinName(string? first, string? last)
    {
        var hasFirst = !string.IsNullOrWhiteSpace(first);
        var hasLast = !string.IsNullOrWhiteSpace(last);
        if (hasFirst && hasLast) return $"{first} {last}";
        return hasFirst ? first : (hasLast ? last : null);
    }

    private async Task<WsjtxSettings> ReadSettingsAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var settings = await settingsService.GetSettingsAsync();
        return settings.Wsjtx;
    }
}
