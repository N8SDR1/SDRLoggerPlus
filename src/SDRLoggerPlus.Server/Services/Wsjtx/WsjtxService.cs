using System.Net;
using System.Net.Sockets;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services.Wsjtx;

public record WsjtxClientInfo(string Id, string? Version, DateTime LastHeardUtc);

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
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<WsjtxService> _logger;
    private readonly WsjtxDedupe _dedupe = new();

    private readonly Listener _primary = new(1);
    private readonly Listener _secondary = new(2);

    public WsjtxService(IServiceProvider serviceProvider, ILogger<WsjtxService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
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
            if (l.Udp != null) StopListener(l);
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

    private async Task HandleDatagramAsync(Listener l, byte[] buffer)
    {
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

            case WsjtxQsoLogged qso:
                await LogQsoAsync(l, qso);
                break;
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

    private async Task<WsjtxSettings> ReadSettingsAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var settings = await settingsService.GetSettingsAsync();
        return settings.Wsjtx;
    }
}
