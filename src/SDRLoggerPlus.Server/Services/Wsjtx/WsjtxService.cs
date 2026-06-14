using System.Net;
using System.Net.Sockets;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services.Wsjtx;

public record WsjtxClientInfo(string Id, string? Version, DateTime LastHeardUtc);

public record WsjtxStatus(
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
/// Settings-gated: the socket is only bound while enabled, and rebinds when
/// port/multicast settings change.
/// </summary>
public class WsjtxService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<WsjtxService> _logger;
    private readonly WsjtxDedupe _dedupe = new();

    private UdpClient? _udpClient;
    private bool _currentEnabled;
    private int _currentPort;
    private string? _currentMulticast;
    private string? _lastError;

    private readonly Dictionary<string, WsjtxClientInfo> _clients = new();
    private string? _lastQsoCall;
    private DateTime? _lastQsoAtUtc;
    private readonly object _stateLock = new();

    public WsjtxService(IServiceProvider serviceProvider, ILogger<WsjtxService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public WsjtxStatus GetStatus()
    {
        lock (_stateLock)
        {
            return new WsjtxStatus(
                _udpClient != null,
                _currentPort,
                _currentMulticast,
                _lastError,
                _clients.Values.OrderBy(c => c.Id).ToList(),
                _lastQsoCall,
                _lastQsoAtUtc);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("WSJT-X service starting");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await ReadSettingsAsync();

                if (!settings.Enabled)
                {
                    StopListener();
                    _currentEnabled = false;
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                    continue;
                }

                if (!_currentEnabled || settings.Port != _currentPort ||
                    settings.MulticastAddress != _currentMulticast)
                {
                    StopListener();
                    StartListener(settings.Port, settings.MulticastAddress);
                    _currentEnabled = true;
                    _currentPort = settings.Port;
                    _currentMulticast = settings.MulticastAddress;
                }

                await ReceiveLoopAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "WSJT-X service error, restarting in 5s");
                StopListener();
                try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }

        StopListener();
        _logger.LogInformation("WSJT-X service stopped");
    }

    private void StartListener(int port, string? multicastAddress)
    {
        try
        {
            _udpClient = new UdpClient();
            _udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _udpClient.Client.Bind(new IPEndPoint(IPAddress.Any, port));

            if (!string.IsNullOrWhiteSpace(multicastAddress) &&
                IPAddress.TryParse(multicastAddress, out var group))
            {
                _udpClient.JoinMulticastGroup(group);
                _logger.LogInformation("WSJT-X listener on port {Port}, multicast group {Group}", port, group);
            }
            else
            {
                _logger.LogInformation("WSJT-X listener on port {Port} (unicast)", port);
            }
            _lastError = null;
        }
        catch (Exception ex)
        {
            _lastError = ex.Message;
            _udpClient?.Dispose();
            _udpClient = null;
            _logger.LogWarning("WSJT-X listener bind failed on port {Port}: {Error}", port, ex.Message);
            throw;
        }
    }

    private void StopListener()
    {
        _udpClient?.Dispose();
        _udpClient = null;
    }

    private async Task ReceiveLoopAsync(CancellationToken stoppingToken)
    {
        if (_udpClient == null) return;

        // Process datagrams for 5 seconds, then re-check settings
        using var loopCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        loopCts.CancelAfter(TimeSpan.FromSeconds(5));

        while (!loopCts.IsCancellationRequested)
        {
            UdpReceiveResult result;
            try
            {
                result = await _udpClient.ReceiveAsync(loopCts.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                await HandleDatagramAsync(result.Buffer);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "WSJT-X datagram handling failed");
            }
        }
    }

    internal async Task HandleDatagramAsync(byte[] buffer)
    {
        var message = WsjtxMessageReader.Parse(buffer);
        switch (message)
        {
            case WsjtxHeartbeat hb:
                lock (_stateLock)
                {
                    _clients[hb.Id] = new WsjtxClientInfo(hb.Id, hb.Version, DateTime.UtcNow);
                }
                break;

            case WsjtxClose close:
                lock (_stateLock)
                {
                    _clients.Remove(close.Id);
                }
                break;

            case WsjtxQsoLogged qso:
                await LogQsoAsync(qso);
                break;
        }
    }

    private async Task LogQsoAsync(WsjtxQsoLogged qso)
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

        lock (_stateLock)
        {
            _lastQsoCall = created.Callsign;
            _lastQsoAtUtc = DateTime.UtcNow;
        }
        _logger.LogInformation("WSJT-X auto-logged {Mode} QSO with {Call} on {Band}",
            request.Mode, created.Callsign, request.Band);
    }

    private async Task<WsjtxSettings> ReadSettingsAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var settings = await settingsService.GetSettingsAsync();
        return settings.Wsjtx;
    }
}
