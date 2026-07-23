using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services.Rig;

namespace SDRLoggerPlus.Server.Services.Flex;

/// <summary>
/// Native FlexRadio 6000-series (SmartSDR) backend — Stage 3 of the rig abstraction.
/// Connects to the radio's TCP command socket (port 4992) ALONGSIDE SmartSDR (the API
/// is multi-client), follows the active slice, and tunes freq/mode. Radios are found
/// via the UDP discovery broadcast. LAN-first; SmartLink / panadapter streaming / DAX
/// are out of scope for v1.
///
/// ⚠ HARDWARE-UNVERIFIED: written from FlexRadio's official smartsdr-api-docs with no
/// Flex on the bench. The wire logic lives in <see cref="FlexProtocol"/> (unit-tested);
/// the parts that need a real radio to confirm are flagged inline: the discovery UDP
/// port (docs say 4991 vs 4992), the slice-status field names, and the exact command
/// acknowledgements. Verify on a 6000-series radio before relying on it.
/// </summary>
public sealed class FlexRadioService : BackgroundService, IRigBackend, ISupportsDiscovery
{
    // Discovery broadcast port. FlexRadio docs are inconsistent (4991 for VITA streaming,
    // 4992 elsewhere); FlexLib clients listen on 4992. VERIFY ON HARDWARE.
    private const int DiscoveryPort = 4992;
    private static readonly TimeSpan DiscoveryStale = TimeSpan.FromSeconds(30);

    private readonly ILogger<FlexRadioService> _logger;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;

    private readonly ConcurrentDictionary<string, (FlexProtocol.FlexRadioInfo Info, DateTime SeenUtc)> _discovered = new();
    private volatile bool _discoveryOn = true;

    // Single active connection (one Flex at a time in v1).
    private TcpClient? _tcp;
    private CancellationTokenSource? _connCts;
    private volatile string? _connectedRadioId;
    private volatile string? _connectedName;
    private string? _handle;
    private int _seq;
    private int _boundSlice = -1;
    private long _lastFreqHz;
    private string _lastMode = "";

    public FlexRadioService(ILogger<FlexRadioService> logger, IHubContext<LogHub, ILogHubClient> hub)
    {
        _logger = logger;
        _hub = hub;
    }

    // ================= IRigBackend =================

    public RadioType Type => RadioType.Flex;
    public string IdScheme => "flex-";
    public bool OwnsRadio(string radioId) => radioId.StartsWith("flex-", StringComparison.Ordinal);

    public IReadOnlyList<string> ConnectedRadioIds
        => _connectedRadioId is { } id ? new[] { id } : Array.Empty<string>();

    public bool IsRadioConnected(string radioId) => _connectedRadioId == radioId;

    public async Task<bool> ConnectAsync(string radioId, CancellationToken ct = default)
    {
        if (_connectedRadioId == radioId && _tcp?.Connected == true) return true;
        if (!_discovered.TryGetValue(radioId, out var entry))
        {
            _logger.LogWarning("Flex connect: radio {RadioId} not in discovery list", radioId);
            return false;
        }
        await DisconnectAsync(_connectedRadioId ?? radioId, ct);

        try
        {
            var tcp = new TcpClient();
            await tcp.ConnectAsync(IPAddress.Parse(entry.Info.Ip), entry.Info.Port, ct);
            _tcp = tcp;
            _connectedRadioId = radioId;
            _connectedName = string.IsNullOrWhiteSpace(entry.Info.Name) ? entry.Info.Model : entry.Info.Name;
            _connCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _ = Task.Run(() => ReadLoopAsync(tcp, _connCts.Token));

            await _hub.BroadcastRadioConnectionStateChanged(
                new RadioConnectionStateChangedEvent(radioId, RadioConnectionState.Connected));
            _logger.LogInformation("Flex connected: {Name} ({RadioId}) at {Ip}:{Port}",
                _connectedName, radioId, entry.Info.Ip, entry.Info.Port);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Flex connect to {RadioId} failed", radioId);
            _connectedRadioId = null;
            _tcp = null;
            return false;
        }
    }

    public async Task DisconnectAsync(string radioId, CancellationToken ct = default)
    {
        var id = _connectedRadioId;
        try { _connCts?.Cancel(); } catch { /* ignore */ }
        try { _tcp?.Close(); } catch { /* ignore */ }
        _tcp = null;
        _connectedRadioId = null;
        _handle = null;
        _boundSlice = -1;
        if (id is not null)
            await _hub.BroadcastRadioConnectionStateChanged(
                new RadioConnectionStateChangedEvent(id, RadioConnectionState.Disconnected));
    }

    // Flex applies freq/mode independently per slice; order isn't sensitive the way a
    // superhet's CW pitch offset is, so tune the frequency then set the mode.
    public async Task<bool> TuneAsync(string radioId, long frequencyHz, string? mode, CancellationToken ct = default)
    {
        var ok = await SetFrequencyAsync(radioId, frequencyHz, ct);
        if (!string.IsNullOrEmpty(mode))
            await SetModeAsync(radioId, mode, frequencyHz, ct);
        return ok;
    }

    public async Task<bool> SetFrequencyAsync(string radioId, long frequencyHz, CancellationToken ct = default)
    {
        if (!IsRadioConnected(radioId)) return false;
        var slice = _boundSlice < 0 ? 0 : _boundSlice;
        var sent = await SendCommandAsync(FlexProtocol.TuneCommand(NextSeq(), slice, frequencyHz), ct);
        if (sent) { _lastFreqHz = frequencyHz; await BroadcastStateAsync(); }
        return sent;
    }

    public async Task<bool> SetModeAsync(string radioId, string mode, long frequencyHz = 0, CancellationToken ct = default)
    {
        if (!IsRadioConnected(radioId) || string.IsNullOrEmpty(mode)) return false;
        var slice = _boundSlice < 0 ? 0 : _boundSlice;
        var sent = await SendCommandAsync(FlexProtocol.ModeCommand(NextSeq(), slice, mode), ct);
        if (sent) { _lastMode = FlexProtocol.MapFlexModeToApp(FlexProtocol.MapAppModeToFlex(mode)); await BroadcastStateAsync(); }
        return sent;
    }

    public IEnumerable<RadioStateChangedEvent> GetRadioStates()
        => _connectedRadioId is { } id
            ? new[] { StateEvent(id) }
            : Array.Empty<RadioStateChangedEvent>();

    public IEnumerable<RadioConnectionStateChangedEvent> GetConnectionStates()
        => _connectedRadioId is { } id
            ? new[] { new RadioConnectionStateChangedEvent(id, RadioConnectionState.Connected) }
            : Array.Empty<RadioConnectionStateChangedEvent>();

    public Task<IEnumerable<RadioDiscoveredEvent>> GetDiscoveredRadiosAsync()
        => Task.FromResult<IEnumerable<RadioDiscoveredEvent>>(
            _discovered.Values.Select(v => ToDiscoveredEvent(v.Info)).ToList());

    // ================= ISupportsDiscovery =================

    public Task StartDiscoveryAsync() { _discoveryOn = true; return Task.CompletedTask; }
    public Task StopDiscoveryAsync() { _discoveryOn = false; return Task.CompletedTask; }

    // ================= discovery listener =================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var udp = new UdpClient();
                udp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                udp.Client.Bind(new IPEndPoint(IPAddress.Any, DiscoveryPort));
                _logger.LogInformation("Flex discovery listening on UDP {Port}", DiscoveryPort);

                while (!stoppingToken.IsCancellationRequested)
                {
                    var result = await udp.ReceiveAsync(stoppingToken);
                    HandleDiscoveryDatagram(result.Buffer);
                    PruneStale();
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Flex discovery socket error; retrying");
                try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); } catch { break; }
            }
        }
    }

    private void HandleDiscoveryDatagram(byte[] buffer)
    {
        var payload = FlexProtocol.FindDiscoveryPayload(buffer, buffer.Length);
        var info = FlexProtocol.ParseDiscoveryPayload(payload);
        if (info is null) return;
        var radioId = FlexProtocol.RadioId(info.Serial);
        var isNew = !_discovered.ContainsKey(radioId);
        _discovered[radioId] = (info, DateTime.UtcNow);
        if (_discoveryOn && isNew)
        {
            _ = _hub.BroadcastRadioDiscovered(ToDiscoveredEvent(info));
            _logger.LogInformation("Flex discovered: {Name} ({RadioId}) at {Ip}:{Port}",
                info.Name, radioId, info.Ip, info.Port);
        }
    }

    private void PruneStale()
    {
        var cutoff = DateTime.UtcNow - DiscoveryStale;
        foreach (var kv in _discovered)
        {
            if (kv.Value.SeenUtc >= cutoff || kv.Key == _connectedRadioId) continue;
            if (_discovered.TryRemove(kv.Key, out _))
                _ = _hub.BroadcastRadioRemoved(new RadioRemovedEvent(kv.Key));
        }
    }

    // ================= TCP read loop + send =================

    private async Task ReadLoopAsync(TcpClient tcp, CancellationToken ct)
    {
        var buffer = new byte[8192];
        var sb = new StringBuilder();
        try
        {
            var stream = tcp.GetStream();
            while (!ct.IsCancellationRequested)
            {
                var n = await stream.ReadAsync(buffer, ct);
                if (n <= 0) break;
                sb.Append(Encoding.ASCII.GetString(buffer, 0, n));
                int nl;
                while ((nl = IndexOfNewline(sb)) >= 0)
                {
                    var line = sb.ToString(0, nl).TrimEnd('\r');
                    sb.Remove(0, nl + 1);
                    if (line.Length > 0) await HandleLineAsync(line, ct);
                }
            }
        }
        catch (OperationCanceledException) { /* disconnect */ }
        catch (Exception ex) { _logger.LogDebug(ex, "Flex read loop ended"); }
        // Connection dropped from the far end.
        if (_connectedRadioId is { } id && _tcp == tcp)
            await DisconnectAsync(id, CancellationToken.None);
    }

    private async Task HandleLineAsync(string line, CancellationToken ct)
    {
        switch (line[0])
        {
            case 'V': // version banner
                _logger.LogDebug("Flex API version {Version}", line[1..]);
                break;
            case 'H': // client handle → subscribe to slice status now that we're identified
                _handle = line[1..].Trim();
                await SendCommandAsync(FlexProtocol.SubscribeSlicesCommand(NextSeq()), ct);
                break;
            case 'S': // status: "S<handle>|<body>"
                var bar = line.IndexOf('|');
                if (bar > 0) await HandleStatusAsync(line[(bar + 1)..]);
                break;
            case 'R': // command response — acks, ignored for now
            case 'M': // message/log from radio
                break;
        }
    }

    private async Task HandleStatusAsync(string body)
    {
        var slice = FlexProtocol.ParseSliceStatus(body);
        if (slice is null) return;
        // Bind to the active slice (fallback: first in-use slice we see).
        if (slice.Active || _boundSlice < 0)
            _boundSlice = slice.Index;
        if (slice.Index != _boundSlice) return;
        if (slice.FrequencyHz is { } hz) _lastFreqHz = hz;
        if (!string.IsNullOrEmpty(slice.Mode)) _lastMode = slice.Mode!;
        await BroadcastStateAsync();
    }

    private async Task<bool> SendCommandAsync(string command, CancellationToken ct)
    {
        var tcp = _tcp;
        if (tcp?.Connected != true) return false;
        try
        {
            var bytes = Encoding.ASCII.GetBytes(command + "\n");
            await tcp.GetStream().WriteAsync(bytes, ct);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Flex send '{Command}' failed", command);
            return false;
        }
    }

    private int NextSeq() => System.Threading.Interlocked.Increment(ref _seq);

    // ================= helpers =================

    private Task BroadcastStateAsync()
        => _connectedRadioId is { } id ? _hub.BroadcastRadioStateChanged(StateEvent(id)) : Task.CompletedTask;

    private RadioStateChangedEvent StateEvent(string radioId) => new(
        RadioId: radioId,
        FrequencyHz: _lastFreqHz,
        Mode: _lastMode,
        IsTransmitting: false,
        Band: _lastFreqHz > 0 ? BandHelper.GetBand(_lastFreqHz) : "",
        SliceOrInstance: _boundSlice >= 0 ? $"RX{_boundSlice}" : null);

    private static RadioDiscoveredEvent ToDiscoveredEvent(FlexProtocol.FlexRadioInfo info) => new(
        Id: FlexProtocol.RadioId(info.Serial),
        Type: RadioType.Flex,
        Model: info.Model,
        IpAddress: info.Ip,
        Port: info.Port,
        Nickname: string.IsNullOrWhiteSpace(info.Name) ? null : info.Name);

    private static int IndexOfNewline(StringBuilder sb)
    {
        for (var i = 0; i < sb.Length; i++)
            if (sb[i] == '\n') return i;
        return -1;
    }
}
