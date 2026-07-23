using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Buffers.Binary;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Dsp;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Service for TCI (Thetis, Hermes, ANAN) radio discovery and CAT control
/// </summary>
public partial class TciRadioService : BackgroundService
{
    private readonly ILogger<TciRadioService> _logger;
    private readonly IHubContext<LogHub, ILogHubClient> _hubContext;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ConcurrentDictionary<string, TciRadioDevice> _discoveredRadios = new();
    private readonly ConcurrentDictionary<string, TciRadioConnection> _connections = new();

    private const int DiscoveryPort = 1024;
    private const int TciDefaultPort = 50001;
    private const int RadioCleanupSeconds = 30;
    private const int DiscoveryBroadcastIntervalMs = 10000;

    private UdpClient? _discoveryClient;
    private CancellationTokenSource? _discoveryCts;
    private bool _isDiscovering;

    public TciRadioService(
        ILogger<TciRadioService> logger,
        IHubContext<LogHub, ILogHubClient> hubContext,
        IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _hubContext = hubContext;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("TCI Radio service starting...");

        // Clear any stale connections and discoveries from previous session
        _connections.Clear();
        _discoveredRadios.Clear();

        // Auto-connect to TCI if configured
        await TryAutoConnectAsync();

        // Run cleanup task periodically
        while (!stoppingToken.IsCancellationRequested)
        {
            await CleanupStaleRadiosAsync();
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
        }
    }

    private async Task TryAutoConnectAsync()
    {
        try
        {
            // Migrate old TCI config from settings if needed
            await MigrateOldTciConfigAsync();

            using var scope = _scopeFactory.CreateScope();
            var settingsRepository = scope.ServiceProvider.GetRequiredService<ISettingsRepository>();
            var settings = await settingsRepository.GetAsync();
            var radioSettings = settings?.Radio;

            if (radioSettings is not { AutoReconnect: true, ActiveRigType: "tci" })
            {
                _logger.LogInformation("TCI auto-reconnect not enabled - skipping");
                return;
            }

            // Load TCI config from radio_configs repo
            var repo = scope.ServiceProvider.GetRequiredService<IRadioConfigRepository>();

            RadioConfigEntity? tciConfig = null;
            if (!string.IsNullOrEmpty(radioSettings.AutoConnectRigId))
            {
                tciConfig = await repo.GetByRadioIdAsync(radioSettings.AutoConnectRigId);
            }

            tciConfig ??= (await repo.GetByTypeAsync("tci")).FirstOrDefault();

            if (tciConfig != null && !string.IsNullOrEmpty(tciConfig.TciHost))
            {
                _logger.LogInformation("Auto-reconnecting to TCI at {Host}:{Port}", tciConfig.TciHost, tciConfig.TciPort);
                await ConnectDirectAsync(tciConfig.TciHost, tciConfig.TciPort ?? 50001, tciConfig.TciName);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to auto-connect to TCI");
        }
    }

    public Task StartDiscoveryAsync()
    {
        if (_isDiscovering)
        {
            _logger.LogDebug("TCI discovery already running");
            return Task.CompletedTask;
        }

        _logger.LogInformation("Starting TCI discovery on UDP port {Port}", DiscoveryPort);

        _discoveryCts = new CancellationTokenSource();
        _isDiscovering = true;

        _ = RunDiscoveryAsync(_discoveryCts.Token);

        return Task.CompletedTask;
    }

    public Task StopDiscoveryAsync()
    {
        if (!_isDiscovering)
        {
            return Task.CompletedTask;
        }

        _logger.LogInformation("Stopping TCI discovery");

        _discoveryCts?.Cancel();
        _discoveryClient?.Close();
        _discoveryClient = null;
        _isDiscovering = false;

        return Task.CompletedTask;
    }

    /// <summary>
    /// Remove a TCI radio from the discovered radios list and disconnect if connected
    /// </summary>
    public async Task RemoveRadioAsync(string radioId)
    {
        // Disconnect if currently connected
        if (_connections.TryRemove(radioId, out var connection))
        {
            await connection.DisconnectAsync();
            _logger.LogInformation("Disconnected TCI radio {RadioId} during removal", radioId);
        }

        // Remove from discovered radios
        if (_discoveredRadios.TryRemove(radioId, out _))
        {
            _logger.LogInformation("Removed TCI radio {RadioId} from discovered radios", radioId);
            await _hubContext.BroadcastRadioRemoved(new RadioRemovedEvent(radioId));
        }
    }

    private async Task RunDiscoveryAsync(CancellationToken ct)
    {
        try
        {
            _discoveryClient = new UdpClient();
            _discoveryClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _discoveryClient.Client.Bind(new IPEndPoint(IPAddress.Any, 0)); // Bind to any available port for sending
            _discoveryClient.EnableBroadcast = true;

            _logger.LogInformation("TCI discovery started, broadcasting on UDP port {Port}", DiscoveryPort);

            // Start listener task
            var listenerTask = ListenForDiscoveryResponsesAsync(ct);

            // Send broadcast discovery requests periodically
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await SendDiscoveryBroadcastAsync();
                    await Task.Delay(DiscoveryBroadcastIntervalMs, ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            await listenerTask;
        }
        catch (OperationCanceledException)
        {
            // Expected on cancellation
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TCI discovery error");
        }
        finally
        {
            _isDiscovering = false;
        }
    }

    private async Task SendDiscoveryBroadcastAsync()
    {
        try
        {
            // Thetis/Hermes discovery message
            var discoveryMessage = Encoding.UTF8.GetBytes("discovery");
            var broadcastEndpoint = new IPEndPoint(IPAddress.Broadcast, DiscoveryPort);

            await _discoveryClient!.SendAsync(discoveryMessage, discoveryMessage.Length, broadcastEndpoint);
            _logger.LogDebug("Sent TCI discovery broadcast");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send TCI discovery broadcast");
        }
    }

    private async Task ListenForDiscoveryResponsesAsync(CancellationToken ct)
    {
        using var listener = new UdpClient();
        listener.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        listener.Client.Bind(new IPEndPoint(IPAddress.Any, DiscoveryPort));

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await listener.ReceiveAsync(ct);
                await ProcessDiscoveryResponseAsync(result.Buffer, result.RemoteEndPoint);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error receiving TCI discovery response");
                await Task.Delay(1000, ct);
            }
        }
    }

    private async Task ProcessDiscoveryResponseAsync(byte[] data, IPEndPoint remoteEndPoint)
    {
        try
        {
            var message = Encoding.UTF8.GetString(data);
            _logger.LogDebug("TCI discovery response from {Ip}: {Message}", remoteEndPoint.Address, message);

            // Parse Thetis/Hermes discovery response
            // Format varies but typically contains: name, model, version, tci_port
            var values = ParseKeyValuePairs(message);

            // Generate ID from IP if no serial available
            var id = values.GetValueOrDefault("serial", remoteEndPoint.Address.ToString().Replace(".", "-"));
            var deviceId = $"tci-{id}";

            var device = new TciRadioDevice
            {
                Id = deviceId,
                Model = values.GetValueOrDefault("model", values.GetValueOrDefault("name", "TCI Radio")),
                IpAddress = remoteEndPoint.Address.ToString(),
                TciPort = int.TryParse(values.GetValueOrDefault("tci_port", TciDefaultPort.ToString()), out var port) ? port : TciDefaultPort,
                Version = values.GetValueOrDefault("version", ""),
                Instances = int.TryParse(values.GetValueOrDefault("receivers", "1"), out var instances) ? instances : 1,
                LastSeen = DateTime.UtcNow
            };

            var isNew = !_discoveredRadios.ContainsKey(device.Id);
            _discoveredRadios[device.Id] = device;

            if (isNew)
            {
                _logger.LogInformation("Discovered TCI radio: {Model} at {Ip}:{Port}",
                    device.Model, device.IpAddress, device.TciPort);

                var evt = new RadioDiscoveredEvent(
                    device.Id,
                    RadioType.Tci,
                    device.Model,
                    device.IpAddress,
                    device.TciPort,
                    null
                );

                await _hubContext.BroadcastRadioDiscovered(evt);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to parse TCI discovery response");
        }
    }

    private async Task CleanupStaleRadiosAsync()
    {
        var cutoff = DateTime.UtcNow.AddSeconds(-RadioCleanupSeconds);
        var staleRadios = _discoveredRadios.Values
            .Where(r => r.LastSeen < cutoff)
            .Where(r => !_connections.ContainsKey(r.Id)) // Don't remove connected radios
            .Where(r => !r.IsDirect) // Don't remove manually-added/saved radios
            .ToList();

        foreach (var radio in staleRadios)
        {
            if (_discoveredRadios.TryRemove(radio.Id, out _))
            {
                _logger.LogInformation("TCI radio {Id} no longer available", radio.Id);
                await _hubContext.BroadcastRadioRemoved(new RadioRemovedEvent(radio.Id));
            }
        }
    }

    public bool HasRadio(string radioId) => _discoveredRadios.ContainsKey(radioId) || _connections.ContainsKey(radioId);

    /// <summary>
    /// Connect directly to a TCI server at a known host:port without discovery
    /// </summary>
    public async Task ConnectDirectAsync(string host, int port = TciDefaultPort, string? name = null)
    {
        var radioId = $"tci-{host}:{port}";

        // If already connected or connecting, disconnect first to allow reconnection
        if (_connections.TryRemove(radioId, out var existingConnection))
        {
            _logger.LogInformation("TCI radio {RadioId} already exists, disconnecting before reconnect", radioId);
            await existingConnection.DisconnectAsync();
        }

        // Create a device entry for direct connection
        var device = new TciRadioDevice
        {
            Id = radioId,
            Model = name ?? $"TCI ({host})",
            IpAddress = host,
            TciPort = port,
            Instances = 1,
            LastSeen = DateTime.UtcNow,
            IsDirect = true
        };

        _discoveredRadios[radioId] = device;

        await _hubContext.BroadcastRadioDiscovered(new RadioDiscoveredEvent(
            radioId,
            RadioType.Tci,
            device.Model,
            host,
            port,
            null
        ));

        await _hubContext.BroadcastRadioConnectionStateChanged(
            new RadioConnectionStateChangedEvent(radioId, RadioConnectionState.Connecting));

        var connection = new TciRadioConnection(device, _logger, _hubContext);
        if (_connections.TryAdd(radioId, connection))
        {
            _ = connection.ConnectAsync();
        }
    }

    public async Task ConnectAsync(string radioId)
    {
        if (!_discoveredRadios.TryGetValue(radioId, out var device))
        {
            _logger.LogWarning("TCI radio {RadioId} not found", radioId);
            return;
        }

        // If already connected or connecting, disconnect first to allow reconnection
        if (_connections.TryRemove(radioId, out var existingConnection))
        {
            _logger.LogInformation("TCI radio {RadioId} already exists, disconnecting before reconnect", radioId);
            await existingConnection.DisconnectAsync();
        }

        await _hubContext.BroadcastRadioConnectionStateChanged(
            new RadioConnectionStateChangedEvent(radioId, RadioConnectionState.Connecting));

        var connection = new TciRadioConnection(device, _logger, _hubContext);
        if (_connections.TryAdd(radioId, connection))
        {
            _ = connection.ConnectAsync();
        }
    }

    public async Task DisconnectAsync(string radioId)
    {
        if (_connections.TryRemove(radioId, out var connection))
        {
            await connection.DisconnectAsync();
            await _hubContext.BroadcastRadioConnectionStateChanged(
                new RadioConnectionStateChangedEvent(radioId, RadioConnectionState.Disconnected));
        }
    }

    public Task SelectInstanceAsync(string radioId, int instance)
    {
        if (_connections.TryGetValue(radioId, out var connection))
        {
            connection.SelectInstance(instance);
        }
        return Task.CompletedTask;
    }

    public async Task<bool> SetFrequencyAsync(string radioId, long frequencyHz)
    {
        if (!_connections.TryGetValue(radioId, out var connection))
        {
            _logger.LogWarning("Cannot set frequency: TCI radio {RadioId} not connected", radioId);
            return false;
        }

        return await connection.SetFrequencyAsync(frequencyHz);
    }

    public async Task<bool> SetModeAsync(string radioId, string mode, long frequencyHz = 0)
    {
        if (!_connections.TryGetValue(radioId, out var connection))
        {
            _logger.LogWarning("Cannot set mode: TCI radio {RadioId} not connected", radioId);
            return false;
        }

        return await connection.SetModeAsync(mode, frequencyHz);
    }

    public async Task<bool> SendCwAsync(string radioId, string message, int speedWpm)
    {
        if (!_connections.TryGetValue(radioId, out var connection))
        {
            _logger.LogWarning("Cannot send CW: TCI radio {RadioId} not connected", radioId);
            return false;
        }

        return await connection.SendCwAsync(message, speedWpm);
    }

    /// <summary>True when at least one TCI radio is connected — lets callers
    /// skip building spot payloads when there's no radio to push them to.</summary>
    public bool AnyConnected => _connections.Values.Any(c => c.IsConnected);

    /// <summary>
    /// Push a DX spot onto every connected TCI radio's panadapter. Used by
    /// DxClusterService to mirror received cluster spots onto Lyra / Thetis
    /// (v1 SDRLogger+ parity). Fan-out is fine — each radio is a distinct
    /// panadapter, so pushing to all connected radios is the intended
    /// behaviour (unlike outbound cluster spots, where we pick one).
    /// </summary>
    public async Task BroadcastSpotAsync(string callsign, string mode, long freqHz, uint argb)
    {
        foreach (var connection in _connections.Values)
        {
            if (!connection.IsConnected) continue;
            try { await connection.SendSpotAsync(callsign, mode, freqHz, argb); }
            catch (Exception ex) { _logger.LogDebug(ex, "TCI spot push failed for {Callsign}", callsign); }
        }
    }

    /// <summary>Clear pushed spots on every connected TCI radio.</summary>
    public async Task ClearAllSpotsAsync()
    {
        foreach (var connection in _connections.Values)
        {
            if (!connection.IsConnected) continue;
            try { await connection.ClearSpotsAsync(); }
            catch (Exception ex) { _logger.LogDebug(ex, "TCI spot_clear failed"); }
        }
    }

    /// <summary>
    /// Stage A′ name-back (docs/COMBO_LINK.md): push a callbook-resolved contact
    /// to every linked Lyra (Combo master) over the existing TCI socket so its
    /// CW Console {NAME}/{GRID} tokens fill. Each connection no-ops unless its
    /// own Combo link is active, so this is safe to fan out. Callers should only
    /// invoke it with genuinely new info (a resolved name/grid) so a bare call
    /// we just received from Lyra doesn't bounce straight back (echo guard).
    /// </summary>
    public async Task PushComboContactAsync(string callsign, string? name, string? grid)
    {
        foreach (var connection in _connections.Values)
        {
            if (!connection.IsConnected) continue;
            try { await connection.SendComboContactAsync(callsign, name, grid); }
            catch (Exception ex) { _logger.LogDebug(ex, "Combo name-back push failed for {Call}", callsign); }
        }
    }

    public async Task<bool> SetCwSpeedAsync(string radioId, int speedWpm)
    {
        if (!_connections.TryGetValue(radioId, out var connection))
        {
            _logger.LogWarning("Cannot set CW speed: TCI radio {RadioId} not connected", radioId);
            return false;
        }

        return await connection.SetCwSpeedAsync(speedWpm);
    }

    public IEnumerable<RadioDiscoveredEvent> GetDiscoveredRadios()
    {
        return _discoveredRadios.Values.Select(d => new RadioDiscoveredEvent(
            d.Id,
            RadioType.Tci,
            d.Model,
            d.IpAddress,
            d.TciPort,
            null
        ));
    }

    /// <summary>
    /// Get discovered radios — merges live discovery with saved TCI configs from the radio_configs repo
    /// </summary>
    public async Task<IEnumerable<RadioDiscoveredEvent>> GetDiscoveredRadiosAsync()
    {
        var radios = GetDiscoveredRadios().ToList();

        // Merge saved TCI configs from the radio_configs repo
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repo = scope.ServiceProvider.GetRequiredService<IRadioConfigRepository>();
            var savedConfigs = await repo.GetByTypeAsync("tci");

            foreach (var entity in savedConfigs)
            {
                if (!radios.Any(r => r.Id == entity.RadioId))
                {
                    radios.Add(new RadioDiscoveredEvent(
                        entity.RadioId,
                        RadioType.Tci,
                        entity.DisplayName,
                        entity.TciHost ?? "",
                        entity.TciPort ?? 50001,
                        null
                    ));
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load saved TCI configs from database");
        }

        return radios;
    }

    public IEnumerable<RadioStateChangedEvent> GetRadioStates()
    {
        return _connections.Values
            .Where(c => c.IsConnected)
            .Select(c => c.GetCurrentState())
            .Where(s => s != null)!;
    }

    public IEnumerable<RadioConnectionStateChangedEvent> GetConnectionStates()
    {
        return _connections.Select(kvp => new RadioConnectionStateChangedEvent(
            kvp.Key,
            kvp.Value.IsConnected ? RadioConnectionState.Connected : RadioConnectionState.Disconnected
        ));
    }

    /// <summary>
    /// Save a TCI radio config to the radio_configs collection
    /// </summary>
    public async Task SaveTciConfigAsync(string host, int port, string? name)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repo = scope.ServiceProvider.GetRequiredService<IRadioConfigRepository>();
            var radioId = $"tci-{host}:{port}";
            var entity = new RadioConfigEntity
            {
                RadioId = radioId,
                RadioType = "tci",
                DisplayName = !string.IsNullOrEmpty(name) ? name : $"TCI ({host})",
                TciHost = host,
                TciPort = port,
                TciName = name,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow,
            };
            await repo.UpsertByRadioIdAsync(entity);
            // Keep the in-memory device's name in sync so a rename shows up
            // immediately. GetDiscoveredRadiosAsync prefers the in-memory entry
            // over the saved config, so without this a stale name would shadow
            // the updated DisplayName for an already-known/connected rig.
            if (_discoveredRadios.TryGetValue(radioId, out var device))
            {
                device.Model = entity.DisplayName;
            }
            _logger.LogInformation("Saved TCI config: {Host}:{Port}", host, port);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to save TCI config to database");
        }
    }

    /// <summary>
    /// Delete a TCI radio config from the radio_configs collection
    /// </summary>
    public async Task DeleteTciConfigAsync(string radioId)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repo = scope.ServiceProvider.GetRequiredService<IRadioConfigRepository>();
            await repo.DeleteByRadioIdAsync(radioId);
            _logger.LogInformation("Deleted TCI config: {RadioId}", radioId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete TCI config from database");
        }
    }

    /// <summary>
    /// One-time migration: if settings.Radio.Tci has a host, migrate to radio_configs
    /// </summary>
    private async Task MigrateOldTciConfigAsync()
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var repo = scope.ServiceProvider.GetRequiredService<IRadioConfigRepository>();

            // Skip if radio_configs already has TCI entries
            var existing = await repo.GetByTypeAsync("tci");
            if (existing.Count > 0) return;

            var settingsRepository = scope.ServiceProvider.GetRequiredService<ISettingsRepository>();
            var settings = await settingsRepository.GetAsync();
            var tciSettings = settings?.Radio?.Tci;

            if (tciSettings == null || string.IsNullOrEmpty(tciSettings.Host)) return;

            _logger.LogInformation("Migrating old TCI config from settings: {Host}:{Port}", tciSettings.Host, tciSettings.Port);

            var radioId = $"tci-{tciSettings.Host}:{tciSettings.Port}";
            var entity = new RadioConfigEntity
            {
                RadioId = radioId,
                RadioType = "tci",
                DisplayName = !string.IsNullOrEmpty(tciSettings.Name) ? tciSettings.Name : $"TCI ({tciSettings.Host})",
                TciHost = tciSettings.Host,
                TciPort = tciSettings.Port,
                TciName = tciSettings.Name,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow,
            };
            await repo.UpsertByRadioIdAsync(entity);
            _logger.LogInformation("Migrated TCI config to radio_configs: {RadioId}", radioId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to migrate old TCI config");
        }
    }

    private static Dictionary<string, string> ParseKeyValuePairs(string text)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        // Try parsing as key=value pairs
        var matches = Regex.Matches(text, @"(\w+)=([^\s;]+)");
        foreach (Match match in matches)
        {
            values[match.Groups[1].Value] = match.Groups[2].Value;
        }

        // Also try parsing as key:value pairs (some TCI implementations use this)
        var colonMatches = Regex.Matches(text, @"(\w+):([^\s;,]+)");
        foreach (Match match in colonMatches)
        {
            if (!values.ContainsKey(match.Groups[1].Value))
            {
                values[match.Groups[1].Value] = match.Groups[2].Value;
            }
        }

        return values;
    }
}

internal class TciRadioDevice
{
    public required string Id { get; set; }
    public required string Model { get; set; }
    public required string IpAddress { get; set; }
    public int TciPort { get; set; }
    public string? Version { get; set; }
    public int Instances { get; set; } = 1;
    public DateTime LastSeen { get; set; }
    /// <summary>
    /// True for radios added via direct connection (not UDP discovery).
    /// These are not subject to stale cleanup.
    /// </summary>
    public bool IsDirect { get; set; }
}

internal class TciRadioConnection
{
    private readonly TciRadioDevice _device;
    private readonly ILogger _logger;
    private readonly IHubContext<LogHub, ILogHubClient> _hubContext;

    private ClientWebSocket? _webSocket;
    private CancellationTokenSource? _cts;

    private int _selectedInstance;
    private long _currentFrequencyHz;
    private string _currentMode = "USB";
    private bool _isTransmitting;
    // Filter passband edges relative to the carrier, in Hz, from TCI's
    // rx_filter_band message. USB reports positive numbers, LSB reports
    // negative — we surface them exactly as the radio reports so the
    // panadapter can draw the passband on the correct side of the VFO
    // without per-mode guessing.
    private int _filterLowHz;
    private int _filterHighHz;
    // CTUN offset: TCI's `dds` message reports the panadapter center; VFO
    // may sit inside that window when the operator is in CTUN mode. When
    // dds == vfo (normal / no-CTUN), the two match.
    private long _ddsCenterHz;
    // CW pitch in Hz — used to position CW's narrow passband correctly
    // relative to the carrier marker (CWU passband sits at +pitch, CWL at
    // -pitch). Default 700 matches Lyra + most rigs.
    private int _cwPitchHz = 700;
    // Lyra ↔ SDRLogger+ "Combo" link (docs/COMBO_LINK.md). Lyra is the master
    // and announces on/off via `lyra_combo`; we mirror it as the "Lyra Combo:
    // Linked" indicator and only act on `lyra_contact` while linked.
    // _lastComboCall dedups repeated contact pushes so a resend doesn't
    // re-trigger the callbook lookup (and, with the name-back, can't loop).
    private bool _comboLinked;
    private string? _lastComboCall;
    private readonly Tci.TciMeterAggregator _meters = new();

    // IQ panadapter: accumulate the TCI IQ stream, FFT it in the backend, and
    // broadcast a power spectrum over SignalR for the panadapter.
    private const int IqFftSize = 4096;
    private const int IqOutputBins = 2048; // finer bins so the meter's narrow (kHz) window stays smooth
    private const long IqMinBroadcastMs = 50; // ~20 fps
    private const int IqSampleRate = 192000;
    private static readonly double[] IqWindow = Fft.BlackmanHarris(IqFftSize);
    private readonly float[] _iqBuf = new float[IqFftSize * 2]; // interleaved I/Q
    private int _iqFill;
    private bool _iqEnabled;
    private long _lastSpectrumTicks;

    public bool IsConnected => _webSocket?.State == WebSocketState.Open;

    public TciRadioConnection(TciRadioDevice device, ILogger logger, IHubContext<LogHub, ILogHubClient> hubContext)
    {
        _device = device;
        _logger = logger;
        _hubContext = hubContext;
    }

    // Reconnect-on-drop backoff bounds. The link is retried with capped
    // exponential backoff after any non-operator-initiated drop.
    private const int InitialReconnectDelayMs = 1000;
    private const int MaxReconnectDelayMs = 15000;

    public async Task ConnectAsync()
    {
        _cts = new CancellationTokenSource();
        var ct = _cts.Token;

        // Reconnect-on-drop: keep (re)establishing the TCI link with capped
        // exponential backoff until the operator explicitly disconnects (which
        // cancels _cts). Without this, a Lyra restart — e.g. a dev rebuild —
        // silently kills the connection AND the Combo link with it until a
        // manual reconnect. Backoff resets to 1 s on every successful connect.
        var backoffMs = InitialReconnectDelayMs;
        var firstAttempt = true;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (firstAttempt)
                {
                    _logger.LogInformation("Connecting to TCI radio at {Ip}:{Port}",
                        _device.IpAddress, _device.TciPort);
                }
                else
                {
                    _logger.LogInformation("Reconnecting to TCI radio {Id} at {Ip}:{Port}",
                        _device.Id, _device.IpAddress, _device.TciPort);
                    await _hubContext.BroadcastRadioConnectionStateChanged(
                        new RadioConnectionStateChangedEvent(_device.Id, RadioConnectionState.Connecting));
                }
                firstAttempt = false;

                _webSocket = new ClientWebSocket();
                var uri = new Uri($"ws://{_device.IpAddress}:{_device.TciPort}");

                await _webSocket.ConnectAsync(uri, ct);

                _logger.LogInformation("Connected to TCI radio {Id}", _device.Id);
                backoffMs = InitialReconnectDelayMs;   // healthy connect → reset backoff

                await _hubContext.BroadcastRadioConnectionStateChanged(
                    new RadioConnectionStateChangedEvent(_device.Id, RadioConnectionState.Connected));

                // TCI protocol: server pushes updates to us automatically — just
                // listen. Runs until the socket closes / errors / is cancelled.
                await ReceiveLoopAsync(ct);
            }
            catch (OperationCanceledException)
            {
                break;   // operator disconnect / radio removal — stop retrying
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "TCI connection lost for {Id} — will retry", _device.Id);
            }
            finally
            {
                try { _webSocket?.Dispose(); } catch { /* already gone */ }
                _webSocket = null;
            }

            if (ct.IsCancellationRequested) break;

            // Dropped but the operator didn't ask to disconnect. Clear the Combo
            // "linked" state so the badge doesn't falsely claim a live link while
            // Lyra is away; the reconnect's sendInit re-announces lyra_combo.
            if (_comboLinked)
            {
                _comboLinked = false;
                _lastComboCall = null;
                try
                {
                    await _hubContext.BroadcastComboLinkChanged(
                        new ComboLinkChangedEvent(false, _device.Id));
                }
                catch (Exception ex) { _logger.LogDebug(ex, "Combo unlink broadcast failed"); }
            }

            try { await Task.Delay(backoffMs, ct); }
            catch (OperationCanceledException) { break; }
            backoffMs = Math.Min(backoffMs * 2, MaxReconnectDelayMs);
        }

        await _hubContext.BroadcastRadioConnectionStateChanged(
            new RadioConnectionStateChangedEvent(_device.Id, RadioConnectionState.Disconnected));
    }

    public async Task DisconnectAsync()
    {
        _cts?.Cancel();
        if (_webSocket?.State == WebSocketState.Open)
        {
            await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disconnecting", CancellationToken.None);
        }
        _webSocket?.Dispose();
        _webSocket = null;
    }

    public void SelectInstance(int instance)
    {
        _selectedInstance = instance;
        _logger.LogInformation("TCI radio {Id} monitoring instance {Instance}", _device.Id, instance);

        _ = _hubContext.BroadcastRadioConnectionStateChanged(
            new RadioConnectionStateChangedEvent(_device.Id, RadioConnectionState.Monitoring));
    }

    public RadioStateChangedEvent? GetCurrentState()
    {
        if (!IsConnected) return null;

        return new RadioStateChangedEvent(
            _device.Id,
            _currentFrequencyHz,
            _currentMode,
            _isTransmitting,
            BandHelper.GetBand(_currentFrequencyHz),
            _selectedInstance.ToString(),
            _filterLowHz,
            _filterHighHz,
            _ddsCenterHz,
            _cwPitchHz
        );
    }

    private async Task SendCommandAsync(string command)
    {
        if (_webSocket?.State != WebSocketState.Open) return;

        var buffer = Encoding.UTF8.GetBytes(command);
        await _webSocket.SendAsync(buffer, WebSocketMessageType.Text, true, _cts!.Token);
    }

    public async Task<bool> SetFrequencyAsync(long frequencyHz)
    {
        if (!IsConnected) return false;

        // TCI protocol: vfo:rx,channel,frequency; (rx=0/1, channel=0 for VFO-A)
        var command = $"vfo:{_selectedInstance},0,{frequencyHz};";
        _logger.LogDebug("Sending TCI command: {Command}", command);
        await SendCommandAsync(command);
        return true;
    }

    public async Task<bool> SetModeAsync(string mode, long frequencyHz = 0)
    {
        if (!IsConnected) return false;

        var tciMode = MapToTciMode(mode, frequencyHz);
        // TCI protocol: modulation:rx,MODE;
        var command = $"modulation:{_selectedInstance},{tciMode};";
        _logger.LogDebug("Sending TCI command: {Command} (app mode: {AppMode})", command, mode);
        await SendCommandAsync(command);
        return true;
    }

    /// <summary>
    /// Maps application-level mode names to TCI protocol mode strings.
    /// TCI modes vary by radio. Common: LSB, USB, DSB, CW, FMN, AM, DIGU, SPEC, DIGL, SAM, DRM
    /// </summary>
    private static string MapToTciMode(string appMode, long frequencyHz)
    {
        switch (appMode.ToUpperInvariant())
        {
            case "CW":
            case "CWU":
            case "CWL":
                return "CW";
            case "SSB":
                // Below 10 MHz convention is LSB, above is USB
                return frequencyHz > 0 && frequencyHz < 10_000_000 ? "LSB" : "USB";
            case "USB":
                return "USB";
            case "LSB":
                return "LSB";
            case "AM":
                return "AM";
            case "FM":
                return "FMN";
            case "FT8":
            case "FT4":
            case "RTTY":
            case "PSK31":
            case "DIGI":
            case "JT65":
            case "JT9":
                return "DIGU";
            case "DSB":
                return "DSB";
            case "SAM":
                return "SAM";
            case "DRM":
                return "DRM";
            default:
                return appMode.ToUpperInvariant();
        }
    }

    public async Task<bool> SendCwAsync(string message, int speedWpm)
    {
        if (!IsConnected) return false;

        // Set speed first
        await SetCwSpeedAsync(speedWpm);

        // TCI protocol: cw_text:channel,text;
        // Note: Some implementations may use different channel numbers
        var command = $"cw_text:{_selectedInstance},{message};";
        _logger.LogInformation("Sending CW via TCI: {Message} at {Wpm} WPM", message, speedWpm);
        await SendCommandAsync(command);
        return true;
    }

    /// <summary>
    /// Push a DX spot onto the connected TCI radio's panadapter/waterfall.
    /// TCI protocol: spot:callsign,mode,freqHz,argb; — the argb is an
    /// unsigned 32-bit colour (alpha in the top byte). Lyra / Thetis
    /// render it as a click-to-tune coloured marker on the spectrum.
    /// Mirrors v1 SDRLogger+'s tci_spot behaviour.
    /// </summary>
    public async Task<bool> SendSpotAsync(string callsign, string mode, long freqHz, uint argb)
    {
        if (!IsConnected) return false;
        // TCI spot mode is lowercase (usb/lsb/cw/…) per the spec + Thetis.
        var spotMode = string.IsNullOrWhiteSpace(mode) ? "ssb" : mode.Trim().ToLowerInvariant();
        var command = $"spot:{callsign.ToUpperInvariant()},{spotMode},{freqHz},{argb};";
        await SendCommandAsync(command);
        return true;
    }

    /// <summary>Clear all pushed spots from the TCI radio (spot_clear;).</summary>
    public async Task<bool> ClearSpotsAsync()
    {
        if (!IsConnected) return false;
        await SendCommandAsync("spot_clear;");
        return true;
    }

    /// <summary>
    /// Stage A′ "name-back" (docs/COMBO_LINK.md): send a callbook-resolved
    /// contact back to a linked Lyra so its CW Console {NAME}/{GRID} tokens fill.
    /// Wire format (Lyra parses on ',' with empties preserved):
    ///   lyra_contact:sdrlog,call,rstSent,rstRcvd,name,qth,grid,serial;
    /// The src=sdrlog tag marks our provenance so Lyra applies it under its echo
    /// guard and never bounces it back. No-op unless this connection's Combo
    /// link is active (Lyra is master) — so we never drive an unlinked radio.
    /// Commas/semicolons in free-text fields would corrupt the TCI framing, so
    /// they are stripped first.
    /// </summary>
    public async Task<bool> SendComboContactAsync(string callsign, string? name, string? grid)
    {
        if (!IsConnected || !_comboLinked) return false;
        if (string.IsNullOrWhiteSpace(callsign)) return false;

        // src,call,rstSent,rstRcvd,name,qth,grid,serial — only name + grid filled.
        var command = $"lyra_contact:sdrlog,{callsign.ToUpperInvariant()},,,{TciField(name)},,{TciField(grid)},;";
        await SendCommandAsync(command);
        return true;
    }

    /// <summary>Strip TCI framing chars (',' arg and ';' command separators) from
    /// a free-text field so a name/QTH like "Smith, John" can't corrupt the
    /// frame or shift Lyra's positional arg parsing.</summary>
    private static string TciField(string? s) =>
        string.IsNullOrWhiteSpace(s) ? "" : s.Replace(',', ' ').Replace(';', ' ').Trim();

    public async Task<bool> SetCwSpeedAsync(int speedWpm)
    {
        if (!IsConnected) return false;

        // TCI protocol: cw_speed:channel,speed;
        var command = $"cw_speed:{_selectedInstance},{speedWpm};";
        _logger.LogDebug("Setting CW speed via TCI: {Wpm} WPM", speedWpm);
        await SendCommandAsync(command);
        return true;
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        // Text commands are small, but binary IQ frames are tens of KB and arrive
        // across several WebSocket fragments, so assemble each message fully
        // (until EndOfMessage) before dispatching it.
        var chunk = new byte[16384];
        using var assembly = new MemoryStream();

        while (!ct.IsCancellationRequested && _webSocket?.State == WebSocketState.Open)
        {
            try
            {
                assembly.SetLength(0);
                WebSocketReceiveResult result;
                var serverClosed = false;
                do
                {
                    result = await _webSocket.ReceiveAsync(chunk, ct);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        serverClosed = true;
                        break;
                    }
                    assembly.Write(chunk, 0, result.Count);
                } while (!result.EndOfMessage);

                if (serverClosed)
                {
                    _logger.LogWarning("TCI connection closed by server");
                    break;
                }

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var message = Encoding.UTF8.GetString(assembly.GetBuffer(), 0, (int)assembly.Length);
                    await ProcessMessageAsync(message);
                }
                else if (result.MessageType == WebSocketMessageType.Binary)
                {
                    await ProcessBinaryFrameAsync(assembly.GetBuffer(), (int)assembly.Length);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                // A drop is expected + recoverable now — ConnectAsync's loop
                // reconnects. Return so it can retry (don't broadcast state here;
                // the reconnect loop owns Connecting/Connected/Disconnected).
                _logger.LogWarning(ex, "TCI receive error for {Id} — reconnecting", _device.Id);
                break;
            }
        }
    }

    /// <summary>
    /// Enable the Thetis IQ panadapter stream after the TCI handshake. A radio
    /// that lacks IQ simply ignores the commands.
    /// </summary>
    private async Task EnableIqStreamAsync()
    {
        try
        {
            _iqEnabled = true;
            _iqFill = 0;
            await SendCommandAsync($"iq_samplerate:{IqSampleRate};");
            await SendCommandAsync($"iq_start:{_selectedInstance};");
            _logger.LogInformation("TCI IQ panadapter stream enabled ({Rate} Hz) for {Id}", IqSampleRate, _device.Id);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to enable TCI IQ stream for {Id}", _device.Id);
        }
    }

    /// <summary>
    /// Accumulate an incoming binary IQ frame; once a full FFT block is buffered,
    /// transform it and broadcast a power spectrum (throttled). Non-IQ frames
    /// (RX/TX audio) and frames for other receivers are ignored.
    /// </summary>
    private async Task ProcessBinaryFrameAsync(byte[] buffer, int length)
    {
        if (!_iqEnabled) return;

        var parsed = TciStreamFrame.Parse(buffer.AsSpan(0, length));
        if (parsed is not { } frame) return;
        if (frame.Type != TciStreamFrame.TypeIq) return;
        if (frame.Receiver != (uint)_selectedInstance) return;

        int floatCount = frame.AvailableFloats(length);
        if (floatCount <= 0) return;

        // Read the span inline per sample — a Span<byte> can't be held across the
        // await below, but the backing byte[] can.
        for (int i = 0; i < floatCount; i++)
        {
            if (_iqFill >= _iqBuf.Length)
            {
                await TryBroadcastSpectrumAsync();
                _iqFill = 0;
            }
            _iqBuf[_iqFill++] = BinaryPrimitives.ReadSingleLittleEndian(
                buffer.AsSpan(TciStreamFrame.HeaderBytes + i * 4));
        }

        if (_iqFill >= _iqBuf.Length)
        {
            await TryBroadcastSpectrumAsync();
            _iqFill = 0;
        }
    }

    private async Task TryBroadcastSpectrumAsync()
    {
        var now = Environment.TickCount64;
        if (now - _lastSpectrumTicks < IqMinBroadcastMs) return; // throttle to ~20 fps
        _lastSpectrumTicks = now;

        var center = _currentFrequencyHz;
        if (center <= 0) return; // no VFO yet — nothing meaningful to label the span with

        long halfSpan = IqSampleRate / 2;
        var data = IqSpectrum.PowerSpectrumDb(_iqBuf, IqFftSize, IqWindow, IqOutputBins);
        await _hubContext.BroadcastSpectrumData(
            new SpectrumDataEvent(center - halfSpan, center + halfSpan, data));
    }

    private async Task ProcessMessageAsync(string message)
    {
        _logger.LogDebug("TCI message: {Message}", message);

        // TCI protocol format: command:arg1,arg2,...;command2:arg1,...;
        // Multiple commands can be in one message, separated by semicolons
        var commands = message.Trim().Split(';', StringSplitOptions.RemoveEmptyEntries);

        var stateChanged = false;

        foreach (var cmd in commands)
        {
            var colonIndex = cmd.IndexOf(':');

            string command;
            string argsStr;
            string[] args;

            if (colonIndex < 0)
            {
                command = cmd.ToLower().Trim();
                argsStr = "";
                args = [];
            }
            else
            {
                command = cmd[..colonIndex].ToLower().Trim();
                argsStr = cmd[(colonIndex + 1)..];
                args = argsStr.Split(',').Select(a => a.Trim()).ToArray();
            }

            switch (command)
            {
                case "vfo":
                    // Format: vfo:rx,channel,frequency; (rx=0/1, channel=0 for VFO-A, frequency in Hz)
                    if (args.Length >= 3)
                    {
                        var rx = int.TryParse(args[0], out var rxVal) ? rxVal : 0;
                        var channel = int.TryParse(args[1], out var chVal) ? chVal : 0;

                        // Only track RX0, VFO-A (channel 0) or match selected instance
                        if (rx == _selectedInstance && channel == 0)
                        {
                            if (long.TryParse(args[2], out var freq) && freq != _currentFrequencyHz)
                            {
                                _currentFrequencyHz = freq;
                                stateChanged = true;
                            }
                        }
                    }
                    break;

                case "modulations_list":
                    // Format: modulations_list:mode1,mode2,...;
                    _logger.LogDebug("TCI supported modulations: {Modes}", argsStr);
                    break;

                case "modulation":
                    // Format: modulation:rx,MODE;
                    // TCI's mode enum has no CW sideband distinction — it's
                    // just "CW". Lyra (and the TCI spec) collapse CWU/CWL →
                    // CW outbound and expect the receiver to re-derive the
                    // sideband from the current dial: CWU above 10 MHz, CWL
                    // below. Without this the log-entry Mode dropdown gets
                    // a stale literal "CW" that doesn't match either of
                    // its CWU/CWL options.
                    if (args.Length >= 2)
                    {
                        var rx = int.TryParse(args[0], out var rxVal) ? rxVal : 0;
                        if (rx == _selectedInstance)
                        {
                            var mode = args[1].ToUpper();
                            if (mode == "CW")
                            {
                                mode = _currentFrequencyHz > 0 && _currentFrequencyHz < 10_000_000
                                    ? "CWL" : "CWU";
                            }
                            if (mode != _currentMode)
                            {
                                _currentMode = mode;
                                stateChanged = true;
                            }
                        }
                    }
                    break;

                case "rx_filter_band":
                    // Format: rx_filter_band:rx,lowHz,highHz;
                    // Signed edges relative to the carrier. USB reports
                    // positive numbers (e.g. 100..2700), LSB negative
                    // (-2700..-100), CW narrow around ±pitch. We forward
                    // as-is; the panadapter draws the passband from
                    // vfo+low to vfo+high without per-mode assumptions.
                    if (args.Length >= 3)
                    {
                        var rx = int.TryParse(args[0], out var rxVal) ? rxVal : 0;
                        if (rx == _selectedInstance
                            && int.TryParse(args[1], out var lo)
                            && int.TryParse(args[2], out var hi))
                        {
                            if (lo != _filterLowHz || hi != _filterHighHz)
                            {
                                _filterLowHz = lo;
                                _filterHighHz = hi;
                                stateChanged = true;
                            }
                        }
                    }
                    break;

                case "dds":
                    // Format: dds:rx,frequencyHz;
                    // The panadapter's center frequency. Usually tracks
                    // the VFO 1:1, but Lyra's CTUN lock decouples them:
                    // dds stays put on the band segment while the VFO
                    // moves inside it. We surface it so the panadapter
                    // can render the correct span.
                    if (args.Length >= 2)
                    {
                        var rx = int.TryParse(args[0], out var rxVal) ? rxVal : 0;
                        if (rx == _selectedInstance
                            && long.TryParse(args[1], out var ddsHz)
                            && ddsHz != _ddsCenterHz)
                        {
                            _ddsCenterHz = ddsHz;
                            stateChanged = true;
                        }
                    }
                    break;

                case "cw_pitch":
                    // Format: cw_pitch:rx,pitchHz;
                    // Offset of the CW audio tone from the carrier. The
                    // panadapter uses it to position the narrow CW filter
                    // rectangle correctly (CWU sits at +pitch, CWL at
                    // -pitch, both centered on the pitch tone).
                    if (args.Length >= 2)
                    {
                        // Some rigs send just `cw_pitch:hz;` without an rx
                        // index; accept both shapes.
                        var raw = args.Length >= 2 && int.TryParse(args[0], out _) && int.TryParse(args[1], out var p)
                            ? p
                            : (int.TryParse(args[0], out var p1) ? p1 : _cwPitchHz);
                        if (raw > 0 && raw != _cwPitchHz)
                        {
                            _cwPitchHz = raw;
                            stateChanged = true;
                        }
                    }
                    break;

                case "trx":
                    // Format: trx:rx,state; (state: true/false or 1/0)
                    if (args.Length >= 2)
                    {
                        var rx = int.TryParse(args[0], out var rxVal) ? rxVal : 0;
                        if (rx == _selectedInstance)
                        {
                            var newTx = args[1].Equals("true", StringComparison.OrdinalIgnoreCase)
                                     || args[1] == "1";
                            if (newTx != _isTransmitting)
                            {
                                _isTransmitting = newTx;
                                stateChanged = true;
                            }
                        }
                    }
                    break;

                case "tx":
                    // Alternative TX format: tx:state;
                    if (args.Length >= 1)
                    {
                        var newTx = args[0].Equals("true", StringComparison.OrdinalIgnoreCase)
                                 || args[0] == "1";
                        if (newTx != _isTransmitting)
                        {
                            _isTransmitting = newTx;
                            stateChanged = true;
                        }
                    }
                    break;

                case "spot_activated":
                    // Lyra (and Thetis) broadcast this when the operator clicks
                    // a spot marker on the SDR's own panadapter:
                    //   spot_activated:CALLSIGN,MODE,FREQ_HZ,ARGB;
                    // Round-trip the click back into SDRLogger+ by reusing the
                    // SAME SpotSelectedEvent that the cluster / POTA / globe spot
                    // clicks fire — so the log-entry panel auto-populates the
                    // callsign, the QRZ lookup kicks off, and the log-history
                    // filter sets, exactly as an internal spot click does. This
                    // is the reverse of the outbound spot push (SDRLogger+ →
                    // Lyra waterfall): click a pushed spot on Lyra, log it here.
                    // Lyra also emits `rx_clicked_on_spot:0,0,CALL,HZ;` for the
                    // same click (no mode) — we ignore it so we don't fire twice.
                    if (args.Length >= 3 && !string.IsNullOrWhiteSpace(args[0]))
                    {
                        var spotCall = args[0].ToUpperInvariant();
                        var spotMode = !string.IsNullOrWhiteSpace(args[1])
                            ? args[1].ToUpperInvariant()
                            : null;
                        // TCI carries the spot frequency in Hz; SpotSelectedEvent
                        // (like all our spot events) is in kHz.
                        double spotFreqKhz = long.TryParse(args[2], out var spotHz)
                            ? spotHz / 1000.0
                            : 0;
                        try
                        {
                            await _hubContext.BroadcastSpotSelected(
                                new SpotSelectedEvent(spotCall, spotFreqKhz, spotMode, null));
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex,
                                "TCI spot_activated broadcast failed for {Call}", spotCall);
                        }
                    }
                    break;

                case "lyra_combo":
                    // Lyra ↔ SDRLogger+ Combo link (docs/COMBO_LINK.md). Lyra is
                    // the master; it announces on/off. We reflect it as the
                    // read-only "Lyra Combo: Linked" indicator and only act on
                    // lyra_contact while linked.
                    {
                        var on = args.Length >= 1
                                 && args[0].Equals("on", StringComparison.OrdinalIgnoreCase);
                        if (on != _comboLinked)
                        {
                            _comboLinked = on;
                            if (!on) _lastComboCall = null;
                            try
                            {
                                await _hubContext.BroadcastComboLinkChanged(
                                    new ComboLinkChangedEvent(_comboLinked, _device.Id));
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Combo link status broadcast failed");
                            }
                        }
                    }
                    break;

                case "lyra_contact":
                    // Inbound shared CW contact from Lyra:
                    //   lyra_contact:<src>,<call>,<rstSent>,<rstRcvd>,<name>,<qth>,<grid>,<serial>
                    // Act only on src=lyra (never our own src=sdrlog echoes) and
                    // only while linked. Reuse the SpotSelected pipeline so the
                    // log-entry callsign populates + the QRZ/HamQTH lookup fires,
                    // exactly like a spot click. Dedup on the call so a resend
                    // (or the name-back round-trip) can't re-fire the lookup.
                    if (_comboLinked && args.Length >= 2
                        && args[0].Equals("lyra", StringComparison.OrdinalIgnoreCase))
                    {
                        var comboCall = args[1].ToUpperInvariant();
                        if (comboCall.Length > 0
                            && !string.Equals(comboCall, _lastComboCall, StringComparison.Ordinal))
                        {
                            _lastComboCall = comboCall;
                            var comboGrid = args.Length >= 7 && !string.IsNullOrWhiteSpace(args[6])
                                ? args[6]
                                : null;
                            // TCI carries no freq here; use the current RX freq (kHz).
                            var comboFreqKhz = _currentFrequencyHz / 1000.0;
                            try
                            {
                                await _hubContext.BroadcastSpotSelected(
                                    new SpotSelectedEvent(comboCall, comboFreqKhz, "CW", comboGrid));
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex,
                                    "Combo contact populate failed for {Call}", comboCall);
                            }
                        }
                    }
                    break;

                case "lyra_log":
                    // Combo Stage B (docs/COMBO_LINK.md): Lyra sent a {LOG}-tagged
                    // CW macro → log the current QSO.
                    //   lyra_log:<call>,<rstSent>,<rstRcvd>,<mode>,<freqHz>
                    // Only while linked (Lyra is master + the {LOG} tag is the
                    // operator's explicit consent). Surface it to the frontend,
                    // which submits the populated Log Entry form; the RST/mode/
                    // freq ride along so the logged QSO matches the on-air
                    // exchange even if the operator never touched those fields.
                    if (_comboLinked && args.Length >= 1 && !string.IsNullOrWhiteSpace(args[0]))
                    {
                        var logCall = args[0].ToUpperInvariant();
                        var logFreqHz = args.Length >= 5 && long.TryParse(args[4], out var lf) ? lf : 0;
                        try
                        {
                            await _hubContext.BroadcastComboLogRequested(new ComboLogRequestedEvent(
                                logCall,
                                args.Length >= 2 && args[1].Length > 0 ? args[1] : null,
                                args.Length >= 3 && args[2].Length > 0 ? args[2] : null,
                                args.Length >= 4 && args[3].Length > 0 ? args[3] : null,
                                logFreqHz,
                                _device.Id));
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Combo log request failed for {Call}", logCall);
                        }
                    }
                    break;

                case "lyra_snr":
                    // Combo received-S auto-fill: Lyra's in-passband SNR (dB),
                    // sent alongside the S-meter while Combo is on. Feeds the
                    // meter stream so the frontend can gate its auto RST-Rcvd
                    // suggestion. lyra_snr:<snrDb>
                    if (args.Length >= 1 && double.TryParse(args[0], out var snrDb))
                    {
                        _meters.UpdateSnr(snrDb);
                    }
                    break;

                case "protocol":
                    // Server identification: protocol:name,version;
                    _logger.LogInformation("TCI protocol: {Args}", argsStr);
                    break;

                case "device":
                    // Device name: device:name;
                    _logger.LogInformation("TCI device: {Args}", argsStr);
                    break;

                case "ready":
                    _logger.LogInformation("TCI server ready");
                    await SendCommandAsync("start;");
                    // Thetis sensor streams (S-meter / TX power / SWR); radios
                    // that don't support them ignore the commands silently.
                    await SendCommandAsync("rx_sensors_enable:true,100;");
                    await SendCommandAsync("tx_sensors_enable:true,100;");
                    // Start the IQ panadapter stream.
                    await EnableIqStreamAsync();
                    break;

                case "rx_sensors":
                    // Format: rx_sensors:rx,dBm;
                    {
                        var reading = Tci.TciSensorParser.ParseRxSensors(args);
                        if (reading.HasValue && reading.Value.Rx == _selectedInstance)
                            _meters.UpdateRx(reading.Value);
                    }
                    break;

                case "rx_channel_sensors":
                case "rx_channel_sensors_ex":
                    // Format: rx_channel_sensors[_ex]:rx,ch,dBm[,avg,peakBin];
                    {
                        var reading = Tci.TciSensorParser.ParseRxChannelSensors(args);
                        if (reading.HasValue
                            && reading.Value.Rx == _selectedInstance
                            && reading.Value.Channel == 0)
                        {
                            _meters.UpdateRx(reading.Value);
                        }
                    }
                    break;

                case "tx_sensors":
                    // Format: tx_sensors:trx,micDbm,watts,peakWatts,swr;
                    // (sent for every trx — keep only the selected instance)
                    {
                        var reading = Tci.TciSensorParser.ParseTxSensors(args);
                        if (reading.HasValue && reading.Value.Trx == _selectedInstance)
                            _meters.UpdateTx(reading.Value);
                    }
                    break;
            }
        }

        if (stateChanged)
        {
            await BroadcastStateAsync();
        }

        if (_meters.TryGetSnapshot(DateTime.UtcNow, _device.Id, _isTransmitting, out var metersEvt))
        {
            try
            {
                await _hubContext.BroadcastTciMeters(metersEvt!);
            }
            catch (Exception ex)
            {
                // Meter data is advisory — a hub/serialization failure must
                // never tear down the radio connection's receive loop.
                _logger.LogWarning(ex, "TCI meters broadcast failed");
            }
        }
    }

    private async Task BroadcastStateAsync()
    {
        var state = GetCurrentState();
        if (state != null)
        {
            await _hubContext.BroadcastRadioStateChanged(state);
        }
    }
}
