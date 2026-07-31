using System.Diagnostics;
using System.Net.Sockets;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services.Rotator;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Service for controlling an antenna rotator over a persistent TCP connection.
/// Owns the connection lifecycle, polling loop, azimuth normalization, moving/target
/// tracking and SignalR broadcasting; the wire protocol is pluggable via
/// <see cref="IRotatorProtocol"/>, selected by <see cref="RotatorSettings.Protocol"/>:
/// - "rotctld": hamlib rotctld (default, port 4533)
/// - "arco_tcp": microHAM ARCO (GS-232A emulation over TCP)
///
/// Poll and command share one socket, so every exchange runs under <see cref="_io"/>.
/// Without it a heading commanded from the UI could interleave with a poll already in
/// flight, and the two would take each other's reply lines — the reported position then
/// lagged further behind with each command until the connection was rebuilt.
/// </summary>
public class RotatorService : BackgroundService
{
    private readonly ILogger<RotatorService> _logger;
    private readonly IHubContext<LogHub, ILogHubClient> _hubContext;
    private readonly IServiceScopeFactory _scopeFactory;

    private readonly IRotatorProtocol _rotctld = new RotctldProtocol();
    private readonly IRotatorProtocol _arco = new ArcoTcpProtocol();

    /// <summary>The wire protocol matching the current settings.</summary>
    private IRotatorProtocol Protocol =>
        _settings.Protocol == "arco_tcp" ? _arco : _rotctld;

    /// <summary>Serialises every exchange on the shared socket (poll, set, stop).</summary>
    private readonly SemaphoreSlim _io = new(1, 1);

    /// <summary>Settings live in LiteDB and every read decrypts credentials; polling at
    /// 2 Hz does not need that, and a busy database would stall the loop behind it.</summary>
    private static readonly TimeSpan SettingsRefreshInterval = TimeSpan.FromSeconds(2);

    /// <summary>A stalled SignalR client must not hold up the next poll for everyone else.</summary>
    private static readonly TimeSpan BroadcastTimeout = TimeSpan.FromSeconds(2);

    /// <summary>Unanswered polls tolerated before rebuilding the connection.</summary>
    private const int MaxSilentPolls = 3;

    private RotatorConnection? _connection;
    private readonly Stopwatch _sinceSettingsRefresh = Stopwatch.StartNew();
    private int _silentPolls;

    private RotatorSettings _settings = new();
    private double _currentAzimuth;
    private double? _targetAzimuth;
    private bool _isMoving;
    private bool _isConnected;

    public RotatorService(
        ILogger<RotatorService> logger,
        IHubContext<LogHub, ILogHubClient> hubContext,
        IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _hubContext = hubContext;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Rotator service starting...");

        // Load initial settings
        await RefreshSettingsAsync();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (_sinceSettingsRefresh.Elapsed >= SettingsRefreshInterval)
                {
                    await RefreshSettingsAsync();
                    _sinceSettingsRefresh.Restart();
                }

                if (!_settings.Enabled)
                {
                    if (_isConnected)
                    {
                        Disconnect();
                    }
                    await Task.Delay(5000, stoppingToken);
                    continue;
                }

                // Try to connect if not connected
                if (!_isConnected)
                {
                    await ConnectAsync(stoppingToken);
                }

                if (_isConnected)
                {
                    // Poll current position
                    await PollPositionAsync(stoppingToken);
                }

                await Task.Delay(_settings.PollingIntervalMs, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in rotator service loop");
                Disconnect();
                await Task.Delay(5000, stoppingToken);
            }
        }

        Disconnect();
        _logger.LogInformation("Rotator service stopped");
    }

    private async Task RefreshSettingsAsync()
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
            var userSettings = await settingsService.GetSettingsAsync();
            var newSettings = userSettings.Rotator;

            // Check if connection settings changed
            if (_settings.IpAddress != newSettings.IpAddress ||
                _settings.Port != newSettings.Port ||
                _settings.Protocol != newSettings.Protocol ||
                _settings.Enabled != newSettings.Enabled)
            {
                if (_isConnected && _settings.Enabled)
                {
                    _logger.LogInformation("Rotator settings changed, reconnecting...");
                    Disconnect();
                }
            }

            _settings = newSettings;

            _logger.LogDebug("Rotator settings: Enabled={Enabled}, IP={Ip}, Port={Port}",
                _settings.Enabled, _settings.IpAddress, _settings.Port);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to refresh rotator settings");
        }
    }

    private async Task ConnectAsync(CancellationToken ct)
    {
        try
        {
            _logger.LogInformation("Connecting to rotctld at {Ip}:{Port}...",
                _settings.IpAddress, _settings.Port);

            var client = new TcpClient();
            await client.ConnectAsync(_settings.IpAddress!, _settings.Port, ct);

            _connection = new RotatorConnection(client);
            _isConnected = true;
            _silentPolls = 0;
            _logger.LogInformation("Connected to rotctld at {Ip}:{Port}",
                _settings.IpAddress, _settings.Port);

            // Initial position poll
            await PollPositionAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to connect to rotctld at {Ip}:{Port}: {Message}",
                _settings.IpAddress, _settings.Port, ex.Message);
            Disconnect();
        }
    }

    private void Disconnect()
    {
        try
        {
            _connection?.Dispose();
        }
        catch { }
        finally
        {
            _connection = null;
            _isConnected = false;
            _silentPolls = 0;
        }
    }

    private async Task PollPositionAsync(CancellationToken ct)
    {
        var connection = _connection;
        if (connection == null) return;

        double? raw;
        await _io.WaitAsync(ct);
        try
        {
            raw = await Protocol.PollAzimuthAsync(connection, ct);
        }
        catch (IOException ex)
        {
            _logger.LogWarning("Lost connection to rotctld: {Message}", ex.Message);
            Disconnect();
            return;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A non-IOException fault (e.g. ObjectDisposedException on a dead socket) is still a
            // failed poll — count it toward the silent-poll budget and rebuild if the controller has
            // gone quiet, otherwise a persistent non-IO fault would loop forever without reconnecting.
            _silentPolls++;
            _logger.LogWarning(ex, "Error polling rotator position (attempt {Attempt} of {Max})",
                _silentPolls, MaxSilentPolls);
            if (_silentPolls >= MaxSilentPolls || connection.IsClosed)
            {
                _logger.LogWarning("Rotator not answering — reconnecting to resynchronise");
                Disconnect();
            }
            return;
        }
        finally
        {
            _io.Release();
        }

        if (raw is not double azimuth)
        {
            // Either the controller said nothing within the protocol's timeout or its answer
            // did not parse. Rebuild the connection rather than keep reading a stream we may
            // no longer be in step with — a fresh socket is the one state we can trust.
            _silentPolls++;
            _logger.LogWarning("No usable rotator position (attempt {Attempt} of {Max})",
                _silentPolls, MaxSilentPolls);

            if (_silentPolls >= MaxSilentPolls || connection.IsClosed)
            {
                _logger.LogWarning("Rotator not answering — reconnecting to resynchronise");
                Disconnect();
            }
            return;
        }

        _silentPolls = 0;

        // Normalize azimuth to 0-360 (rotctld can return negative values like -75 for 285°)
        azimuth = ((azimuth % 360) + 360) % 360;

        var previousAzimuth = _currentAzimuth;
        _currentAzimuth = azimuth;

        // Determine if moving (azimuth changed since last poll)
        _isMoving = Math.Abs(_currentAzimuth - previousAzimuth) > 0.5;

        // Clear target if we've reached it
        if (_targetAzimuth.HasValue && Math.Abs(_currentAzimuth - _targetAzimuth.Value) < 2.0)
        {
            _targetAzimuth = null;
            _isMoving = false;
        }

        _logger.LogDebug("Rotator position: {Azimuth:F1}° (moving: {IsMoving})",
            _currentAzimuth, _isMoving);

        // Broadcast position update
        await BroadcastPositionAsync();
    }

    private async Task BroadcastPositionAsync()
    {
        var evt = new RotatorPositionEvent(
            _settings.RotatorId,
            _currentAzimuth,
            _isMoving,
            _targetAzimuth
        );

        try
        {
            await _hubContext.Clients.All.OnRotatorPosition(evt).WaitAsync(BroadcastTimeout);
        }
        catch (TimeoutException)
        {
            // One slow client (a backgrounded tab, a multi-op operator on a poor link) must
            // not pace the polling loop for everyone.
            _logger.LogDebug("Rotator position broadcast timed out");
        }
    }

    /// <summary>
    /// Command the rotator to move to a target azimuth.
    /// Called from LogHub.
    /// </summary>
    public async Task SetPositionAsync(double targetAzimuth)
    {
        // Normalize to 0-360
        targetAzimuth = ((targetAzimuth % 360) + 360) % 360;

        _logger.LogInformation("Commanding rotator to {Azimuth}°", targetAzimuth);

        var connection = _connection;
        if (!_isConnected || connection == null)
        {
            _logger.LogWarning("Cannot command rotator - not connected");

            // Still broadcast the target so UI updates
            _targetAzimuth = targetAzimuth;
            _isMoving = true;
            await BroadcastPositionAsync();
            return;
        }

        try
        {
            _targetAzimuth = targetAzimuth;
            _isMoving = true;

            await _io.WaitAsync();
            try
            {
                await Protocol.SetAzimuthAsync(targetAzimuth, connection, CancellationToken.None);
            }
            finally
            {
                _io.Release();
            }

            // Broadcast updated state
            await BroadcastPositionAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error commanding rotator");
            Disconnect();
        }
    }

    /// <summary>
    /// Stop the rotator.
    /// </summary>
    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping rotator");

        var connection = _connection;
        if (!_isConnected || connection == null)
        {
            _logger.LogWarning("Cannot stop rotator - not connected");
            return;
        }

        try
        {
            await _io.WaitAsync();
            try
            {
                await Protocol.StopAsync(connection, CancellationToken.None);
            }
            finally
            {
                _io.Release();
            }

            _isMoving = false;
            _targetAzimuth = null;

            await BroadcastPositionAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stopping rotator");
            Disconnect();
        }
    }

    /// <summary>
    /// Get current rotator status.
    /// </summary>
    public RotatorPositionEvent GetCurrentStatus()
    {
        return new RotatorPositionEvent(
            _settings.RotatorId,
            _currentAzimuth,
            _isMoving,
            _targetAzimuth
        );
    }

    /// <summary>
    /// Check if rotator is connected.
    /// </summary>
    public bool IsConnected => _isConnected;
}
