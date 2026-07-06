using System.Net;
using System.Net.Sockets;
using System.Text;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Generic ADIF-over-UDP listener (v1 SDRLogger+ port). Binds a UDP
/// socket, treats each datagram as a UTF-8 ADIF payload (with or
/// without a preceding &lt;EOH&gt; header), and pushes it through the
/// shared AdifService.ImportAdifAsync pipeline. That gives us dedup,
/// DXCC back-fill from cty.dat, and a SignalR broadcast to any open
/// clients — same behaviour as a file-based ADIF Monitor tick.
///
/// Reconfiguration polls the settings every 3 seconds; toggling
/// Enabled or changing the Port rebinds the socket without a restart.
/// Multiple senders (VarAC + N1MM + Logger32 all pointed at the same
/// port) are fine — datagrams arrive interleaved and each is parsed
/// independently.
/// </summary>
public class AdifUdpListenerService : BackgroundService
{
    private readonly IServiceProvider _services;
    private readonly ILogger<AdifUdpListenerService> _logger;

    private UdpClient? _client;
    private bool _currentEnabled;
    private int _currentPort;
    private static readonly TimeSpan SettingsPollInterval = TimeSpan.FromSeconds(3);

    public AdifUdpListenerService(IServiceProvider services, ILogger<AdifUdpListenerService> logger)
    {
        _services = services;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ADIF-over-UDP listener starting (dormant until enabled in Settings)");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await GetSettingsAsync();

                // Rebind if the enabled flag flipped or the port changed.
                if (settings.Enabled != _currentEnabled || settings.Port != _currentPort)
                {
                    Stop();
                    if (settings.Enabled) Start(settings.Port);
                    _currentEnabled = settings.Enabled;
                    _currentPort = settings.Port;
                }

                if (_client is null)
                {
                    // Not listening — sleep and re-check settings.
                    await Task.Delay(SettingsPollInterval, stoppingToken);
                    continue;
                }

                // Receive with a settings-poll timeout so a config change
                // doesn't have to wait for a datagram to unblock us.
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                timeoutCts.CancelAfter(SettingsPollInterval);
                UdpReceiveResult result;
                try
                {
                    result = await _client.ReceiveAsync(timeoutCts.Token);
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    continue;
                }

                await HandleDatagramAsync(result, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "ADIF-over-UDP loop error — restarting listener in 5 s");
                Stop();
                try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
                catch (OperationCanceledException) { break; }
            }
        }

        Stop();
    }

    private async Task HandleDatagramAsync(UdpReceiveResult result, CancellationToken ct)
    {
        var text = Encoding.UTF8.GetString(result.Buffer).Trim();
        if (string.IsNullOrWhiteSpace(text)) return;

        _logger.LogDebug("ADIF-over-UDP: {Bytes} B from {Peer}",
            result.Buffer.Length, result.RemoteEndPoint);

        // AdifService expects the ADIF stream to include an <EOH>
        // (end-of-header) marker before the first record. A lot of the
        // loggers on this port skip the header block, so we prepend a
        // synthetic <EOH> if the datagram doesn't have one already.
        // Doubling the header when it IS present is harmless (empty
        // header block).
        if (text.IndexOf("<eoh>", StringComparison.OrdinalIgnoreCase) < 0)
        {
            text = "<EOH>\n" + text;
        }

        using var scope = _services.CreateScope();
        var adif = scope.ServiceProvider.GetRequiredService<IAdifService>();
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(text));
        var report = await adif.ImportAdifAsync(stream, skipDuplicates: true,
            markAsSyncedToQrz: false, cancellationToken: ct);
        if (report.ImportedCount > 0)
        {
            _logger.LogInformation("ADIF-over-UDP: imported {N} QSO(s) from {Peer}",
                report.ImportedCount, result.RemoteEndPoint);
        }
        else if (report.SkippedDuplicates > 0 || report.ErrorCount > 0)
        {
            _logger.LogDebug("ADIF-over-UDP from {Peer}: {Dup} dup / {Err} err",
                result.RemoteEndPoint, report.SkippedDuplicates, report.ErrorCount);
        }
    }

    private void Start(int port)
    {
        try
        {
            _client = new UdpClient();
            _client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _client.Client.Bind(new IPEndPoint(IPAddress.Any, port));
            _logger.LogInformation("ADIF-over-UDP listener bound on port {Port}", port);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("ADIF-over-UDP bind failed on port {Port}: {Error}", port, ex.Message);
            _client?.Dispose();
            _client = null;
        }
    }

    private void Stop()
    {
        _client?.Dispose();
        _client = null;
    }

    private async Task<AdifUdpSettings> GetSettingsAsync()
    {
        using var scope = _services.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var all = await settingsService.GetSettingsAsync();
        return all.AdifUdp;
    }
}
