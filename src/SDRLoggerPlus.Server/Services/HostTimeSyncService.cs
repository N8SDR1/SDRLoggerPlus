using System.Net.Http.Headers;
using System.Net.Http.Json;
using SDRLoggerPlus.Server.Core.Time;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// On a field CLIENT, periodically measures this machine's clock offset from the HOST (SNTP-style) and
/// stores it in <see cref="HostTimeOffset"/>. The host is the group's time authority, so all stations
/// agree even with no internet — the offset is what the logging path uses to stamp contacts in the
/// host's frame and what the UI shows as the sync state. Registered client-mode only.
/// </summary>
public sealed class HostTimeSyncService : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(2);

    private readonly HostTimeOffset _offset;
    private readonly IUserConfigService _config;
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger<HostTimeSyncService> _log;

    public HostTimeSyncService(HostTimeOffset offset, IUserConfigService config,
        IHttpClientFactory httpFactory, ILogger<HostTimeSyncService> log)
    {
        _offset = offset;
        _config = config;
        _httpFactory = httpFactory;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await SyncOnce(stoppingToken); }
            catch (Exception ex) { _log.LogDebug("Time sync failed: {Msg}", ex.Message); }
            try { await Task.Delay(Interval, stoppingToken); } catch { }
        }
    }

    private async Task SyncOnce(CancellationToken ct)
    {
        var cfg = await _config.GetConfigAsync();
        if (string.IsNullOrWhiteSpace(cfg.HostUrl)) return;

        var client = _httpFactory.CreateClient();
        client.BaseAddress = new Uri(cfg.HostUrl.TrimEnd('/') + "/");
        client.Timeout = TimeSpan.FromSeconds(10);
        if (!string.IsNullOrWhiteSpace(cfg.HostToken))
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", cfg.HostToken);

        // SNTP-lite: the host's UTC corresponds ~to the midpoint of our round trip.
        var t0 = DateTime.UtcNow;
        var dto = await client.GetFromJsonAsync<TimeDto>("api/time", ct);
        var t1 = DateTime.UtcNow;
        if (dto is null) return;

        var localMidpoint = t0 + TimeSpan.FromTicks((t1 - t0).Ticks / 2);
        var offset = dto.Utc - localMidpoint;
        _offset.Update(offset);
        _log.LogInformation("Time sync: this clock is {Ms:F0} ms from the host.", offset.TotalMilliseconds);
    }

    private sealed record TimeDto(DateTime Utc);
}
