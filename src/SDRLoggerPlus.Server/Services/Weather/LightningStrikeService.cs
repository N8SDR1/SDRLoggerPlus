using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services.Weather;

/// <summary>
/// Polls Blitzortung on two cadences — local regions (Americas) fast, all regions
/// slow — only while the globe lightning toggle is enabled. Feeds a StrikeBuffer
/// and pushes new strikes over SignalR. Ephemeral; nothing is persisted.
/// </summary>
public class LightningStrikeService : BackgroundService
{
    private static readonly int[] LocalRegions = { 7, 12, 13 };
    private static readonly int[] GlobalRegions = Enumerable.Range(1, 14).Except(LocalRegions).ToArray();
    private static readonly TimeSpan LocalInterval = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan GlobalInterval = TimeSpan.FromSeconds(300);

    private readonly IServiceProvider _serviceProvider;
    private readonly IBlitzortungClient _blitzortung;
    private readonly IHubContext<LogHub, ILogHubClient>? _hubContext;
    private readonly ILogger<LightningStrikeService> _logger;
    private readonly StrikeBuffer _buffer = new(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10), cap: 2000);
    private readonly object _lock = new();

    public LightningStrikeService(
        IServiceProvider serviceProvider,
        IBlitzortungClient blitzortung,
        ILogger<LightningStrikeService> logger,
        IHubContext<LogHub, ILogHubClient>? hubContext = null)
    {
        _serviceProvider = serviceProvider;
        _blitzortung = blitzortung;
        _hubContext = hubContext;
        _logger = logger;
    }

    public IReadOnlyList<LightningStrike> GetCurrent()
    {
        lock (_lock) return _buffer.Current(DateTime.UtcNow);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var lastLocal = DateTime.MinValue;
        var lastGlobal = DateTime.MinValue;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await ReadSettingsAsync();
                if (settings.Map.ShowLightning)
                {
                    var now = DateTime.UtcNow;
                    var (stationLat, stationLon) = ResolveStation(settings);

                    if (now - lastLocal >= LocalInterval)
                    {
                        lastLocal = now;
                        await PollAsync(LocalRegions, local: true, stationLat, stationLon, stoppingToken);
                    }
                    if (now - lastGlobal >= GlobalInterval)
                    {
                        lastGlobal = now;
                        await PollAsync(GlobalRegions, local: false, stationLat, stationLon, stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _logger.LogWarning(ex, "Lightning strike poll error"); }

            try { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task PollAsync(int[] regions, bool local, double? stationLat, double? stationLon, CancellationToken ct)
    {
        var raw = await _blitzortung.GetStrikesRawAsync(regions, ct);
        // Local and global region sets are disjoint, so the whole batch shares this tier's tag.
        var tagged = raw.Select(s => s with { Local = local }).ToList();
        IReadOnlyList<LightningStrike> added;
        lock (_lock) added = _buffer.Add(tagged, DateTime.UtcNow, stationLat, stationLon);
        if (added.Count > 0 && _hubContext != null)
            await _hubContext.BroadcastLightningStrikes(new LightningStrikesEvent(added));
    }

    private static (double?, double?) ResolveStation(UserSettings settings)
    {
        var st = settings.Station;
        if (st?.Latitude is double lat && st?.Longitude is double lon) return (lat, lon);
        return (null, null);
    }

    private async Task<UserSettings> ReadSettingsAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        return await settingsService.GetSettingsAsync();
    }
}
