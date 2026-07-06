using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services.Weather;

/// <summary>
/// Polls Blitzortung's worldwide strike feed — only while the globe lightning
/// toggle is enabled. The feed's `n` parameter is a 5-minute time slice (0 =
/// newest), NOT a geographic region; slices {0,1} cover the buffer's full
/// 10-minute retention window. Strikes within LocalRadiusKm of the station are
/// tagged Local (bright tier). Feeds a StrikeBuffer and pushes new strikes over
/// SignalR. Ephemeral; nothing is persisted.
/// </summary>
public class LightningStrikeService : BackgroundService
{
    private static readonly int[] TimeSlices = { 0, 1 };
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(60);
    private const double LocalRadiusKm = 750;

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
        var lastPoll = DateTime.MinValue;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await ReadSettingsAsync();
                if (settings.Map.ShowLightning && DateTime.UtcNow - lastPoll >= PollInterval)
                {
                    lastPoll = DateTime.UtcNow;
                    var (stationLat, stationLon) = ResolveStation(settings);
                    await PollAsync(stationLat, stationLon, stoppingToken);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _logger.LogWarning(ex, "Lightning strike poll error"); }

            try { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task PollAsync(double? stationLat, double? stationLon, CancellationToken ct)
    {
        var raw = await _blitzortung.GetStrikesRawAsync(TimeSlices, ct);
        var tagged = raw.Select(s => s with
        {
            Local = stationLat.HasValue && stationLon.HasValue &&
                    PropagationService.HaversineDistanceKm(stationLat.Value, stationLon.Value, s.Lat, s.Lon) <= LocalRadiusKm
        }).ToList();
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
