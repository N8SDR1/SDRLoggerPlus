using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;
using Microsoft.AspNetCore.SignalR;

namespace SDRLoggerPlus.Server.Services.Weather;

public record LightningStatus(
    bool Active,
    double? ClosestKm,
    double? ClosestMi,
    string Direction,
    // Aggregate strike count across enabled sources. Deliberately NOT named
    // for a time window: each source counts over its own — Blitzortung is a
    // fresh ~10-minute snapshot (slices {0,1}), Ambient reports the last
    // hour, Ecowitt its own firmware-defined counter. It is an activity
    // magnitude, not a rate.
    int StrikeCount,
    List<string> Sources,
    string? NwsWarning,
    DateTime? LastUpdateUtc);

public record WindStatus(
    bool Active,
    string Severity,
    double? SustainedMph,
    double? GustMph,
    double? SustainedKph,
    double? GustKph,
    string Direction,
    List<string> Sources,
    string? NwsAlert,
    DateTime? LastUpdateUtc,
    string Unit,
    double ThreshSustMph,
    double ThreshGustMph);

/// <summary>
/// Station weather alerts (SDRLogger+ port): lightning proximity polled every
/// 90 s and high-wind polled every 120 s, each aggregating its enabled
/// sources. Wind takes the max sustained/gust across sources; severity tiers
/// come from WindSeverity; after conditions clear, new "elevated"-only alerts
/// are suppressed for the configured cooldown.
/// </summary>
public class WeatherAlertService : BackgroundService
{
    private static readonly TimeSpan LightningInterval = TimeSpan.FromSeconds(90);
    private static readonly TimeSpan WindInterval = TimeSpan.FromSeconds(120);

    private readonly IServiceProvider _serviceProvider;
    private readonly INwsClient _nws;
    private readonly IAmbientWeatherClient _ambient;
    private readonly IEcowittClient _ecowitt;
    private readonly IBlitzortungClient _blitzortung;
    private readonly IHubContext<LogHub, ILogHubClient>? _hubContext;
    private readonly ILogger<WeatherAlertService> _logger;

    private LightningStatus _lightning = new(false, null, null, "", 0, [], null, null);
    private WindStatus _wind = new(false, "", null, null, null, null, "", [], null, null, "mph", 30, 45);
    private DateTime _windLastClearUtc = DateTime.MinValue;
    private readonly object _statusLock = new();

    public WeatherAlertService(
        IServiceProvider serviceProvider,
        INwsClient nws,
        IAmbientWeatherClient ambient,
        IEcowittClient ecowitt,
        IBlitzortungClient blitzortung,
        ILogger<WeatherAlertService> logger,
        IHubContext<LogHub, ILogHubClient>? hubContext = null)
    {
        _serviceProvider = serviceProvider;
        _nws = nws;
        _ambient = ambient;
        _ecowitt = ecowitt;
        _blitzortung = blitzortung;
        _hubContext = hubContext;
        _logger = logger;
    }

    public LightningStatus GetLightningStatus() { lock (_statusLock) return _lightning; }
    public WindStatus GetWindStatus() { lock (_statusLock) return _wind; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Weather alert service starting");
        var lastLightning = DateTime.MinValue;
        var lastWind = DateTime.MinValue;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await ReadSettingsAsync();
                var now = DateTime.UtcNow;

                if (settings.Weather.Lightning.Enabled && now - lastLightning >= LightningInterval)
                {
                    lastLightning = now;
                    await PollLightningAsync(settings, stoppingToken);
                }
                else if (!settings.Weather.Lightning.Enabled && _lightning.Active)
                {
                    lock (_statusLock) _lightning = _lightning with { Active = false };
                }

                if (settings.Weather.Wind.Enabled && now - lastWind >= WindInterval)
                {
                    lastWind = now;
                    await PollWindAsync(settings, stoppingToken);
                }
                else if (!settings.Weather.Wind.Enabled && _wind.Active)
                {
                    lock (_statusLock) _wind = _wind with { Active = false, Severity = "" };
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Weather alert poll error");
            }

            try { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    internal async Task PollLightningAsync(UserSettings settings, CancellationToken ct)
    {
        var location = ResolveLocation(settings);
        var lightning = settings.Weather.Lightning;
        var rangeKm = lightning.RangeUnit == "mi" ? lightning.Range * 1.60934 : lightning.Range;

        double? closestKm = null;
        double? closestBearing = null;
        var totalStrikes = 0;
        var sources = new List<string>();
        string? nwsWarning = null;

        if (lightning.UseBlitzortung && location != null)
        {
            var strikes = await _blitzortung.GetStrikesAsync(location.Value.Lat, location.Value.Lon, rangeKm, ct);
            if (strikes.Count > 0)
            {
                sources.Add("blitzortung");
                totalStrikes += strikes.Count;
                var closest = strikes.MinBy(s => s.DistanceKm)!;
                if (closestKm == null || closest.DistanceKm < closestKm)
                {
                    closestKm = closest.DistanceKm;
                    closestBearing = closest.BearingDeg;
                }
            }
        }

        if (lightning.UseNws && location != null)
        {
            nwsWarning = await _nws.GetThunderstormWarningAsync(location.Value.Lat, location.Value.Lon, ct);
            if (!string.IsNullOrEmpty(nwsWarning)) sources.Add("nws");
        }

        if (lightning.UseAmbient && AmbientCreds(settings) is { } ambientCreds)
        {
            var reading = await _ambient.GetLightningAsync(ambientCreds, ct);
            if (reading?.DistanceKm != null && reading.StrikeCount > 0)
            {
                sources.Add("ambient");
                totalStrikes += reading.StrikeCount;
                if (closestKm == null || reading.DistanceKm < closestKm)
                    closestKm = reading.DistanceKm; // PWS gives no bearing
            }
        }

        if (lightning.UseEcowitt && EcowittCreds(settings) is { } ecowittCreds)
        {
            var reading = await _ecowitt.GetLightningAsync(ecowittCreds, ct);
            if (reading?.DistanceKm != null && reading.StrikeCount > 0)
            {
                sources.Add("ecowitt");
                totalStrikes += reading.StrikeCount;
                if (closestKm == null || reading.DistanceKm < closestKm)
                    closestKm = reading.DistanceKm;
            }
        }

        var status = new LightningStatus(
            Active: (closestKm != null && closestKm <= rangeKm) || !string.IsNullOrEmpty(nwsWarning),
            ClosestKm: closestKm != null ? Math.Round(closestKm.Value, 1) : null,
            ClosestMi: closestKm != null ? Math.Round(closestKm.Value / 1.60934, 1) : null,
            Direction: closestBearing != null ? GeoMath.BearingToCompass(closestBearing.Value) : "",
            StrikeCount: totalStrikes,
            Sources: sources,
            NwsWarning: nwsWarning,
            LastUpdateUtc: DateTime.UtcNow);

        lock (_statusLock) _lightning = status;
        if (_hubContext != null)
            await _hubContext.Clients.All.OnLightningStatus(status);
    }

    internal async Task PollWindAsync(UserSettings settings, CancellationToken ct)
    {
        var location = ResolveLocation(settings);
        var wind = settings.Weather.Wind;

        double? bestSust = null;
        double? bestGust = null;
        double? bestDir = null;
        var sources = new List<string>();
        string? nwsEvent = null;
        var nwsIsExtreme = false;

        if (wind.UseNwsAlerts && location != null)
        {
            var alert = await _nws.GetWindAlertAsync(location.Value.Lat, location.Value.Lon, ct);
            if (alert != null)
            {
                nwsEvent = alert.Headline;
                nwsIsExtreme = alert.IsExtreme;
                sources.Add("nws_alert");
            }
        }

        void Aggregate(WindReading? reading, string source)
        {
            if (reading == null || (reading.SustainedMph == null && reading.GustMph == null)) return;
            sources.Add(source);
            if (reading.SustainedMph != null && (bestSust == null || reading.SustainedMph > bestSust))
            {
                bestSust = reading.SustainedMph;
                bestDir = reading.DirectionDeg;
            }
            if (reading.GustMph != null && (bestGust == null || reading.GustMph > bestGust))
                bestGust = reading.GustMph;
        }

        if (wind.UseNwsMetar && !string.IsNullOrWhiteSpace(wind.MetarStation))
            Aggregate(await _nws.GetMetarWindAsync(wind.MetarStation, ct), $"metar/{wind.MetarStation.ToUpperInvariant()}");

        if (wind.UseAmbient && AmbientCreds(settings) is { } ambientCreds)
            Aggregate(await _ambient.GetWindAsync(ambientCreds, ct), "ambient");

        if (wind.UseEcowitt && EcowittCreds(settings) is { } ecowittCreds)
            Aggregate(await _ecowitt.GetWindAsync(ecowittCreds, ct), "ecowitt");

        var severity = WindSeverity.Classify(bestSust, bestGust, nwsEvent, nwsIsExtreme,
            wind.ThreshSustainedMph, wind.ThreshGustMph);

        // Cooldown: once conditions clear, suppress elevated-only re-alerts
        var now = DateTime.UtcNow;
        if (severity == WindSeverity.None)
        {
            _windLastClearUtc = now;
        }
        else if (severity == WindSeverity.Elevated &&
                 now - _windLastClearUtc < TimeSpan.FromMinutes(wind.CooldownMinutes))
        {
            severity = WindSeverity.None;
        }

        var status = new WindStatus(
            Active: severity != WindSeverity.None,
            Severity: severity,
            SustainedMph: Round1(bestSust),
            GustMph: Round1(bestGust),
            SustainedKph: Round1(bestSust * 1.60934),
            GustKph: Round1(bestGust * 1.60934),
            Direction: bestDir != null ? GeoMath.BearingToCompass(bestDir.Value) : "",
            Sources: sources,
            NwsAlert: nwsEvent,
            LastUpdateUtc: now,
            Unit: wind.DisplayUnit,
            ThreshSustMph: wind.ThreshSustainedMph,
            ThreshGustMph: wind.ThreshGustMph);

        lock (_statusLock) _wind = status;
        if (_hubContext != null)
            await _hubContext.Clients.All.OnWindStatus(status);
    }

    private static double? Round1(double? v) => v != null ? Math.Round(v.Value, 1) : null;

    private static (double Lat, double Lon)? ResolveLocation(UserSettings settings)
    {
        var station = settings.Station;
        if (station.Latitude != null && station.Longitude != null)
            return (station.Latitude.Value, station.Longitude.Value);
        return GeoMath.GridToLatLon(station.GridSquare);
    }

    private static AmbientCredentials? AmbientCreds(UserSettings settings)
    {
        var c = settings.Weather.Credentials;
        return !string.IsNullOrWhiteSpace(c.AmbientApiKey) && !string.IsNullOrWhiteSpace(c.AmbientAppKey)
            ? new AmbientCredentials(c.AmbientApiKey, c.AmbientAppKey)
            : null;
    }

    private static EcowittCredentials? EcowittCreds(UserSettings settings)
    {
        var c = settings.Weather.Credentials;
        return !string.IsNullOrWhiteSpace(c.EcowittAppKey) && !string.IsNullOrWhiteSpace(c.EcowittApiKey)
               && !string.IsNullOrWhiteSpace(c.EcowittMac)
            ? new EcowittCredentials(c.EcowittAppKey, c.EcowittApiKey, c.EcowittMac)
            : null;
    }

    private async Task<UserSettings> ReadSettingsAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        return await settingsService.GetSettingsAsync();
    }
}
