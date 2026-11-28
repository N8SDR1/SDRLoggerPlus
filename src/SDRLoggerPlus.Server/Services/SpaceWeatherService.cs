using System.Text.Json;
using System.Text.Json.Serialization;

namespace SDRLoggerPlus.Server.Services;

public record SpaceWeatherData(
    int SolarFluxIndex,    // SFI: 10.7cm solar flux (typical range 65-300, higher is better for HF)
    int KIndex,            // Planetary K-index (0-9, lower is better, >=4 indicates geomagnetic storm)
    int SunspotNumber,     // SSN: Daily sunspot number (0-300+, higher indicates more solar activity)
    DateTime Timestamp
);

public interface ISpaceWeatherService
{
    Task<SpaceWeatherData> GetCurrentAsync();
}

public class SpaceWeatherService : ISpaceWeatherService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<SpaceWeatherService> _logger;

    private SpaceWeatherData? _cached;
    private DateTime _lastFetch = DateTime.MinValue;
    private readonly TimeSpan _cacheExpiration = TimeSpan.FromMinutes(15);
    private readonly SemaphoreSlim _fetchLock = new(1, 1);

    public SpaceWeatherService(IHttpClientFactory httpClientFactory, ILogger<SpaceWeatherService> logger)
    {
        _httpClient = httpClientFactory.CreateClient();
        _httpClient.Timeout = TimeSpan.FromSeconds(10);
        _logger = logger;
    }

    public async Task<SpaceWeatherData> GetCurrentAsync()
    {
        if (_cached != null && DateTime.UtcNow - _lastFetch < _cacheExpiration)
            return _cached;

        await _fetchLock.WaitAsync();
        try
        {
            // Double-check after acquiring lock
            if (_cached != null && DateTime.UtcNow - _lastFetch < _cacheExpiration)
                return _cached;

            var data = await FetchSpaceWeatherData();
            _cached = data;
            _lastFetch = DateTime.UtcNow;
            return data;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to fetch space weather data");
            return _cached ?? new SpaceWeatherData(70, 2, 0, DateTime.UtcNow);
        }
        finally
        {
            _fetchLock.Release();
        }
    }

    /// <summary>Daily 10.7 cm solar flux (Penticton, three readings a day).</summary>
    public const string DailyFluxUrl = "https://services.swpc.noaa.gov/json/f107_cm_flux.json";
    /// <summary>Daily SWPC sunspot number, one value per day.</summary>
    public const string DailySunspotUrl = "https://services.swpc.noaa.gov/json/solar-cycle/swpc_observed_ssn.json";
    public const string KIndexUrl = "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json";
    public const string HamQslUrl = "https://www.hamqsl.com/solarxml.php";

    /// <summary>
    /// A daily value older than this is not "current"; use HamQSL instead.
    /// NOAA posts flux three times a day and the sunspot number daily, so a
    /// healthy feed is never more than about a day behind.
    /// </summary>
    public static readonly TimeSpan MaxDailyAge = TimeSpan.FromDays(3);

    // SFI and SSN come from NOAA's DAILY feeds. The previous source,
    // solar-cycle/observed-solar-cycle-indices.json, is a MONTHLY series: its
    // newest row is last month's average (Nov 27, 2026 showed SFI 108 / SSN 46,
    // October's means, while the day's values were 91 / 30). The header, the
    // Solar panel, the DX Coach and the propagation forecast all read these.
    private async Task<SpaceWeatherData> FetchSpaceWeatherData()
    {
        try
        {
            var fluxJson = await _httpClient.GetStringAsync(DailyFluxUrl);
            var ssnJson = await _httpClient.GetStringAsync(DailySunspotUrl);
            var flux = LatestDailyFlux(fluxJson);
            var ssn = LatestDailySunspots(ssnJson);
            var now = DateTime.UtcNow;

            if (flux is { } f && ssn is { } n
                && now - f.TimeUtc <= MaxDailyAge && now - n.DateUtc <= MaxDailyAge)
            {
                var kIndex = await FetchKIndex();
                return new SpaceWeatherData((int)Math.Round(f.Flux), kIndex, n.Ssn, f.TimeUtc);
            }

            _logger.LogWarning("NOAA daily solar data missing or stale (flux {Flux}, SSN {Ssn}); using HamQSL",
                flux?.TimeUtc, ssn?.DateUtc);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fetch from NOAA, trying fallback");
        }

        return await FetchFromHamQSL();
    }

    /// <summary>Newest 2800 MHz flux reading, or null when the feed has none.</summary>
    public static (double Flux, DateTime TimeUtc)? LatestDailyFlux(string json)
    {
        var rows = JsonSerializer.Deserialize<List<NoaaDailyFlux>>(json);
        var latest = rows?
            .Where(r => r.Frequency == 2800 && r.Flux > 0 && TryUtc(r.TimeTag, out _))
            .OrderByDescending(r => ParseUtc(r.TimeTag))
            .FirstOrDefault();
        return latest == null ? null : (latest.Flux, ParseUtc(latest.TimeTag));
    }

    /// <summary>Newest daily sunspot number, or null when the feed has none.</summary>
    public static (int Ssn, DateTime DateUtc)? LatestDailySunspots(string json)
    {
        var rows = JsonSerializer.Deserialize<List<NoaaDailySunspots>>(json);
        var latest = rows?
            .Where(r => r.Ssn >= 0 && TryUtc(r.ObsDate, out _))
            .OrderByDescending(r => ParseUtc(r.ObsDate))
            .FirstOrDefault();
        return latest == null ? null : (latest.Ssn, ParseUtc(latest.ObsDate));
    }

    // NOAA time tags carry no zone and are UTC.
    private static bool TryUtc(string? s, out DateTime utc)
    {
        var ok = DateTime.TryParse(s, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out utc);
        return ok;
    }

    private static DateTime ParseUtc(string? s) => TryUtc(s, out var utc) ? utc : DateTime.MinValue;

    private async Task<int> FetchKIndex()
    {
        try
        {
            var response = await _httpClient.GetStringAsync(KIndexUrl);
            var data = JsonSerializer.Deserialize<List<KIndexData>>(response);

            if (data != null && data.Count > 0)
            {
                var latest = data.OrderByDescending(d => d.TimeTag).First();
                return (int)Math.Round(latest.KpIndex);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fetch K-index");
        }

        return 2;
    }

    private async Task<SpaceWeatherData> FetchFromHamQSL()
    {
        try
        {
            var response = await _httpClient.GetStringAsync(HamQslUrl);

            var doc = System.Xml.Linq.XDocument.Parse(response);
            var solar = doc.Root?.Element("solardata") ?? doc.Root;

            if (solar != null)
            {
                var sfi = int.Parse(solar.Element("solarflux")?.Value?.Trim() ?? "70");
                var kIndex = int.Parse(solar.Element("kindex")?.Value?.Trim() ?? "2");
                var ssn = int.Parse(solar.Element("sunspots")?.Value?.Trim() ?? "0");

                // Try to parse the updated timestamp if available, otherwise use current time
                DateTime timestamp = DateTime.UtcNow;
                var updatedField = solar.Element("updated")?.Value?.Trim();
                if (!string.IsNullOrEmpty(updatedField) && DateTime.TryParse(updatedField, out var parsedTime))
                {
                    timestamp = parsedTime;
                }

                return new SpaceWeatherData(sfi, kIndex, ssn, timestamp);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to fetch from HamQSL fallback");
        }

        return new SpaceWeatherData(70, 2, 0, DateTime.UtcNow);
    }
}

internal class NoaaDailyFlux
{
    [JsonPropertyName("time_tag")]
    public string TimeTag { get; set; } = string.Empty;

    [JsonPropertyName("frequency")]
    public int Frequency { get; set; }

    [JsonPropertyName("flux")]
    public double Flux { get; set; }
}

internal class NoaaDailySunspots
{
    [JsonPropertyName("Obsdate")]
    public string ObsDate { get; set; } = string.Empty;

    [JsonPropertyName("swpc_ssn")]
    public int Ssn { get; set; }
}

internal class KIndexData
{
    [JsonPropertyName("time_tag")]
    public string TimeTag { get; set; } = string.Empty;

    [JsonPropertyName("kp_index")]
    public double KpIndex { get; set; }
}
