using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using System.Text.Json;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Proxies NOAA SWPC's OVATION aurora model (short-term auroral oval forecast) for the
/// map overlay. The raw grid is ~65k points; we drop near-zero cells server-side and cache
/// for 5 minutes (SWPC refreshes the product roughly every few minutes).
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class AuroraController : ControllerBase
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<AuroraController> _logger;
    private readonly IMemoryCache _cache;

    private const string OvationUrl = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json";
    private const string CacheKey = "aurora_ovation";
    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(5);
    // Aurora probability (%) below this is visual noise — drop it to shrink the payload.
    private const int MinProbability = 3;

    public AuroraController(IHttpClientFactory httpClientFactory, ILogger<AuroraController> logger, IMemoryCache cache)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
        _cache = cache;
    }

    /// <summary>Get the latest OVATION aurora oval as a sparse list of significant cells.</summary>
    [HttpGet("ovation")]
    [ProducesResponseType(typeof(AuroraForecast), StatusCodes.Status200OK)]
    public async Task<ActionResult<AuroraForecast>> GetOvation()
    {
        if (_cache.TryGetValue(CacheKey, out AuroraForecast? cached) && cached is not null)
            return Ok(cached);

        try
        {
            var httpClient = _httpClientFactory.CreateClient();
            httpClient.Timeout = TimeSpan.FromSeconds(15);

            var response = await httpClient.GetAsync(OvationUrl);
            response.EnsureSuccessStatusCode();
            var content = await response.Content.ReadAsStringAsync();

            var forecast = ParseOvation(content);
            _cache.Set(CacheKey, forecast, CacheDuration);
            return Ok(forecast);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to fetch OVATION aurora data");
            var empty = new AuroraForecast();
            _cache.Set(CacheKey, empty, TimeSpan.FromMinutes(1));
            return Ok(empty);
        }
    }

    internal static AuroraForecast ParseOvation(string json)
    {
        var forecast = new AuroraForecast();
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        if (root.TryGetProperty("Observation Time", out var obs))
            forecast.ObservationTime = obs.GetString() ?? string.Empty;
        if (root.TryGetProperty("Forecast Time", out var fc))
            forecast.ForecastTime = fc.GetString() ?? string.Empty;

        if (root.TryGetProperty("coordinates", out var coords) && coords.ValueKind == JsonValueKind.Array)
        {
            foreach (var cell in coords.EnumerateArray())
            {
                if (cell.ValueKind != JsonValueKind.Array || cell.GetArrayLength() < 3) continue;
                var lon = cell[0].GetDouble();
                var lat = cell[1].GetDouble();
                var prob = cell[2].GetInt32();
                if (prob < MinProbability) continue;

                // OVATION longitudes are 0..359; normalize to -180..180 for Leaflet.
                if (lon > 180) lon -= 360;

                forecast.Points.Add(new AuroraPoint { Lat = lat, Lon = lon, Aurora = prob });
            }
        }

        return forecast;
    }
}

public class AuroraForecast
{
    public string ObservationTime { get; set; } = string.Empty;
    public string ForecastTime { get; set; } = string.Empty;
    public List<AuroraPoint> Points { get; set; } = new();
}

public class AuroraPoint
{
    public double Lat { get; set; }
    public double Lon { get; set; }
    /// <summary>Aurora probability percentage (0-100).</summary>
    public int Aurora { get; set; }
}
