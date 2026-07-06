using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class PotaController : ControllerBase
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<PotaController> _logger;
    private readonly IMemoryCache _cache;
    private readonly ISettingsService _settingsService;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public PotaController(IHttpClientFactory httpClientFactory, ILogger<PotaController> logger, IMemoryCache cache, ISettingsService settingsService)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
        _cache = cache;
        _settingsService = settingsService;
    }

    public record SelfSpotRequest(string Callsign, string Reference, double FrequencyKhz, string Mode, string? Comment = null);

    /// <summary>
    /// Submit a self-spot to POTA.app so the operator's activation shows
    /// up on the POTA spot page. Ports v1 SDRLogger+'s /api/pota_spot
    /// route (main.py:3708). Requires POTA username + password to be set
    /// in Settings — POTA's /spot endpoint uses HTTP basic auth and
    /// rejects unauthenticated writes.
    /// </summary>
    [HttpPost("spot")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public async Task<ActionResult> SelfSpot([FromBody] SelfSpotRequest request, CancellationToken ct)
    {
        var settings = await _settingsService.GetSettingsAsync();
        var pota = settings.Pota;
        if (string.IsNullOrWhiteSpace(pota.Username) || string.IsNullOrWhiteSpace(pota.Password))
            return Ok(new { success = false, message = "Set your POTA username + password in Settings → POTA first" });

        if (string.IsNullOrWhiteSpace(request.Callsign))
            return Ok(new { success = false, message = "Activator callsign is required" });
        if (string.IsNullOrWhiteSpace(request.Reference))
            return Ok(new { success = false, message = "Park reference is required (e.g. K-1234)" });
        if (request.FrequencyKhz <= 0)
            return Ok(new { success = false, message = "Frequency is required" });

        try
        {
            var http = _httpClientFactory.CreateClient();
            http.Timeout = TimeSpan.FromSeconds(10);
            var authBytes = Encoding.UTF8.GetBytes($"{pota.Username}:{pota.Password}");
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(authBytes));

            // POTA's self-spot payload matches v1's exactly. `spotter == activator`
            // is what marks it as a self-spot (POTA otherwise treats spots as
            // third-party observations).
            var payload = new
            {
                activator = request.Callsign.Trim().ToUpperInvariant(),
                spotter = request.Callsign.Trim().ToUpperInvariant(),
                frequency = request.FrequencyKhz.ToString(System.Globalization.CultureInfo.InvariantCulture),
                mode = string.IsNullOrWhiteSpace(request.Mode) ? "SSB" : request.Mode.Trim().ToUpperInvariant(),
                reference = request.Reference.Trim().ToUpperInvariant(),
                source = "SDRLoggerPlus",
                comments = request.Comment ?? string.Empty,
            };
            var body = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
            var resp = await http.PostAsync("https://api.pota.app/spot", body, ct);

            if (resp.StatusCode == System.Net.HttpStatusCode.OK || resp.StatusCode == System.Net.HttpStatusCode.Created)
            {
                _logger.LogInformation("POTA self-spot ok: {Call} @ {Ref} on {Freq} kHz", payload.activator, payload.reference, payload.frequency);
                return Ok(new { success = true, message = $"Spotted {payload.activator} at {payload.reference} on {payload.frequency} kHz" });
            }
            if (resp.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                return Ok(new { success = false, message = "POTA rejected the credentials — check username/password in Settings" });

            var errBody = await resp.Content.ReadAsStringAsync(ct);
            return Ok(new { success = false, message = $"POTA API HTTP {(int)resp.StatusCode}: {(errBody.Length > 200 ? errBody[..200] : errBody)}" });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "POTA self-spot failed");
            return Ok(new { success = false, message = $"POTA self-spot failed: {ex.Message}" });
        }
    }

    /// <summary>
    /// Get active POTA spots from POTA API (/spot/activator endpoint), enriched with park coordinates
    /// </summary>
    [HttpGet("spots")]
    [ProducesResponseType(typeof(IEnumerable<PotaSpot>), StatusCodes.Status200OK)]
    public async Task<ActionResult<IEnumerable<PotaSpot>>> GetSpots()
    {
        try
        {
            var httpClient = _httpClientFactory.CreateClient();
            httpClient.Timeout = TimeSpan.FromSeconds(10);

            var response = await httpClient.GetAsync("https://api.pota.app/spot/activator");
            response.EnsureSuccessStatusCode();

            var content = await response.Content.ReadAsStringAsync();
            var spots = JsonSerializer.Deserialize<List<PotaSpot>>(content, JsonOptions)
                ?? new List<PotaSpot>();

            // Enrich spots with park coordinates from cache or park API
            var uniqueRefs = spots
                .Where(s => !string.IsNullOrEmpty(s.Reference) && s.Latitude is null)
                .Select(s => s.Reference)
                .Distinct()
                .ToList();

            var parkCoords = await GetParkCoordinates(httpClient, uniqueRefs);

            foreach (var spot in spots)
            {
                if (spot.Latitude is null && parkCoords.TryGetValue(spot.Reference, out var coords))
                {
                    spot.Latitude = coords.Lat;
                    spot.Longitude = coords.Lon;
                }

                // POTA API returns UTC timestamps without 'Z' suffix, violating ISO 8601
                // Ensure timestamps are properly interpreted as UTC
                if (!string.IsNullOrEmpty(spot.SpotTime) &&
                    !spot.SpotTime.EndsWith("Z", StringComparison.OrdinalIgnoreCase))
                {
                    spot.SpotTime += "Z";
                }
            }

            return Ok(spots);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to fetch POTA spots");
            return Ok(new List<PotaSpot>());
        }
    }

    private async Task<Dictionary<string, (double Lat, double Lon)>> GetParkCoordinates(
        HttpClient httpClient, List<string> references)
    {
        var result = new Dictionary<string, (double Lat, double Lon)>();
        var toFetch = new List<string>();

        // Check cache first
        foreach (var reference in references)
        {
            var cacheKey = $"pota_park_{reference}";
            if (_cache.TryGetValue(cacheKey, out (double Lat, double Lon) cached))
            {
                result[reference] = cached;
            }
            else
            {
                toFetch.Add(reference);
            }
        }

        // Fetch missing park data in parallel (limited concurrency)
        if (toFetch.Count > 0)
        {
            using var semaphore = new SemaphoreSlim(10);
            var tasks = toFetch.Select(async reference =>
            {
                await semaphore.WaitAsync();
                try
                {
                    var parkResponse = await httpClient.GetAsync($"https://api.pota.app/park/{reference}");
                    if (parkResponse.IsSuccessStatusCode)
                    {
                        var parkJson = await parkResponse.Content.ReadAsStringAsync();
                        var park = JsonSerializer.Deserialize<PotaPark>(parkJson, JsonOptions);
                        if (park?.Latitude is not null && park.Longitude is not null)
                        {
                            var coords = (park.Latitude.Value, park.Longitude.Value);
                            _cache.Set($"pota_park_{reference}", coords, TimeSpan.FromHours(24));
                            return (reference, coords: ((double Lat, double Lon)?)coords);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Failed to fetch park {Reference}", reference);
                }
                finally
                {
                    semaphore.Release();
                }
                return (reference, coords: ((double Lat, double Lon)?)null);
            });

            foreach (var (reference, coords) in await Task.WhenAll(tasks))
            {
                if (coords.HasValue)
                    result[reference] = coords.Value;
            }
        }

        return result;
    }
}

public class PotaSpot
{
    public int SpotId { get; set; }
    public string Activator { get; set; } = string.Empty;
    public string Frequency { get; set; } = string.Empty;
    public string Mode { get; set; } = string.Empty;
    public string Reference { get; set; } = string.Empty;
    public string ParkName { get; set; } = string.Empty;
    public string SpotTime { get; set; } = string.Empty;
    public string Spotter { get; set; } = string.Empty;
    public string Comments { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public bool? Invalid { get; set; }
    public string? Name { get; set; }
    public string? LocationDesc { get; set; }
    public string? Grid4 { get; set; }
    public string? Grid6 { get; set; }
    public double? Latitude { get; set; }
    public double? Longitude { get; set; }
}

public class PotaPark
{
    public double? Latitude { get; set; }
    public double? Longitude { get; set; }
}
