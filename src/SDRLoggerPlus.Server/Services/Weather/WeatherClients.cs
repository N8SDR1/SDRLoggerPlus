using System.Globalization;
using System.Text.Json;
using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services.Weather;

public record WindReading(double? SustainedMph, double? GustMph, double? DirectionDeg);
public record LightningReading(double? DistanceKm, int StrikeCount);
public record NwsWindAlert(string Headline, bool IsExtreme);
public record StrikeInfo(double DistanceKm, double BearingDeg);

public record EcowittCredentials(string AppKey, string ApiKey, string Mac);
public record AmbientCredentials(string ApiKey, string AppKey);

public interface INwsClient
{
    Task<NwsWindAlert?> GetWindAlertAsync(double lat, double lon, CancellationToken ct = default);
    Task<string?> GetThunderstormWarningAsync(double lat, double lon, CancellationToken ct = default);
    Task<WindReading?> GetMetarWindAsync(string station, CancellationToken ct = default);
}

public interface IAmbientWeatherClient
{
    Task<LightningReading?> GetLightningAsync(AmbientCredentials creds, CancellationToken ct = default);
    Task<WindReading?> GetWindAsync(AmbientCredentials creds, CancellationToken ct = default);
}

public interface IEcowittClient
{
    Task<LightningReading?> GetLightningAsync(EcowittCredentials creds, CancellationToken ct = default);
    Task<WindReading?> GetWindAsync(EcowittCredentials creds, CancellationToken ct = default);
}

public interface IBlitzortungClient
{
    Task<List<StrikeInfo>> GetStrikesAsync(double lat, double lon, double rangeKm, CancellationToken ct = default);
    Task<List<LightningStrike>> GetStrikesRawAsync(IEnumerable<int> slices, CancellationToken ct = default);
}

/// <summary>
/// api.weather.gov client. NWS requires a descriptive User-Agent.
/// Wind products filtered: High Wind Warning/Watch, Wind Advisory,
/// Extreme Wind Warning; "extreme" = High Wind Warning or Extreme Wind.
/// METAR speeds normalized to mph from km/h or m/s unit codes.
/// </summary>
public class NwsClient : INwsClient
{
    private static readonly string[] WindProducts =
        ["high wind warning", "high wind watch", "wind advisory", "extreme wind warning"];

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<NwsClient> _logger;

    public NwsClient(IHttpClientFactory httpClientFactory, ILogger<NwsClient> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    private HttpClient CreateClient()
    {
        var client = _httpClientFactory.CreateClient();
        client.Timeout = TimeSpan.FromSeconds(10);
        client.DefaultRequestHeaders.Add("User-Agent", "SDRLoggerPlus Ham Radio Logger (https://github.com/sdrloggerplus)");
        return client;
    }

    public async Task<NwsWindAlert?> GetWindAlertAsync(double lat, double lon, CancellationToken ct = default)
    {
        try
        {
            using var client = CreateClient();
            var json = await client.GetStringAsync(
                $"https://api.weather.gov/alerts/active?point={lat:F4},{lon:F4}", ct);
            using var doc = JsonDocument.Parse(json);
            foreach (var feat in doc.RootElement.GetProperty("features").EnumerateArray())
            {
                var ev = feat.GetProperty("properties").TryGetProperty("event", out var e)
                    ? e.GetString() ?? "" : "";
                var lower = ev.ToLowerInvariant();
                if (WindProducts.Any(p => lower.Contains(p)))
                {
                    var isExtreme = lower.Contains("high wind warning") || lower.Contains("extreme wind");
                    return new NwsWindAlert(ev, isExtreme);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("NWS wind alerts error: {Error}", ex.Message);
        }
        return null;
    }

    public async Task<string?> GetThunderstormWarningAsync(double lat, double lon, CancellationToken ct = default)
    {
        try
        {
            using var client = CreateClient();
            var json = await client.GetStringAsync(
                $"https://api.weather.gov/alerts/active?point={lat:F4},{lon:F4}", ct);
            using var doc = JsonDocument.Parse(json);
            foreach (var feat in doc.RootElement.GetProperty("features").EnumerateArray())
            {
                var ev = feat.GetProperty("properties").TryGetProperty("event", out var e)
                    ? e.GetString() ?? "" : "";
                var lower = ev.ToLowerInvariant();
                if (lower.Contains("thunderstorm") || lower.Contains("lightning"))
                    return ev;
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("NWS thunderstorm warnings error: {Error}", ex.Message);
        }
        return null;
    }

    public async Task<WindReading?> GetMetarWindAsync(string station, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(station) || station.Length < 3) return null;
        try
        {
            using var client = CreateClient();
            var json = await client.GetStringAsync(
                $"https://api.weather.gov/stations/{station.Trim().ToUpperInvariant()}/observations/latest", ct);
            using var doc = JsonDocument.Parse(json);
            var props = doc.RootElement.GetProperty("properties");

            double? Mph(string field)
            {
                if (!props.TryGetProperty(field, out var f)) return null;
                if (!f.TryGetProperty("value", out var v) || v.ValueKind == JsonValueKind.Null) return null;
                var value = v.GetDouble();
                var unitCode = f.TryGetProperty("unitCode", out var uc) ? uc.GetString() ?? "" : "";
                return NormalizeToMph(value, unitCode);
            }

            double? dir = null;
            if (props.TryGetProperty("windDirection", out var wd) &&
                wd.TryGetProperty("value", out var dv) && dv.ValueKind != JsonValueKind.Null)
                dir = dv.GetDouble();

            return new WindReading(Mph("windSpeed"), Mph("windGust"), dir);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("NWS METAR {Station} error: {Error}", station, ex.Message);
            return null;
        }
    }

    /// <summary>NWS reports km/h or m/s depending on field; assume mph as last resort.</summary>
    public static double NormalizeToMph(double value, string unitCode)
    {
        if (unitCode.Contains("km_h") || unitCode.Contains("kmh")) return value * 0.621371;
        if (unitCode.Contains("m_s")) return value * 2.23694;
        return value;
    }
}

/// <summary>rt.ambientweather.net — first device's lastData, fields in mph/miles.</summary>
public class AmbientWeatherClient : IAmbientWeatherClient
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<AmbientWeatherClient> _logger;

    public AmbientWeatherClient(IHttpClientFactory httpClientFactory, ILogger<AmbientWeatherClient> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    private async Task<JsonElement?> GetLastDataAsync(AmbientCredentials creds, CancellationToken ct)
    {
        try
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(10);
            var json = await client.GetStringAsync(
                $"https://rt.ambientweather.net/v1/devices?apiKey={creds.ApiKey}&applicationKey={creds.AppKey}", ct);
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array || doc.RootElement.GetArrayLength() == 0)
                return null;
            return doc.RootElement[0].GetProperty("lastData").Clone();
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Ambient Weather error: {Error}", ex.Message);
            return null;
        }
    }

    public async Task<LightningReading?> GetLightningAsync(AmbientCredentials creds, CancellationToken ct = default)
    {
        var last = await GetLastDataAsync(creds, ct);
        if (last == null) return null;
        var distMi = GetDouble(last.Value, "lightning_distance");
        var hour = (int)(GetDouble(last.Value, "lightning_hour") ?? 0);
        if (distMi == null || hour <= 0) return new LightningReading(null, 0);
        return new LightningReading(distMi * 1.60934, hour);
    }

    public async Task<WindReading?> GetWindAsync(AmbientCredentials creds, CancellationToken ct = default)
    {
        var last = await GetLastDataAsync(creds, ct);
        if (last == null) return null;
        return new WindReading(
            GetDouble(last.Value, "windspeedmph"),
            GetDouble(last.Value, "windgustmph"),
            GetDouble(last.Value, "winddir"));
    }

    private static double? GetDouble(JsonElement el, string name) =>
        el.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetDouble() : null;
}

/// <summary>
/// Ecowitt v3 cloud API. One real_time call returns every sensor, so a
/// 30-second cache serves both the lightning and wind pollers from a single
/// HTTP hit — staying under the 1-call/min/MAC free-tier limit.
/// </summary>
public class EcowittClient : IEcowittClient
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<EcowittClient> _logger;
    private readonly TimeSpan _cacheTtl;

    private JsonElement? _cached;
    private DateTime _cachedAtUtc = DateTime.MinValue;
    private readonly SemaphoreSlim _cacheLock = new(1, 1);

    public EcowittClient(IHttpClientFactory httpClientFactory, ILogger<EcowittClient> logger, TimeSpan? cacheTtl = null)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
        _cacheTtl = cacheTtl ?? TimeSpan.FromSeconds(30);
    }

    internal async Task<JsonElement?> GetLastDataAsync(EcowittCredentials creds, CancellationToken ct)
    {
        await _cacheLock.WaitAsync(ct);
        try
        {
            if (_cached != null && DateTime.UtcNow - _cachedAtUtc < _cacheTtl)
                return _cached;

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(12);
            var url = "https://api.ecowitt.net/api/v3/device/real_time" +
                $"?application_key={creds.AppKey}&api_key={creds.ApiKey}&mac={creds.Mac}" +
                "&call_back=all&temp_unitid=2&pressure_unitid=4&wind_unitid=9&rainfall_unitid=13";
            var json = await client.GetStringAsync(url, ct);
            using var doc = JsonDocument.Parse(json);
            if ((doc.RootElement.TryGetProperty("code", out var code) ? code.ToString() : "") != "0")
            {
                _logger.LogDebug("Ecowitt API code {Code}", code.ToString());
                return null;
            }
            _cached = doc.RootElement.GetProperty("data").Clone();
            _cachedAtUtc = DateTime.UtcNow;
            return _cached;
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Ecowitt error: {Error}", ex.Message);
            return null;
        }
        finally
        {
            _cacheLock.Release();
        }
    }

    public async Task<LightningReading?> GetLightningAsync(EcowittCredentials creds, CancellationToken ct = default)
    {
        var data = await GetLastDataAsync(creds, ct);
        if (data == null) return null;
        // Ecowitt v3 reports lightning distance in km on most firmwares
        var distKm = Leaf(data.Value, "lightning", "distance");
        var hour = (int)(Leaf(data.Value, "lightning", "count") ?? 0);
        if (distKm == null || hour <= 0) return new LightningReading(null, 0);
        return new LightningReading(distKm, hour);
    }

    public async Task<WindReading?> GetWindAsync(EcowittCredentials creds, CancellationToken ct = default)
    {
        var data = await GetLastDataAsync(creds, ct);
        if (data == null) return null;
        return new WindReading(
            Leaf(data.Value, "wind", "wind_speed"),
            Leaf(data.Value, "wind", "wind_gust"),
            Leaf(data.Value, "wind", "wind_direction"));
    }

    /// <summary>Ecowitt nests values as {time, unit, value} — walk to the leaf and pull value.</summary>
    private static double? Leaf(JsonElement el, params string[] path)
    {
        var cur = el;
        foreach (var p in path)
        {
            if (cur.ValueKind != JsonValueKind.Object || !cur.TryGetProperty(p, out cur))
                return null;
        }
        if (cur.ValueKind == JsonValueKind.Object && cur.TryGetProperty("value", out var v))
            cur = v;
        if (cur.ValueKind == JsonValueKind.Number) return cur.GetDouble();
        if (cur.ValueKind == JsonValueKind.String && double.TryParse(cur.GetString(), out var d)) return d;
        return null;
    }
}

/// <summary>
/// Blitzortung.org public strike feed. The GEOjson `n` parameter selects a
/// worldwide 5-minute time slice (0 = newest) — NOT a geographic region, despite
/// the historical naming here. The alert path still fetches slices 7/12/13 as
/// SDRLogger+ always did; strikes are filtered to the configured range.
/// </summary>
public class BlitzortungClient : IBlitzortungClient
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<BlitzortungClient> _logger;
    private static readonly int[] Regions = [7, 12, 13];

    public BlitzortungClient(IHttpClientFactory httpClientFactory, ILogger<BlitzortungClient> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task<List<StrikeInfo>> GetStrikesAsync(double lat, double lon, double rangeKm, CancellationToken ct = default)
    {
        var strikes = new List<StrikeInfo>();
        foreach (var region in Regions)
        {
            try
            {
                var client = _httpClientFactory.CreateClient();
                client.Timeout = TimeSpan.FromSeconds(8);
                client.DefaultRequestHeaders.Add("Referer", "https://map.blitzortung.org/");
                client.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SDRLoggerPlus");
                var json = await client.GetStringAsync(
                    $"https://map.blitzortung.org/GEOjson/getjson.php?f=s&n={region:D2}", ct);
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) continue;
                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    // Flat arrays: [lon, lat, timestamp, ...]
                    if (item.ValueKind != JsonValueKind.Array || item.GetArrayLength() < 2) continue;
                    if (item[0].ValueKind != JsonValueKind.Number || item[1].ValueKind != JsonValueKind.Number) continue;
                    var sLon = item[0].GetDouble();
                    var sLat = item[1].GetDouble();
                    var dist = PropagationService.HaversineDistanceKm(lat, lon, sLat, sLon);
                    if (dist <= rangeKm)
                        strikes.Add(new StrikeInfo(dist, GeoMath.BearingDeg(lat, lon, sLat, sLon)));
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Blitzortung region {Region} error: {Error}", region, ex.Message);
            }
        }
        return strikes;
    }

    public async Task<List<LightningStrike>> GetStrikesRawAsync(IEnumerable<int> slices, CancellationToken ct = default)
    {
        var strikes = new List<LightningStrike>();
        foreach (var slice in slices)
        {
            try
            {
                var client = _httpClientFactory.CreateClient();
                client.Timeout = TimeSpan.FromSeconds(8);
                client.DefaultRequestHeaders.Add("Referer", "https://map.blitzortung.org/");
                client.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SDRLoggerPlus");
                var json = await client.GetStringAsync(
                    $"https://map.blitzortung.org/GEOjson/getjson.php?f=s&n={slice:D2}", ct);
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) continue;
                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    // Flat arrays: [lon, lat, timestamp, ...]. The live feed sends the
                    // timestamp as a "yyyy-MM-dd HH:mm:ss.fffffffff" UTC string; the
                    // ns-since-epoch number form is accepted for compatibility.
                    if (item.ValueKind != JsonValueKind.Array || item.GetArrayLength() < 3) continue;
                    if (item[0].ValueKind != JsonValueKind.Number || item[1].ValueKind != JsonValueKind.Number) continue;
                    var ts = ParseStrikeTimestamp(item[2]);
                    if (ts is null) continue;
                    var lon = item[0].GetDouble();
                    var lat = item[1].GetDouble();
                    strikes.Add(new LightningStrike(lat, lon, ts.Value, Local: false));
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Blitzortung raw slice {Slice} error: {Error}", slice, ex.Message);
            }
        }
        return strikes;
    }

    /// <summary>
    /// Feed timestamps are UTC "yyyy-MM-dd HH:mm:ss.fffffffff" strings (9-digit ns
    /// fraction — beyond DateTime's 7-digit tick precision, so the tail is trimmed)
    /// or, historically, ns-since-epoch numbers. Null for anything unparseable.
    /// </summary>
    private static DateTime? ParseStrikeTimestamp(JsonElement el)
    {
        if (el.ValueKind == JsonValueKind.Number && el.TryGetInt64(out var ns))
            return DateTimeOffset.FromUnixTimeMilliseconds(ns / 1_000_000).UtcDateTime;
        if (el.ValueKind != JsonValueKind.String) return null;
        var s = el.GetString();
        if (string.IsNullOrEmpty(s)) return null;
        var dot = s.IndexOf('.');
        if (dot >= 0 && s.Length > dot + 8) s = s[..(dot + 8)];
        return DateTime.TryParse(s, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts)
            ? ts : null;
    }
}
