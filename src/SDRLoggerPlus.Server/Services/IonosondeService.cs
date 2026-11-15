using System.Globalization;
using System.Text.Json;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// One live ionosonde measurement. FoF2 is the F2-layer critical frequency
/// (MHz) actually measured overhead at that station at <see cref="TimeUtc"/>.
/// </summary>
public record IonosondeReading(string Name, double Lat, double Lon, double FoF2, double? MufD, DateTime TimeUtc);

/// <summary>Live ionosonde readings for the client (the DX Coach calibrates with them).</summary>
public record IonosondeSnapshot(IReadOnlyList<IonosondeReading> Readings, string Attribution, DateTime FetchedUtc);

public interface IIonosondeService
{
    /// <summary>Fresh readings (measured within the last hour). Empty when the feed is unreachable.</summary>
    Task<IonosondeSnapshot> GetReadingsAsync(CancellationToken ct = default);
}

/// <summary>
/// Live ionosonde data from KC2G's public feed (prop.kc2g.com), which collects the
/// international GIRO ionosonde network. Free, no key. Polled at most every 15
/// minutes (stations report every 5–15). Any failure yields an empty list, and the
/// propagation model then simply runs uncalibrated — the feed is an improvement,
/// never a dependency.
/// </summary>
public class IonosondeService : IIonosondeService
{
    private const string FeedUrl = "https://prop.kc2g.com/api/stations.json";
    public const string Attribution = "Ionosonde data: prop.kc2g.com (GIRO network)";
    private static readonly TimeSpan CacheTtl = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan FailureRetry = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan MaxReadingAge = TimeSpan.FromMinutes(60);

    private readonly HttpClient _http;
    private readonly ILogger<IonosondeService> _logger;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IonosondeSnapshot? _cached;
    private DateTime _nextFetchUtc = DateTime.MinValue;

    public IonosondeService(IHttpClientFactory httpClientFactory, ILogger<IonosondeService> logger)
    {
        _http = httpClientFactory.CreateClient("Ionosonde");
        _logger = logger;
    }

    public async Task<IonosondeSnapshot> GetReadingsAsync(CancellationToken ct = default)
    {
        if (_cached != null && DateTime.UtcNow < _nextFetchUtc) return Fresh(_cached);

        await _lock.WaitAsync(ct);
        try
        {
            if (_cached != null && DateTime.UtcNow < _nextFetchUtc) return Fresh(_cached);
            try
            {
                var json = await _http.GetStringAsync(FeedUrl, ct);
                var readings = Parse(json, DateTime.UtcNow);
                _cached = new IonosondeSnapshot(readings, Attribution, DateTime.UtcNow);
                _nextFetchUtc = DateTime.UtcNow + CacheTtl;
                _logger.LogInformation("Ionosondes: {Count} fresh reading(s)", readings.Count);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                _logger.LogWarning("Ionosonde feed unavailable ({Message}); propagation runs uncalibrated", ex.Message);
                _cached ??= new IonosondeSnapshot(Array.Empty<IonosondeReading>(), Attribution, DateTime.UtcNow);
                _nextFetchUtc = DateTime.UtcNow + FailureRetry;
            }
            return Fresh(_cached);
        }
        finally
        {
            _lock.Release();
        }
    }

    // A cached reading keeps ageing; drop any that passed the freshness limit since the fetch.
    private static IonosondeSnapshot Fresh(IonosondeSnapshot s)
    {
        var cutoff = DateTime.UtcNow - MaxReadingAge;
        return s.Readings.All(r => r.TimeUtc >= cutoff) ? s : s with { Readings = s.Readings.Where(r => r.TimeUtc >= cutoff).ToList() };
    }

    /// <summary>
    /// Parse the KC2G stations feed. Keeps readings measured within the last hour
    /// with a plausible foF2. Longitudes arrive as 0–360 east and are normalised
    /// to −180…180. Numbers may be JSON numbers or strings, and any may be null.
    /// </summary>
    internal static List<IonosondeReading> Parse(string json, DateTime nowUtc)
    {
        var list = new List<IonosondeReading>();
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind != JsonValueKind.Array) return list;

        foreach (var el in doc.RootElement.EnumerateArray())
        {
            if (Num(el, "fof2") is not { } fof2 || fof2 < 0.5 || fof2 > 20) continue;
            if (!el.TryGetProperty("time", out var t) || t.ValueKind != JsonValueKind.String ||
                !DateTime.TryParse(t.GetString(), CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var time))
                continue;
            if (nowUtc - time > MaxReadingAge || time - nowUtc > TimeSpan.FromMinutes(10)) continue;
            if (!el.TryGetProperty("station", out var st) || st.ValueKind != JsonValueKind.Object) continue;
            if (Num(st, "latitude") is not { } lat || Num(st, "longitude") is not { } lon) continue;
            if (lon > 180) lon -= 360;

            var name = st.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String ? n.GetString()! : "?";
            list.Add(new IonosondeReading(name, lat, lon, fof2, Num(el, "mufd"), time));
        }
        return list;
    }

    private static double? Num(JsonElement el, string name)
    {
        if (!el.TryGetProperty(name, out var p)) return null;
        if (p.ValueKind == JsonValueKind.Number && p.TryGetDouble(out var d)) return d;
        if (p.ValueKind == JsonValueKind.String &&
            double.TryParse(p.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var s)) return s;
        return null;
    }
}

/// <summary>Where a path forecast's level came from.</summary>
public enum ForecastSource
{
    /// <summary>Ionosondes report near every control point of the path.</summary>
    Measured,
    /// <summary>Corrected by live ionosondes, but none close to this path.</summary>
    Calibrated,
    /// <summary>No live data — solar-flux model only.</summary>
    Model,
}

/// <summary>Live correction per path control point (aligned with PropagationService.ControlPoints).</summary>
public sealed record PathCalibration(double[] Factors, ForecastSource Source);

/// <summary>
/// Calibrates the solar-flux foF2 model with live ionosonde measurements.
///
/// At each station: factor = measured foF2 / model foF2 (same place, same moment).
/// The factor is interpolated to each control point of a path — nearby stations
/// weighted heavily, blended with the worldwide median so a path far from any
/// ionosonde still gets the global correction. The model keeps the geometry
/// (day/night, latitude, hop length); the measurements fix its level, which at
/// moderate solar flux it under-reads ~2×.
///
/// This is a faithful twin of the TypeScript in Web/src/utils/propagationGate.ts.
/// </summary>
public static class IonosondeCalibration
{
    /// <summary>Within this distance an ionosonde counts as "near" a control point.</summary>
    public const double NearKm = 2000;
    private const double WeightScaleKm = 1500;
    private const double MaxInfluenceKm = 4000;
    private const double GlobalWeight = 0.2;
    private const double MinFactor = 0.5, MaxFactor = 3.0;

    public static PathCalibration ForPath(
        IReadOnlyList<IonosondeReading> readings, int ssn,
        double deLat, double deLon, double dxLat, double dxLon, DateTime utc)
    {
        var points = PropagationService.ControlPoints(deLat, deLon, dxLat, dxLon);
        if (readings.Count == 0)
            return new PathCalibration(points.Select(_ => 1.0).ToArray(), ForecastSource.Model);

        var factors = readings
            .Select(r => (r, k: Math.Clamp(r.FoF2 / PropagationService.EstimateFoF2(ssn, r.Lat, r.Lon, utc), MinFactor, MaxFactor)))
            .ToList();
        var global = Median(factors.Select(f => f.k));

        var allNear = true;
        var result = new double[points.Count];
        for (int i = 0; i < points.Count; i++)
        {
            var (lat, lon) = points[i];
            double wSum = GlobalWeight, kSum = GlobalWeight * global, nearest = double.MaxValue;
            foreach (var (r, k) in factors)
            {
                var d = PropagationService.HaversineDistanceKm(lat, lon, r.Lat, r.Lon);
                nearest = Math.Min(nearest, d);
                if (d > MaxInfluenceKm) continue;
                var w = Math.Exp(-Math.Pow(d / WeightScaleKm, 2));
                wSum += w;
                kSum += w * k;
            }
            if (nearest > NearKm) allNear = false;
            result[i] = kSum / wSum;
        }

        return new PathCalibration(result, allNear ? ForecastSource.Measured : ForecastSource.Calibrated);
    }

    private static double Median(IEnumerable<double> values)
    {
        var v = values.OrderBy(x => x).ToArray();
        return v.Length % 2 == 1 ? v[v.Length / 2] : (v[v.Length / 2 - 1] + v[v.Length / 2]) / 2;
    }
}
