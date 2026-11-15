using System.Collections.Concurrent;
using System.Xml.Linq;

namespace SDRLoggerPlus.Server.Services;

#region DTOs

public record BandPrediction(
    string Band,
    double FreqMHz,
    int Reliability,
    string Status
);

public record PropagationPrediction(
    double DeLat,
    double DeLon,
    double DxLat,
    double DxLon,
    double DistanceKm,
    double BearingDeg,
    double MufMHz,
    double LufMHz,
    int Sfi,
    int KIndex,
    BandPrediction[] CurrentBands,
    int[][] HeatmapData,
    string[] BandNames,
    DateTime Timestamp,
    // How the model's level was set: "measured" (live ionosondes near the path),
    // "calibrated" (live ionosondes elsewhere), or "model" (solar flux only).
    string ForecastSource = "model",
    // Live correction applied to the model's MUF (1.0 = none).
    double Calibration = 1.0,
    string? Attribution = null
);

public record GenericBandConditions(
    BandConditionEntry[] Bands,
    int Sfi,
    int KIndex,
    int Ssn,
    string Source,
    DateTime Timestamp
);

public record BandConditionEntry(
    string Band,
    string DayStatus,
    string NightStatus
);

#endregion

public interface IPropagationService
{
    Task<PropagationPrediction> PredictAsync(double deLat, double deLon, double dxLat, double dxLon);
    Task<GenericBandConditions> GetGenericConditionsAsync();
}

public class PropagationService : IPropagationService
{
    private readonly ISpaceWeatherService _spaceWeatherService;
    private readonly IIonosondeService _ionosondeService;
    private readonly HttpClient _httpClient;
    private readonly ILogger<PropagationService> _logger;

    // Path prediction cache: key = rounded DX coords, value = (prediction, timestamp)
    private readonly ConcurrentDictionary<string, (PropagationPrediction Prediction, DateTime FetchedAt)> _pathCache = new();
    private static readonly TimeSpan PathCacheTtl = TimeSpan.FromMinutes(5);

    // Generic conditions cache
    private GenericBandConditions? _cachedGeneric;
    private DateTime _lastGenericFetch = DateTime.MinValue;
    private static readonly TimeSpan GenericCacheTtl = TimeSpan.FromMinutes(30);

    // HF bands to evaluate (matching frontend HF_BANDS)
    internal static readonly (string Name, double FreqMHz)[] HfBands =
    {
        ("80m", 3.5), ("60m", 5.3), ("40m", 7.0), ("30m", 10.1),
        ("20m", 14.0), ("17m", 18.1), ("15m", 21.0), ("12m", 24.9), ("10m", 28.0)
    };

    public PropagationService(
        ISpaceWeatherService spaceWeatherService,
        IIonosondeService ionosondeService,
        IHttpClientFactory httpClientFactory,
        ILogger<PropagationService> logger)
    {
        _spaceWeatherService = spaceWeatherService;
        _ionosondeService = ionosondeService;
        _httpClient = httpClientFactory.CreateClient();
        _httpClient.Timeout = TimeSpan.FromSeconds(10);
        _logger = logger;
    }

    public async Task<PropagationPrediction> PredictAsync(double deLat, double deLon, double dxLat, double dxLon)
    {
        // Check cache
        var key = MakeCacheKey(deLat, deLon, dxLat, dxLon);
        if (_pathCache.TryGetValue(key, out var cached) && DateTime.UtcNow - cached.FetchedAt < PathCacheTtl)
            return cached.Prediction;

        var weather = await _spaceWeatherService.GetCurrentAsync();
        int sfi = weather.SolarFluxIndex;
        int kIndex = weather.KIndex;
        int ssn = SsnFromSfi(sfi);

        double distKm = HaversineDistanceKm(deLat, deLon, dxLat, dxLon);
        double bearing = BearingDeg(deLat, deLon, dxLat, dxLon);
        var (midLat, midLon) = MidpointLatLon(deLat, deLon, dxLat, dxLon);
        int hops = Math.Max(1, (int)Math.Ceiling(distKm / 3500.0));

        var now = DateTime.UtcNow;
        int currentHour = now.Hour;

        // Live ionosondes correct the model's level (it under-reads at moderate
        // flux). The correction is measured now and applied to the whole day's
        // curve; the model still supplies the day/night shape.
        var ionosondes = await _ionosondeService.GetReadingsAsync();
        var calibration = IonosondeCalibration.ForPath(
            ionosondes.Readings, ssn, deLat, deLon, dxLat, dxLon, now);

        // Compute 24-hour heatmap: [bandIdx][hourIdx]
        int[][] heatmap = new int[HfBands.Length][];
        for (int b = 0; b < HfBands.Length; b++)
        {
            heatmap[b] = new int[24];
            for (int h = 0; h < 24; h++)
            {
                var utcAtHour = now.Date.AddHours(h);
                double muf = CalculatePathMuf(ssn, deLat, deLon, dxLat, dxLon, utcAtHour, calibration.Factors);
                double luf = CalculateLuf(ssn, distKm, midLat, midLon, utcAtHour, kIndex);
                heatmap[b][h] = CalculateReliability(HfBands[b].FreqMHz, muf, luf, kIndex, hops);
            }
        }

        // Current-hour MUF/LUF
        double currentMuf = CalculatePathMuf(ssn, deLat, deLon, dxLat, dxLon, now, calibration.Factors);
        double currentLuf = CalculateLuf(ssn, distKm, midLat, midLon, now, kIndex);

        // Current-hour per-band predictions
        var currentBands = HfBands.Select((band, idx) =>
        {
            int rel = heatmap[idx][currentHour];
            string status = rel >= 80 ? "EXCELLENT" : rel >= 60 ? "GOOD" : rel >= 40 ? "FAIR" : rel >= 20 ? "POOR" : "CLOSED";
            return new BandPrediction(band.Name, band.FreqMHz, rel, status);
        }).ToArray();

        var prediction = new PropagationPrediction(
            deLat, deLon, dxLat, dxLon,
            Math.Round(distKm, 1),
            Math.Round(bearing, 1),
            Math.Round(currentMuf, 1),
            Math.Round(currentLuf, 1),
            sfi, kIndex,
            currentBands,
            heatmap,
            HfBands.Select(b => b.Name).ToArray(),
            DateTime.UtcNow,
            calibration.Source.ToString().ToLowerInvariant(),
            Math.Round(calibration.Factors.Min(), 2),
            calibration.Source == Services.ForecastSource.Model ? null : ionosondes.Attribution
        );

        // Update cache (evict stale entries if too many)
        if (_pathCache.Count > 100)
        {
            foreach (var k in _pathCache.Where(kv => DateTime.UtcNow - kv.Value.FetchedAt > PathCacheTtl).Select(kv => kv.Key).ToList())
                _pathCache.TryRemove(k, out _);
        }
        _pathCache[key] = (prediction, DateTime.UtcNow);

        return prediction;
    }

    public async Task<GenericBandConditions> GetGenericConditionsAsync()
    {
        if (_cachedGeneric != null && DateTime.UtcNow - _lastGenericFetch < GenericCacheTtl)
            return _cachedGeneric;

        try
        {
            var xml = await _httpClient.GetStringAsync("https://www.hamqsl.com/solarxml.php");
            var doc = XDocument.Parse(xml);
            var solar = doc.Root?.Element("solardata") ?? doc.Root;

            int sfi = int.Parse(solar?.Element("solarflux")?.Value?.Trim() ?? "70");
            int kIndex = int.Parse(solar?.Element("kindex")?.Value?.Trim() ?? "2");
            int ssn = int.Parse(solar?.Element("sunspots")?.Value?.Trim() ?? "0");

            var bands = new List<BandConditionEntry>();
            var conditions = solar?.Element("calculatedconditions");

            if (conditions != null)
            {
                var dayEntries = conditions.Elements("band")
                    .Where(b => b.Attribute("time")?.Value == "day")
                    .ToDictionary(
                        b => b.Attribute("name")?.Value ?? "",
                        b => NormalizeCondition(b.Value.Trim())
                    );
                var nightEntries = conditions.Elements("band")
                    .Where(b => b.Attribute("time")?.Value == "night")
                    .ToDictionary(
                        b => b.Attribute("name")?.Value ?? "",
                        b => NormalizeCondition(b.Value.Trim())
                    );

                // Map N0NBH band groups to individual bands
                var bandGroupMap = new Dictionary<string, string[]>
                {
                    ["80m-40m"] = ["80m", "60m", "40m"],
                    ["30m-20m"] = ["30m", "20m"],
                    ["17m-15m"] = ["17m", "15m"],
                    ["12m-10m"] = ["12m", "10m"],
                };

                foreach (var (group, individualBands) in bandGroupMap)
                {
                    string dayStatus = dayEntries.GetValueOrDefault(group, "Fair");
                    string nightStatus = nightEntries.GetValueOrDefault(group, "Fair");
                    foreach (var band in individualBands)
                        bands.Add(new BandConditionEntry(band, dayStatus, nightStatus));
                }
            }

            var result = new GenericBandConditions(bands.ToArray(), sfi, kIndex, ssn, "N0NBH", DateTime.UtcNow);
            _cachedGeneric = result;
            _lastGenericFetch = DateTime.UtcNow;
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fetch N0NBH conditions");
            if (_cachedGeneric != null) return _cachedGeneric;

            var weather = await _spaceWeatherService.GetCurrentAsync();
            return new GenericBandConditions(
                [],
                weather.SolarFluxIndex, weather.KIndex, weather.SunspotNumber,
                "Unavailable", DateTime.UtcNow
            );
        }
    }

    #region Propagation Math

    /// <summary>SSN derived from SFI (OpenHamClock formula)</summary>
    public static int SsnFromSfi(int sfi)
        => Math.Max(0, (int)Math.Round((sfi - 67.0) / 0.97));

    /// <summary>Great-circle distance in km using Haversine formula</summary>
    public static double HaversineDistanceKm(double lat1, double lon1, double lat2, double lon2)
    {
        const double R = 6371.0;
        double dLat = ToRad(lat2 - lat1);
        double dLon = ToRad(lon2 - lon1);
        double a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                   Math.Cos(ToRad(lat1)) * Math.Cos(ToRad(lat2)) *
                   Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        double c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        return R * c;
    }

    /// <summary>Initial bearing in degrees from point 1 to point 2</summary>
    public static double BearingDeg(double lat1, double lon1, double lat2, double lon2)
    {
        double dLon = ToRad(lon2 - lon1);
        double y = Math.Sin(dLon) * Math.Cos(ToRad(lat2));
        double x = Math.Cos(ToRad(lat1)) * Math.Sin(ToRad(lat2)) -
                   Math.Sin(ToRad(lat1)) * Math.Cos(ToRad(lat2)) * Math.Cos(dLon);
        return (ToDeg(Math.Atan2(y, x)) + 360) % 360;
    }

    /// <summary>Great-circle midpoint between two coordinates</summary>
    public static (double Lat, double Lon) MidpointLatLon(double lat1, double lon1, double lat2, double lon2)
    {
        double lat1r = ToRad(lat1), lon1r = ToRad(lon1);
        double lat2r = ToRad(lat2);
        double dLon = ToRad(lon2 - lon1);
        double bx = Math.Cos(lat2r) * Math.Cos(dLon);
        double by = Math.Cos(lat2r) * Math.Sin(dLon);
        double midLat = Math.Atan2(
            Math.Sin(lat1r) + Math.Sin(lat2r),
            Math.Sqrt((Math.Cos(lat1r) + bx) * (Math.Cos(lat1r) + bx) + by * by));
        double midLon = lon1r + Math.Atan2(by, Math.Cos(lat1r) + bx);
        return (ToDeg(midLat), ToDeg(midLon));
    }

    /// <summary>Simplified solar zenith angle at a given location and UTC time</summary>
    public static double SolarZenithAngle(double lat, double lon, DateTime utc)
    {
        int dayOfYear = utc.DayOfYear;
        double hourUtc = utc.Hour + utc.Minute / 60.0;

        // Solar declination (simplified)
        double declination = -23.44 * Math.Cos(ToRad(360.0 / 365.0 * (dayOfYear + 10)));

        // Hour angle
        double solarNoon = 12.0 - lon / 15.0;
        double hourAngle = 15.0 * (hourUtc - solarNoon);

        // Zenith angle
        double cosZenith = Math.Sin(ToRad(lat)) * Math.Sin(ToRad(declination)) +
                           Math.Cos(ToRad(lat)) * Math.Cos(ToRad(declination)) * Math.Cos(ToRad(hourAngle));
        return ToDeg(Math.Acos(Math.Clamp(cosZenith, -1.0, 1.0)));
    }

    /// <summary>
    /// Maximum Usable Frequency of a whole path, ITU-R P.533 style: the F2 critical
    /// frequency at each control point (model, times that point's live ionosonde
    /// correction) times the hop's M-factor; the weakest control point limits the
    /// path. <paramref name="factors"/> aligns with <see cref="ControlPoints"/>;
    /// null = uncorrected model.
    /// </summary>
    public static double CalculatePathMuf(
        int ssn, double deLat, double deLon, double dxLat, double dxLon, DateTime utc,
        IReadOnlyList<double>? factors = null)
    {
        double distanceKm = HaversineDistanceKm(deLat, deLon, dxLat, dxLon);
        int hops = Math.Max(1, (int)Math.Ceiling(distanceKm / 3500.0));
        double m = HopMFactor(distanceKm / hops);

        double foF2 = ControlPoints(deLat, deLon, dxLat, dxLon)
            .Select((p, i) => EstimateFoF2(ssn, p.Lat, p.Lon, utc) * (factors?[i] ?? 1.0))
            .Min();
        return Math.Max(2.0, foF2 * m);
    }

    /// <summary>
    /// MUF ÷ foF2 for one hop of the given length (the "M-factor"): low-angle long
    /// hops reflect frequencies ~3.5× the critical frequency, short hops far less.
    /// </summary>
    public static double HopMFactor(double hopKm)
        => 1.0 + 2.5 * Math.Sin(ToRad(Math.Min(90, hopKm / 3500.0 * 90)));

    /// <summary>
    /// Where the ionosphere sets a path's MUF: the midpoint for paths up to 4,000 km,
    /// else the points 2,000 km in from each end (ITU-R P.533).
    /// </summary>
    public static IReadOnlyList<(double Lat, double Lon)> ControlPoints(double lat1, double lon1, double lat2, double lon2)
    {
        var d = HaversineDistanceKm(lat1, lon1, lat2, lon2);
        if (d <= 4000) return new[] { MidpointLatLon(lat1, lon1, lat2, lon2) };
        return new[]
        {
            IntermediatePoint(lat1, lon1, lat2, lon2, 2000 / d),
            IntermediatePoint(lat1, lon1, lat2, lon2, 1 - 2000 / d),
        };
    }

    /// <summary>
    /// Model estimate of foF2 (MHz), the F2-layer critical frequency, at a point.
    /// IonosondeCalibration compares this with live measurements to correct it.
    /// </summary>
    public static double EstimateFoF2(int ssn, double lat, double lon, DateTime utc)
    {
        double zenith = SolarZenithAngle(lat, lon, utc);

        // Base increases with SSN
        double ssnFactor = 4.0 + ssn * 0.035;

        // Day/night variation: foF2 drops at night but doesn't go to zero
        double dayNightFactor = zenith < 90
            ? Math.Pow(Math.Cos(ToRad(zenith)), 0.3)
            : 0.2 + 0.1 * Math.Cos(ToRad(Math.Min(zenith, 120) - 90));

        // Latitude factor: foF2 higher near equator
        double latFactor = 1.0 + 0.3 * Math.Cos(ToRad(lat * 2));

        return ssnFactor * dayNightFactor * latFactor;
    }

    /// <summary>Point a fraction <paramref name="f"/> (0–1) of the way along the great circle.</summary>
    public static (double Lat, double Lon) IntermediatePoint(double lat1, double lon1, double lat2, double lon2, double f)
    {
        double phi1 = ToRad(lat1), lam1 = ToRad(lon1), phi2 = ToRad(lat2), lam2 = ToRad(lon2);
        double delta = HaversineDistanceKm(lat1, lon1, lat2, lon2) / 6371.0;
        if (delta < 1e-9) return (lat1, lon1);
        double a = Math.Sin((1 - f) * delta) / Math.Sin(delta);
        double b = Math.Sin(f * delta) / Math.Sin(delta);
        double x = a * Math.Cos(phi1) * Math.Cos(lam1) + b * Math.Cos(phi2) * Math.Cos(lam2);
        double y = a * Math.Cos(phi1) * Math.Sin(lam1) + b * Math.Cos(phi2) * Math.Sin(lam2);
        double z = a * Math.Sin(phi1) + b * Math.Sin(phi2);
        return (ToDeg(Math.Atan2(z, Math.Sqrt(x * x + y * y))), ToDeg(Math.Atan2(y, x)));
    }

    /// <summary>
    /// Calculate Lowest Usable Frequency (D-layer absorption model).
    /// </summary>
    public static double CalculateLuf(int ssn, double distanceKm, double midLat, double midLon, DateTime utc, int kIndex)
    {
        double zenith = SolarZenithAngle(midLat, midLon, utc);
        int hops = Math.Max(1, (int)Math.Ceiling(distanceKm / 3500.0));

        // Nighttime: D-layer largely disappears
        if (zenith >= 90)
            return 2.0 + kIndex * 0.3;

        // Daytime: D-layer absorption increases
        double cosZenith = Math.Cos(ToRad(zenith));
        double baseLuf = 2.0 + (ssn * 0.02 + 1.5) * Math.Pow(cosZenith, 0.5);

        // Multi-hop increases absorption
        baseLuf *= 1.0 + 0.2 * (hops - 1);

        // K-index storms increase absorption
        baseLuf += kIndex * 0.5;

        return Math.Max(1.8, Math.Min(baseLuf, 15.0));
    }

    /// <summary>
    /// Calculate reliability (0-99%) for a frequency on a given path.
    /// Based on where the frequency sits relative to MUF and LUF.
    /// </summary>
    public static int CalculateReliability(double freqMHz, double mufMHz, double lufMHz, int kIndex, int hops)
    {
        if (freqMHz <= 0 || mufMHz <= 0) return 0;

        // Top end: a forecast MUF is a MEDIAN — the real MUF is above it on about
        // half of days and spreads roughly ±15% day to day (ITU-R P.533 deciles).
        // So the chance the band reaches this frequency is ~50% at the MUF, ~87%
        // at the FOT (0.85·MUF), ~13% at 1.15·MUF, near zero by 1.3·MUF — not a
        // cliff that calls a marginal 10m opening impossible.
        double reachesUp = NormalCdf((mufMHz - freqMHz) / (MufSpread * mufMHz));

        // Bottom end: D-layer absorption grows as 1/f², so usefulness rises from
        // nothing at the LUF toward full well above it.
        double clearsAbsorption = freqMHz <= lufMHz ? 0 : 1 - Math.Pow(lufMHz / freqMHz, 2);

        double reliability = 99.0 * reachesUp * clearsAbsorption;

        // K-index degradation
        double kDegradation = 1.0;
        if (kIndex >= 4)
            kDegradation = Math.Max(0.1, 1.0 - (kIndex - 3) * 0.2);
        else if (kIndex >= 2)
            kDegradation = 1.0 - (kIndex - 1) * 0.05;
        reliability *= kDegradation;

        // Each extra hop adds ground-reflection and absorption loss.
        reliability *= Math.Pow(0.92, Math.Max(0, hops - 1));

        return (int)Math.Clamp(Math.Round(reliability), 0, 99);
    }

    /// <summary>Day-to-day spread of the real MUF around the forecast median, as a fraction (1σ).</summary>
    public const double MufSpread = 0.13;

    /// <summary>Standard normal CDF (Abramowitz &amp; Stegun 7.1.26, |error| &lt; 1.5e-7).</summary>
    public static double NormalCdf(double z)
    {
        double x = Math.Abs(z) / Math.Sqrt(2);
        double t = 1 / (1 + 0.3275911 * x);
        double erf = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.Exp(-x * x);
        return z >= 0 ? 0.5 * (1 + erf) : 0.5 * (1 - erf);
    }

    #endregion

    #region Helpers

    private static string MakeCacheKey(double deLat, double deLon, double dxLat, double dxLon)
        => $"{Math.Round(deLat, 1)},{Math.Round(deLon, 1)},{Math.Round(dxLat, 1)},{Math.Round(dxLon, 1)}";

    private static string NormalizeCondition(string condition)
    {
        // N0NBH returns "Good", "Fair", "Poor" — normalize casing
        return condition switch
        {
            var c when c.Contains("good", StringComparison.OrdinalIgnoreCase) => "Good",
            var c when c.Contains("fair", StringComparison.OrdinalIgnoreCase) => "Fair",
            var c when c.Contains("poor", StringComparison.OrdinalIgnoreCase) => "Poor",
            _ => condition
        };
    }

    private static double ToRad(double deg) => deg * Math.PI / 180.0;
    private static double ToDeg(double rad) => rad * 180.0 / Math.PI;

    #endregion
}
