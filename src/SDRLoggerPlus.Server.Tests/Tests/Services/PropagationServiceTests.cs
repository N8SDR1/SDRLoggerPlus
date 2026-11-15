using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Moq;
using Microsoft.Extensions.Logging;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class PropagationServiceTests
{
    #region SSN from SFI

    [Theory]
    [InlineData(67, 0)]       // Minimum SFI -> SSN=0
    [InlineData(70, 3)]       // Low SFI
    [InlineData(150, 86)]     // Moderate SFI
    [InlineData(200, 137)]    // High SFI
    public void SsnFromSfi_ReturnsExpectedValue(int sfi, int expectedSsn)
    {
        PropagationService.SsnFromSfi(sfi).Should().Be(expectedSsn);
    }

    [Fact]
    public void SsnFromSfi_BelowMinimum_ReturnsZero()
    {
        PropagationService.SsnFromSfi(50).Should().Be(0);
    }

    #endregion

    #region Haversine Distance

    [Theory]
    [InlineData(40.7128, -74.0060, 51.5074, -0.1278, 5570, 100)]  // NYC to London
    [InlineData(35.6762, 139.6503, -33.8688, 151.2093, 7823, 100)] // Tokyo to Sydney
    [InlineData(0, 0, 0, 180, 20015, 100)]                          // Antipodal points (~half circumference)
    public void HaversineDistance_ReturnsExpectedDistance(
        double lat1, double lon1, double lat2, double lon2,
        double expectedKm, double toleranceKm)
    {
        var result = PropagationService.HaversineDistanceKm(lat1, lon1, lat2, lon2);
        result.Should().BeApproximately(expectedKm, toleranceKm);
    }

    [Fact]
    public void HaversineDistance_SamePoint_ReturnsZero()
    {
        PropagationService.HaversineDistanceKm(52.0, -8.0, 52.0, -8.0).Should().Be(0);
    }

    #endregion

    #region Bearing

    [Fact]
    public void Bearing_NorthPole_ReturnsZero()
    {
        var bearing = PropagationService.BearingDeg(0, 0, 90, 0);
        bearing.Should().BeApproximately(0, 1);
    }

    [Fact]
    public void Bearing_East_Returns90()
    {
        var bearing = PropagationService.BearingDeg(0, 0, 0, 90);
        bearing.Should().BeApproximately(90, 1);
    }

    #endregion

    #region Solar Zenith Angle

    [Fact]
    public void SolarZenithAngle_NoonAtEquator_NearEquinox_IsLow()
    {
        // March 20 is near equinox, noon UTC at lon=0 should have low zenith
        var equinox = new DateTime(2025, 3, 20, 12, 0, 0, DateTimeKind.Utc);
        var zenith = PropagationService.SolarZenithAngle(0, 0, equinox);
        zenith.Should().BeLessThan(30);
    }

    [Fact]
    public void SolarZenithAngle_MidnightAtEquator_IsHigh()
    {
        var midnight = new DateTime(2025, 3, 20, 0, 0, 0, DateTimeKind.Utc);
        var zenith = PropagationService.SolarZenithAngle(0, 0, midnight);
        zenith.Should().BeGreaterThan(90);
    }

    #endregion

    #region MUF Calculations

    // London → Rome (~1,430 km, one control point) and Ohio → Germany (~7,000 km, two).
    [Fact]
    public void Muf_DaytimeHigherThanNighttime()
    {
        var daytime = new DateTime(2025, 6, 21, 12, 0, 0, DateTimeKind.Utc);
        var nighttime = new DateTime(2025, 6, 21, 0, 0, 0, DateTimeKind.Utc);

        var mufDay = PropagationService.CalculatePathMuf(80, 51.5, -0.1, 41.9, 12.5, daytime);
        var mufNight = PropagationService.CalculatePathMuf(80, 51.5, -0.1, 41.9, 12.5, nighttime);

        mufDay.Should().BeGreaterThan(mufNight);
    }

    [Fact]
    public void Muf_HigherSsn_ProducesHigherMuf()
    {
        var noon = new DateTime(2025, 6, 21, 12, 0, 0, DateTimeKind.Utc);
        PropagationService.CalculatePathMuf(150, 51.5, -0.1, 41.9, 12.5, noon)
            .Should().BeGreaterThan(PropagationService.CalculatePathMuf(10, 51.5, -0.1, 41.9, 12.5, noon));
    }

    [Fact]
    public void Muf_NeverBelowMinimum()
    {
        var midnight = new DateTime(2025, 12, 21, 0, 0, 0, DateTimeKind.Utc);
        PropagationService.CalculatePathMuf(0, 70, 0, 75, 20, midnight).Should().BeGreaterOrEqualTo(2.0);
    }

    /// <summary>
    /// A multi-hop path's MUF is its weakest control point's — not an extra 10% off
    /// per hop, which the old model applied and which shut 10m on paths measured open.
    /// </summary>
    [Fact]
    public void PathMuf_IsWeakestControlPoint_TimesHopFactor()
    {
        var t = new DateTime(2026, 11, 15, 13, 30, 0, DateTimeKind.Utc);
        var ssn = PropagationService.SsnFromSfi(110);
        var points = PropagationService.ControlPoints(39.7, -84.5, 51.0, 10.0);
        var weakest = points.Min(p => PropagationService.EstimateFoF2(ssn, p.Lat, p.Lon, t));
        var hopKm = PropagationService.HaversineDistanceKm(39.7, -84.5, 51.0, 10.0) / 2;

        PropagationService.CalculatePathMuf(ssn, 39.7, -84.5, 51.0, 10.0, t)
            .Should().BeApproximately(weakest * PropagationService.HopMFactor(hopKm), 1e-9);
    }

    [Fact]
    public void PathMuf_AppliesPerControlPointCorrection()
    {
        var t = new DateTime(2026, 11, 15, 13, 30, 0, DateTimeKind.Utc);
        var plain = PropagationService.CalculatePathMuf(44, 39.7, -84.5, 51.0, 10.0, t);
        PropagationService.CalculatePathMuf(44, 39.7, -84.5, 51.0, 10.0, t, new[] { 2.0, 2.0 })
            .Should().BeApproximately(plain * 2, 1e-9);
    }

    #endregion

    #region LUF Calculations

    [Fact]
    public void Luf_DaytimeHigherThanNighttime()
    {
        int ssn = 80;
        double dist = 5000;
        double midLat = 45, midLon = 0;

        var daytime = new DateTime(2025, 6, 21, 12, 0, 0, DateTimeKind.Utc);
        var nighttime = new DateTime(2025, 6, 21, 0, 0, 0, DateTimeKind.Utc);

        var lufDay = PropagationService.CalculateLuf(ssn, dist, midLat, midLon, daytime, 2);
        var lufNight = PropagationService.CalculateLuf(ssn, dist, midLat, midLon, nighttime, 2);

        lufDay.Should().BeGreaterThan(lufNight);
    }

    [Fact]
    public void Luf_HighKIndex_IncreasesLuf()
    {
        double dist = 5000;
        double midLat = 45, midLon = 0;
        var noon = new DateTime(2025, 6, 21, 12, 0, 0, DateTimeKind.Utc);

        var lufCalm = PropagationService.CalculateLuf(80, dist, midLat, midLon, noon, 1);
        var lufStorm = PropagationService.CalculateLuf(80, dist, midLat, midLon, noon, 7);

        lufStorm.Should().BeGreaterThan(lufCalm);
    }

    #endregion

    #region Reliability Calculations

    [Fact]
    public void Reliability_TapersAboveMuf_NotACliff()
    {
        var atMuf = PropagationService.CalculateReliability(20.0, 20.0, 5.0, 0, 1);
        var above = PropagationService.CalculateReliability(22.0, 20.0, 5.0, 0, 1);
        atMuf.Should().BeInRange(41, 54);
        above.Should().BeInRange(6, atMuf - 1);
    }

    [Fact]
    public void Reliability_MatchesClientPort()
    {
        // Same inputs as propagationGate.test.ts "matches the server".
        PropagationService.CalculateReliability(28.0, 25.5, 3.9, 0, 2).Should().Be(20);
    }

    [Fact]
    public void Reliability_WellAboveMuf_ReturnsZero()
    {
        var rel = PropagationService.CalculateReliability(30.0, 20.0, 5.0, 2, 1);
        rel.Should().Be(0);
    }

    [Fact]
    public void Reliability_BelowLuf_ReturnsZero()
    {
        var rel = PropagationService.CalculateReliability(3.0, 20.0, 5.0, 2, 1);
        rel.Should().Be(0);
    }

    [Fact]
    public void Reliability_AtFot_IsHighest()
    {
        double muf = 20.0;
        double luf = 5.0;
        double fot = muf * 0.85; // 17.0

        var relAtFot = PropagationService.CalculateReliability(fot, muf, luf, 2, 1);
        var relFarFromFot = PropagationService.CalculateReliability(8.0, muf, luf, 2, 1);

        relAtFot.Should().BeGreaterThan(relFarFromFot);
    }

    [Fact]
    public void Reliability_HighKIndex_ReducesReliability()
    {
        double freq = 14.0, muf = 20.0, luf = 5.0;

        var relCalm = PropagationService.CalculateReliability(freq, muf, luf, 1, 1);
        var relStorm = PropagationService.CalculateReliability(freq, muf, luf, 7, 1);

        relStorm.Should().BeLessThan(relCalm);
    }

    [Fact]
    public void Reliability_MultiHop_ReducesReliability()
    {
        double freq = 14.0, muf = 20.0, luf = 5.0;

        var relSingleHop = PropagationService.CalculateReliability(freq, muf, luf, 2, 1);
        var relMultiHop = PropagationService.CalculateReliability(freq, muf, luf, 2, 3);

        relMultiHop.Should().BeLessThan(relSingleHop);
    }

    [Fact]
    public void Reliability_NeverExceeds99()
    {
        // Test with ideal conditions
        double muf = 20.0;
        double fot = muf * 0.85;
        var rel = PropagationService.CalculateReliability(fot, muf, 3.0, 0, 1);
        rel.Should().BeLessOrEqualTo(99);
    }

    #endregion

    #region Full Prediction

    [Fact]
    public async Task PredictAsync_ReturnsCorrectDimensions()
    {
        var mockWeather = new Mock<ISpaceWeatherService>();
        mockWeather.Setup(s => s.GetCurrentAsync())
            .ReturnsAsync(new SpaceWeatherData(120, 2, 55, DateTime.UtcNow));

        var mockHttpFactory = new Mock<IHttpClientFactory>();
        mockHttpFactory.Setup(f => f.CreateClient(It.IsAny<string>()))
            .Returns(new HttpClient());

        var mockLogger = new Mock<ILogger<PropagationService>>();

        var service = new PropagationService(mockWeather.Object, NoIonosondes(), mockHttpFactory.Object, mockLogger.Object);
        var result = await service.PredictAsync(40.7, -74.0, 51.5, -0.1);

        result.HeatmapData.Should().HaveCount(9);
        result.HeatmapData[0].Should().HaveCount(24);
        result.BandNames.Should().HaveCount(9);
        result.CurrentBands.Should().HaveCount(9);
        result.MufMHz.Should().BeGreaterThan(0);
        result.LufMHz.Should().BeGreaterThan(0);
        result.DistanceKm.Should().BeApproximately(5570, 100);
        result.BearingDeg.Should().BeInRange(0, 360);
    }

    [Fact]
    public async Task PredictAsync_AllReliabilityValuesInRange()
    {
        var mockWeather = new Mock<ISpaceWeatherService>();
        mockWeather.Setup(s => s.GetCurrentAsync())
            .ReturnsAsync(new SpaceWeatherData(150, 3, 86, DateTime.UtcNow));

        var mockHttpFactory = new Mock<IHttpClientFactory>();
        mockHttpFactory.Setup(f => f.CreateClient(It.IsAny<string>()))
            .Returns(new HttpClient());

        var mockLogger = new Mock<ILogger<PropagationService>>();

        var service = new PropagationService(mockWeather.Object, NoIonosondes(), mockHttpFactory.Object, mockLogger.Object);
        var result = await service.PredictAsync(52.67, -8.63, 35.68, 139.65);

        foreach (var bandData in result.HeatmapData)
        {
            foreach (var rel in bandData)
            {
                rel.Should().BeInRange(0, 99);
            }
        }

        foreach (var band in result.CurrentBands)
        {
            band.Reliability.Should().BeInRange(0, 99);
            band.Status.Should().BeOneOf("EXCELLENT", "GOOD", "FAIR", "POOR", "CLOSED");
        }
    }

    #endregion

    #region Live ionosonde calibration

    private static IIonosondeService Ionosondes(params IonosondeReading[] readings)
    {
        var mock = new Mock<IIonosondeService>();
        mock.Setup(s => s.GetReadingsAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(new IonosondeSnapshot(readings, IonosondeService.Attribution, DateTime.UtcNow));
        return mock.Object;
    }

    private static IIonosondeService NoIonosondes() => Ionosondes();

    private static PropagationService ServiceWith(IIonosondeService ionosondes, int sfi = 110)
    {
        var weather = new Mock<ISpaceWeatherService>();
        weather.Setup(s => s.GetCurrentAsync()).ReturnsAsync(new SpaceWeatherData(sfi, 0, 40, DateTime.UtcNow));
        var http = new Mock<IHttpClientFactory>();
        http.Setup(f => f.CreateClient(It.IsAny<string>())).Returns(new HttpClient());
        return new PropagationService(weather.Object, ionosondes, http.Object, new Mock<ILogger<PropagationService>>().Object);
    }

    [Fact]
    public async Task PredictAsync_NoIonosondes_IsPlainModel()
    {
        var result = await ServiceWith(NoIonosondes()).PredictAsync(39.7, -84.5, 51.0, 10.0);
        result.ForecastSource.Should().Be("model");
        result.Calibration.Should().Be(1.0);
        result.Attribution.Should().BeNull();
    }

    /// <summary>
    /// Ionosondes along the path reading twice the model's foF2 double its MUF:
    /// the measured level wins, the model keeps the geometry.
    /// </summary>
    [Fact]
    public async Task PredictAsync_IonosondesAlongPath_ScaleMufToMeasurement()
    {
        var now = DateTime.UtcNow;
        var ssn = PropagationService.SsnFromSfi(110);
        // Ohio → Germany (~7,000 km): control points 2,000 km in from each end.
        var points = PropagationService.ControlPoints(39.7, -84.5, 51.0, 10.0);
        var readings = points
            .Select((p, i) => new IonosondeReading($"S{i}", p.Lat, p.Lon, 2 * PropagationService.EstimateFoF2(ssn, p.Lat, p.Lon, now), null, now))
            .ToArray();

        var plain = await ServiceWith(NoIonosondes()).PredictAsync(39.7, -84.5, 51.0, 10.0);
        var live = await ServiceWith(Ionosondes(readings)).PredictAsync(39.7, -84.5, 51.0, 10.0);

        live.ForecastSource.Should().Be("measured");
        live.Calibration.Should().BeApproximately(2.0, 0.05);
        live.MufMHz.Should().BeApproximately(plain.MufMHz * 2, 0.3);
        live.Attribution.Should().Contain("kc2g");
    }

    [Fact]
    public void Calibration_FarFromEveryIonosonde_UsesWorldwideCorrection()
    {
        var now = DateTime.UtcNow;
        var ssn = PropagationService.SsnFromSfi(110);
        // Only Australian stations, all reading 1.8x the model; path is Ohio → Germany.
        var readings = new[] { (-35.3, 149.1), (-19.6, 146.8), (-42.9, 147.3) }
            .Select((p, i) => new IonosondeReading($"VK{i}", p.Item1, p.Item2, 1.8 * PropagationService.EstimateFoF2(ssn, p.Item1, p.Item2, now), null, now))
            .ToList();

        var cal = IonosondeCalibration.ForPath(readings, ssn, 39.7, -84.5, 51.0, 10.0, now);

        cal.Source.Should().Be(ForecastSource.Calibrated);
        cal.Factors.Should().AllSatisfy(f => f.Should().BeApproximately(1.8, 0.01));
    }

    [Fact]
    public void ControlPoints_MidpointForShortPaths_TwoPointsForLong()
    {
        PropagationService.ControlPoints(39.7, -84.5, 42.4, -71.1).Should().ContainSingle(); // Ohio → Boston
        var longPath = PropagationService.ControlPoints(39.7, -84.5, 51.0, 10.0);
        longPath.Should().HaveCount(2);
        PropagationService.HaversineDistanceKm(39.7, -84.5, longPath[0].Lat, longPath[0].Lon).Should().BeApproximately(2000, 5);
        PropagationService.HaversineDistanceKm(51.0, 10.0, longPath[1].Lat, longPath[1].Lon).Should().BeApproximately(2000, 5);
    }

    [Fact]
    public void Parse_KeepsFreshReadings_NormalisesLongitude_SkipsStaleAndNull()
    {
        var now = new DateTime(2026, 11, 15, 13, 30, 0, DateTimeKind.Utc);
        var json = """
        [
          { "fof2": 8.0, "mufd": 29.4, "time": "2026-11-15T13:20:05", "station": { "name": "Millstone Hill", "latitude": "42.6", "longitude": "288.5" } },
          { "fof2": 8.6, "mufd": 28.8, "time": "2026-03-19T22:10:05", "station": { "name": "Stale", "latitude": "30.4", "longitude": "262.3" } },
          { "fof2": null, "mufd": null, "time": "2026-11-15T13:25:00", "station": { "name": "NoData", "latitude": "50", "longitude": "14" } },
          { "fof2": "7.9", "time": "2026-11-15T13:15:00", "station": { "name": "Sopron", "latitude": 47.6, "longitude": 16.7 } }
        ]
        """;

        var readings = IonosondeService.Parse(json, now);

        readings.Select(r => r.Name).Should().Equal("Millstone Hill", "Sopron");
        readings[0].Lon.Should().BeApproximately(-71.5, 0.01);
        readings[0].MufD.Should().Be(29.4);
        readings[1].FoF2.Should().Be(7.9);
    }

    #endregion
}
