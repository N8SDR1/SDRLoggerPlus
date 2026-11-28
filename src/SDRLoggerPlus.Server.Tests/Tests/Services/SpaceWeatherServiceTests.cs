using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using System.Net;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// SFI and SSN must be TODAY's values. The old source was NOAA's monthly
/// solar-cycle series, so the header showed last month's averages (SFI 108 /
/// SSN 46 on 2026-11-27, against a day's 91 / 30).
/// </summary>
[Trait("Category", "Unit")]
public class SpaceWeatherServiceTests
{
    private readonly Mock<ILogger<SpaceWeatherService>> _loggerMock = new();
    private readonly Mock<IHttpClientFactory> _httpClientFactoryMock = new();
    private readonly Mock<HttpMessageHandler> _httpMessageHandlerMock = new();

    public SpaceWeatherServiceTests()
    {
        var httpClient = new HttpClient(_httpMessageHandlerMock.Object);
        _httpClientFactoryMock.Setup(f => f.CreateClient(It.IsAny<string>())).Returns(httpClient);
    }

    private static string Tag(DateTime utc) => utc.ToString("yyyy-MM-ddTHH:mm:ss");

    private static string Flux(DateTime day) => $$"""
        [
          { "time_tag": "{{Tag(day.AddHours(22))}}", "frequency": 2800, "flux": 91.0, "reporting_schedule": "Afternoon" },
          { "time_tag": "{{Tag(day.AddHours(20))}}", "frequency": 2800, "flux": 90.0, "reporting_schedule": "Noon" },
          { "time_tag": "{{Tag(day.AddDays(-1).AddHours(22))}}", "frequency": 2800, "flux": 88.0, "reporting_schedule": "Afternoon" }
        ]
        """;

    private static string Sunspots(DateTime day) => $$"""
        [
          { "Obsdate": "{{Tag(day.AddDays(-1))}}", "swpc_ssn": 22 },
          { "Obsdate": "{{Tag(day)}}", "swpc_ssn": 30 }
        ]
        """;

    private const string KIndex = """
        [ { "time_tag": "2026-11-28T00:05:00", "kp_index": 1.0 } ]
        """;

    private const string HamQsl = """
        <solar><solardata>
          <updated>28 Nov 2026 0010 GMT</updated>
          <solarflux>93</solarflux><kindex>2</kindex><sunspots>35</sunspots>
        </solardata></solar>
        """;

    private SpaceWeatherService Service() => new(_httpClientFactoryMock.Object, _loggerMock.Object);

    [Fact]
    public async Task UsesTodaysDailyFluxAndSunspotNumber()
    {
        var today = DateTime.UtcNow.Date;
        SetupHttpResponse(SpaceWeatherService.DailyFluxUrl, Flux(today));
        SetupHttpResponse(SpaceWeatherService.DailySunspotUrl, Sunspots(today));
        SetupHttpResponse(SpaceWeatherService.KIndexUrl, KIndex);

        var result = await Service().GetCurrentAsync();

        result.SolarFluxIndex.Should().Be(91);        // newest reading of the day
        result.SunspotNumber.Should().Be(30);         // today's, not yesterday's 22
        result.KIndex.Should().Be(1);
        result.Timestamp.Should().Be(today.AddHours(22));
    }

    [Fact]
    public async Task NeverReadsTheMonthlySeries()
    {
        var today = DateTime.UtcNow.Date;
        SetupHttpResponse(SpaceWeatherService.DailyFluxUrl, Flux(today));
        SetupHttpResponse(SpaceWeatherService.DailySunspotUrl, Sunspots(today));
        SetupHttpResponse(SpaceWeatherService.KIndexUrl, KIndex);

        await Service().GetCurrentAsync();

        _httpMessageHandlerMock.Protected().Verify("SendAsync", Times.Never(),
            ItExpr.Is<HttpRequestMessage>(r => r.RequestUri!.ToString().Contains("observed-solar-cycle-indices")),
            ItExpr.IsAny<CancellationToken>());
    }

    [Fact]
    public async Task FallsBackToHamQsl_WhenNoaaDailyDataIsStale()
    {
        var weekAgo = DateTime.UtcNow.Date.AddDays(-7);
        SetupHttpResponse(SpaceWeatherService.DailyFluxUrl, Flux(weekAgo));
        SetupHttpResponse(SpaceWeatherService.DailySunspotUrl, Sunspots(weekAgo));
        SetupHttpResponse(SpaceWeatherService.KIndexUrl, KIndex);
        SetupHttpResponse(SpaceWeatherService.HamQslUrl, HamQsl);

        var result = await Service().GetCurrentAsync();

        result.SolarFluxIndex.Should().Be(93);
        result.SunspotNumber.Should().Be(35);
        result.KIndex.Should().Be(2);
    }

    [Fact]
    public async Task FallsBackToHamQsl_WhenNoaaFails()
    {
        SetupHttpResponse(SpaceWeatherService.DailyFluxUrl, "not json", HttpStatusCode.InternalServerError);
        SetupHttpResponse(SpaceWeatherService.HamQslUrl, HamQsl);

        var result = await Service().GetCurrentAsync();

        result.SolarFluxIndex.Should().Be(93);
        result.SunspotNumber.Should().Be(35);
    }

    [Fact]
    public async Task CachesResults_For15Minutes()
    {
        var today = DateTime.UtcNow.Date;
        SetupHttpResponse(SpaceWeatherService.DailyFluxUrl, Flux(today));
        SetupHttpResponse(SpaceWeatherService.DailySunspotUrl, Sunspots(today));
        SetupHttpResponse(SpaceWeatherService.KIndexUrl, KIndex);

        var service = Service();
        var result1 = await service.GetCurrentAsync();
        var result2 = await service.GetCurrentAsync();

        result1.Should().Be(result2);
        _httpMessageHandlerMock.Protected().Verify("SendAsync", Times.Exactly(3), // flux, sunspots, K-index
            ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>());
    }

    [Fact]
    public void LatestDailyFlux_PicksTheNewestReading()
    {
        var day = new DateTime(2026, 11, 27, 0, 0, 0, DateTimeKind.Utc);
        var latest = SpaceWeatherService.LatestDailyFlux(Flux(day));
        latest.Should().NotBeNull();
        latest!.Value.Flux.Should().Be(91.0);
        latest.Value.TimeUtc.Should().Be(day.AddHours(22));
    }

    [Fact]
    public void LatestDailySunspots_ReturnsNull_ForAnEmptyFeed()
    {
        SpaceWeatherService.LatestDailySunspots("[]").Should().BeNull();
    }

    private void SetupHttpResponse(string url, string responseContent, HttpStatusCode status = HttpStatusCode.OK)
    {
        _httpMessageHandlerMock
            .Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => req.RequestUri!.ToString() == url),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(() => new HttpResponseMessage
            {
                StatusCode = status,
                Content = new StringContent(responseContent)
            });
    }
}
