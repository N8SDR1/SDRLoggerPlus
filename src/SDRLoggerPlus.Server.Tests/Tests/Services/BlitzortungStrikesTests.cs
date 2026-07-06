using System.Net;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RichardSzalay.MockHttp;
using SDRLoggerPlus.Server.Services.Weather;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BlitzortungStrikesTests
{
    private static IHttpClientFactory Factory(MockHttpMessageHandler handler)
    {
        var mock = new Moq.Mock<IHttpClientFactory>();
        mock.Setup(f => f.CreateClient(It.IsAny<string>())).Returns(() => handler.ToHttpClient());
        return mock.Object;
    }

    [Fact]
    public async Task GetStrikesRawAsync_parses_coords_and_timestamp_and_skips_malformed()
    {
        var handler = new MockHttpMessageHandler();
        // Flat arrays: [lon, lat, timestamp(ns), ...]; last row malformed (too short).
        handler.When("*getjson.php*")
            .Respond("application/json", "[[-91.6,44.8,1751690000000000000,0],[2.3,48.9,1751690001000000000,0],[999]]");

        var client = new BlitzortungClient(Factory(handler), NullLogger<BlitzortungClient>.Instance);
        var strikes = await client.GetStrikesRawAsync(new[] { 7 }, CancellationToken.None);

        strikes.Should().HaveCount(2);
        strikes[0].Lat.Should().BeApproximately(44.8, 1e-6);
        strikes[0].Lon.Should().BeApproximately(-91.6, 1e-6);
        strikes[0].Local.Should().BeFalse();
        strikes[0].TimestampUtc.Year.Should().Be(2025); // 1.7518e18 ns → 2025
    }
}
