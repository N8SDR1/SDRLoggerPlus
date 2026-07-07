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
        // Flat arrays: [lon, lat, timestamp, ...]. The live feed's timestamp is a UTC
        // datetime string with a 9-digit (ns) fraction; the ns-since-epoch number form
        // is kept for compatibility. Last two rows malformed (too short / bad date).
        handler.When("*getjson.php*")
            .Respond("application/json",
                "[[-91.6,44.8,\"2026-07-05 12:00:00.792261120\",1,13661,121,117]," +
                "[2.3,48.9,1751690001000000000,0]," +
                "[999],[1.0,2.0,\"not-a-date\"]]");

        var client = new BlitzortungClient(Factory(handler), NullLogger<BlitzortungClient>.Instance);
        var strikes = await client.GetStrikesRawAsync(new[] { 0 }, CancellationToken.None);

        strikes.Should().HaveCount(2);
        strikes[0].Lat.Should().BeApproximately(44.8, 1e-6);
        strikes[0].Lon.Should().BeApproximately(-91.6, 1e-6);
        strikes[0].Local.Should().BeFalse();
        strikes[0].TimestampUtc.Kind.Should().Be(DateTimeKind.Utc);
        strikes[0].TimestampUtc.Should().BeCloseTo(
            new DateTime(2026, 7, 5, 12, 0, 0, DateTimeKind.Utc).AddTicks(7922611),
            TimeSpan.FromMilliseconds(1));
        strikes[1].TimestampUtc.Year.Should().Be(2025); // 1.7518e18 ns → 2025
    }

    [Fact]
    public async Task GetStrikesAsync_fetches_fresh_time_slices_and_filters_by_range()
    {
        var handler = new MockHttpMessageHandler();
        // Only the FRESH slices (n=00 current 5-min bucket, n=01 previous) are
        // mocked. The pre-fix code fetched n=07/12/13 — worldwide strikes
        // 35–70 minutes old, mislabeled as "Americas regions" — and would get
        // nothing here. Rows use the live feed's string-timestamp format.
        handler.When("*getjson.php*").WithQueryString("n", "00")
            .Respond("application/json",
                "[[-91.6,44.9,\"2026-07-06 12:00:01.000000000\",1]," +   // ~11 km from station
                "[2.3,48.9,\"2026-07-06 12:00:02.000000000\",1]]");      // Paris — far outside range
        handler.When("*getjson.php*").WithQueryString("n", "01")
            .Respond("application/json", "[]");

        var client = new BlitzortungClient(Factory(handler), NullLogger<BlitzortungClient>.Instance);
        var strikes = await client.GetStrikesAsync(44.8, -91.6, rangeKm: 100, CancellationToken.None);

        strikes.Should().HaveCount(1);
        strikes[0].DistanceKm.Should().BeLessThan(100);
    }
}
