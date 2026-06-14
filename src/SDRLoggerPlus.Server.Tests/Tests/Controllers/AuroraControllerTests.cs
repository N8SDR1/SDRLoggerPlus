using FluentAssertions;
using SDRLoggerPlus.Server.Controllers;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Controllers;

[Trait("Category", "Unit")]
public class AuroraControllerTests
{
    // Mirrors the real NOAA OVATION product: [Longitude(0..359), Latitude, Aurora%].
    private const string SampleJson = """
        {
          "Observation Time": "2026-06-12T18:55:00Z",
          "Forecast Time": "2026-06-12T19:48:00Z",
          "Data Format": "[Longitude, Latitude, Aurora]",
          "coordinates": [
            [0, -90, 6],
            [0, -89, 0],
            [10, 65, 80],
            [200, 70, 40],
            [359, 60, 2]
          ]
        }
        """;

    [Fact]
    public void ParseOvation_KeepsSignificantCellsAndCopiesTimes()
    {
        var forecast = AuroraController.ParseOvation(SampleJson);

        forecast.ObservationTime.Should().Be("2026-06-12T18:55:00Z");
        forecast.ForecastTime.Should().Be("2026-06-12T19:48:00Z");

        // The prob=0 and prob=2 cells fall below the MinProbability=3 threshold and are dropped.
        forecast.Points.Should().HaveCount(3);
        forecast.Points.Select(p => p.Aurora).Should().BeEquivalentTo(new[] { 6, 80, 40 });
    }

    [Fact]
    public void ParseOvation_NormalizesLongitudeToMinus180To180()
    {
        var forecast = AuroraController.ParseOvation(SampleJson);

        // Longitude 200 -> 200-360 = -160; 10 stays 10.
        forecast.Points.Should().Contain(p => p.Aurora == 40 && p.Lon == -160);
        forecast.Points.Should().Contain(p => p.Aurora == 80 && p.Lon == 10);
    }

    [Fact]
    public void ParseOvation_HandlesMissingCoordinates()
    {
        var forecast = AuroraController.ParseOvation("""{ "Observation Time": "t" }""");

        forecast.ObservationTime.Should().Be("t");
        forecast.Points.Should().BeEmpty();
    }
}
