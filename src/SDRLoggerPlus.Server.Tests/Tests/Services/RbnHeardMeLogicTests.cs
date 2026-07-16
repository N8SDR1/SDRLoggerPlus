using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services.Rbn;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class RbnHeardMeLogicTests
{
    private static RbnSpot Spot(string skimmer, string dx, double khz, string band) =>
        new() { Callsign = skimmer, Dx = dx, Frequency = khz, Band = band, Mode = "CW", Snr = 20, Timestamp = DateTime.UtcNow };

    [Fact]
    public void HeardBy_matches_dx_call_case_insensitively()
    {
        var spots = new[] { Spot("W3LPL", "K1ABC", 14025, "20m"), Spot("N4ZR", "W9XYZ", 7025, "40m") };
        var result = RbnHeardMeLogic.HeardBy(spots, "k1abc", null);
        Assert.Single(result);
        Assert.Equal("W3LPL", result[0].Callsign);
    }

    [Fact]
    public void HeardBy_filters_by_band_when_supplied()
    {
        var spots = new[] { Spot("W3LPL", "K1ABC", 14025, "20m"), Spot("N4ZR", "K1ABC", 7025, "40m") };
        Assert.Single(RbnHeardMeLogic.HeardBy(spots, "K1ABC", "20m"));
        Assert.Equal(2, RbnHeardMeLogic.HeardBy(spots, "K1ABC", null).Count);
    }

    [Theory]
    [InlineData(0, 5)]
    [InlineData(10, 10)]
    [InlineData(15, 15)]
    [InlineData(999, 15)]
    public void ClampWindowMinutes_clamps_to_5_to_15(int input, int expected)
        => Assert.Equal(expected, RbnHeardMeLogic.ClampWindowMinutes(input));

    [Fact]
    public void PickLocation_prefers_qrz_then_cty_then_null()
    {
        Assert.Equal((40.0, -75.0), RbnHeardMeLogic.PickLocation(40.0, -75.0, (1.0, 2.0)));
        Assert.Equal((1.0, 2.0), RbnHeardMeLogic.PickLocation(null, null, (1.0, 2.0)));
        Assert.Equal((1.0, 2.0), RbnHeardMeLogic.PickLocation(40.0, null, (1.0, 2.0))); // partial QRZ → cty
        Assert.Null(RbnHeardMeLogic.PickLocation(null, null, null));
    }
}
