using System.Text.Json;
using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Fixtures mirror the REAL spothole.app /api/v1/spots schema (verified live
/// 2026-06-11): received_time is a NUMERIC unix epoch, spotter country comes
/// as de_country, DX enrichment as dx_country/dx_continent/dx_grid/dx_dxcc_id,
/// freq in Hz.
/// </summary>
[Trait("Category", "Unit")]
public class SpotholeServiceTests
{
    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public void MapSpot_MapsCoreFields_FreqHzToKhz_AndApiEnrichment()
    {
        var spot = SpotholeService.MapSpot(Parse("""
            { "dx_call": "kc1df", "de_call": "9A5THR", "freq": 14283000.0,
              "mode": "USB", "comment": "tnx 4 new band , 73",
              "dx_country": "United States", "dx_continent": "NA",
              "dx_grid": "FN42EJ11", "dx_dxcc_id": 291,
              "received_time": 1781222727.633025 }
            """));

        spot.Should().NotBeNull();
        spot!.DxCall.Should().Be("KC1DF");
        spot.Spotter.Should().Be("9A5THR");
        spot.Frequency.Should().Be(14283.0); // kHz, matching the cluster pipeline
        spot.Mode.Should().Be("USB");
        spot.Country.Should().Be("United States");
        spot.Continent.Should().Be("NA");
        spot.Grid.Should().Be("FN42EJ11");
        spot.Dxcc.Should().Be(291);
    }

    [Fact]
    public void MapSpot_MissingDxCall_ReturnsNull()
    {
        SpotholeService.MapSpot(Parse("""{ "de_call": "W9ABC", "freq": 14074000 }""")).Should().BeNull();
    }

    [Fact]
    public void MapSpot_MissingFreq_ReturnsNull()
    {
        SpotholeService.MapSpot(Parse("""{ "dx_call": "K5XYZ", "de_call": "W9ABC" }""")).Should().BeNull();
    }

    [Fact]
    public void MapSpot_SigAppendedToComment_NullSigIgnored()
    {
        var withSig = SpotholeService.MapSpot(Parse("""
            { "dx_call": "K5XYZ", "de_call": "W9ABC", "freq": 7200000, "comment": "POTA", "sig": "POTA" }
            """));
        withSig!.Comment.Should().Be("POTA [POTA]");

        var nullSig = SpotholeService.MapSpot(Parse("""
            { "dx_call": "K5XYZ", "de_call": "W9ABC", "freq": 7200000, "comment": "73", "sig": null }
            """));
        nullSig!.Comment.Should().Be("73");
    }

    [Fact]
    public void MapSpot_NoApiCountry_FallsBackToCty()
    {
        var spot = SpotholeService.MapSpot(Parse("""
            { "dx_call": "DL1ABC", "de_call": "W9ABC", "freq": 14074000 }
            """));
        spot!.Country.Should().Be("Fed. Rep. of Germany");
    }

    // ── spotter country filter ───────────────────────────────────

    [Theory]
    [InlineData("""{ "de_call": "9A5THR", "de_country": "Croatia" }""", "United States", false)]
    [InlineData("""{ "de_call": "W1AW", "de_country": "United States" }""", "United States", true)]
    [InlineData("""{ "de_call": "9A5THR", "de_country": "Croatia" }""", "", true)]   // empty filter = all
    [InlineData("""{ "de_call": "W1AW" }""", "United States", true)]                  // no field → cty fallback
    [InlineData("""{ "de_call": "DL1ABC" }""", "United States", false)]
    public void SpotterMatchesCountry_PrefersApiField_FallsBackToCty(string json, string filter, bool expected)
    {
        SpotholeService.SpotterMatchesCountry(Parse(json), filter).Should().Be(expected);
    }

    // ── cursor (numeric epoch!) ──────────────────────────────────

    [Fact]
    public void Cursor_AdvancesToNewestNumericReceivedTime()
    {
        var spots = new[]
        {
            Parse("""{ "received_time": 1781222705.5 }"""),
            Parse("""{ "received_time": 1781222727.633025 }"""),
            Parse("""{ "no_received_time": true }"""),
            Parse("""{ "received_time": 1781222710.0 }"""), // older — must not move cursor back
        };

        double? cursor = null;
        foreach (var s in spots)
            cursor = SpotholeService.AdvanceCursor(cursor, s);

        cursor.Should().Be(1781222727.633025);
    }
}
