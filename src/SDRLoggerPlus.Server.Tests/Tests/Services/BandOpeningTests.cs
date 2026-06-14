using FluentAssertions;
using SDRLoggerPlus.Server.Services.BandOpening;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BandOpeningTests
{
    // ── RBN spot line parsing ────────────────────────────────────

    [Fact]
    public void ParseSpotLine_StandardCwSpot()
    {
        var spot = BandOpeningLogic.ParseSpotLine(
            "DX de W3OA-#:     50125.1  K5XYZ          CW    24 dB  22 WPM  CQ      1830Z");

        spot.Should().NotBeNull();
        spot!.Skimmer.Should().Be("W3OA-#");
        spot.FrequencyKhz.Should().Be(50125.1);
        spot.DxCall.Should().Be("K5XYZ");
        spot.Mode.Should().Be("CW");
        spot.Snr.Should().Be(24);
    }

    [Fact]
    public void ParseSpotLine_Ft8WithBps()
    {
        var spot = BandOpeningLogic.ParseSpotLine(
            "DX de KM3T-2-#:   144174.0  W1ABC          FT8   15 dB  6 BPS   CQ      0405Z");
        spot.Should().NotBeNull();
        spot!.Mode.Should().Be("FT8");
        spot.FrequencyKhz.Should().Be(144174.0);
    }

    [Theory]
    [InlineData("login: ")]
    [InlineData("Welcome to the Reverse Beacon Network")]
    [InlineData("")]
    public void ParseSpotLine_NonSpotLines_ReturnNull(string line)
    {
        BandOpeningLogic.ParseSpotLine(line).Should().BeNull();
    }

    // ── skimmer call normalization ───────────────────────────────

    [Theory]
    [InlineData("W3OA-#", "W3OA")]
    [InlineData("KM3T-2-#", "KM3T")]
    [InlineData("DL9GTB-11", "DL9GTB")]
    [InlineData("K5XYZ", "K5XYZ")]
    public void NormalizeSkimmerCall_StripsSuffixes(string raw, string expected)
    {
        BandOpeningLogic.NormalizeSkimmerCall(raw).Should().Be(expected);
    }

    // ── band mapping (VHF/UHF alert bands) ───────────────────────

    [Theory]
    [InlineData(28500.0, "10m")]
    [InlineData(50125.0, "6m")]
    [InlineData(144200.0, "2m")]
    [InlineData(432100.0, "70cm")]
    [InlineData(14025.0, null)]   // HF — not an alert band
    public void AlertBandFor_MapsFrequencies(double khz, string? expected)
    {
        BandOpeningLogic.AlertBandFor(khz).Should().Be(expected);
    }

    // ── distance ─────────────────────────────────────────────────

    [Fact]
    public void HaversineKm_KnownDistance()
    {
        // Chicago → Milwaukee ≈ 131 km
        var km = BandOpeningLogic.HaversineKm(41.88, -87.63, 43.04, -87.91);
        km.Should().BeApproximately(131, 5);
    }

    // ── per-band cooldown ────────────────────────────────────────

    [Fact]
    public void Cooldown_FirstAlertFires_SecondWithinWindowSuppressed()
    {
        var gate = new BandCooldownGate();
        var t0 = DateTime.UtcNow;
        gate.ShouldAlert("6m", cooldownMinutes: 15, now: t0).Should().BeTrue();
        gate.ShouldAlert("6m", 15, t0.AddMinutes(5)).Should().BeFalse();
        gate.ShouldAlert("2m", 15, t0.AddMinutes(5)).Should().BeTrue();  // other band independent
        gate.ShouldAlert("6m", 15, t0.AddMinutes(16)).Should().BeTrue(); // window elapsed
    }
}
