using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Rig mode-string mapping. A blank/missing mode (e.g. a POTA spot the feed left mode-less) must fall
/// back to the band's phone mode instead of sending an empty modulation command that leaves the rig on
/// a wrong/digital mode — #59 (Zuzudaddy's clicked-spot-sets-DIGU-on-AetherSDR report).
/// </summary>
[Trait("Category", "Unit")]
public class RigModeMappingTests
{
    private const long Freq40m = 7_100_000;   // below 10 MHz → LSB
    private const long Freq20m = 14_074_000;  // above 10 MHz → USB

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Tci_blankMode_fallsBackToBandPhoneMode(string? mode)
    {
        TciRadioConnection.MapToTciMode(mode!, Freq40m).Should().Be("LSB");
        TciRadioConnection.MapToTciMode(mode!, Freq20m).Should().Be("USB");
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void Hamlib_blankMode_fallsBackToBandPhoneMode(string? mode)
    {
        HamlibService.MapToHamlibMode(mode!, Freq40m).Should().Be("LSB");
        HamlibService.MapToHamlibMode(mode!, Freq20m).Should().Be("USB");
    }

    [Fact]
    public void Ssb_and_phone_still_map_by_band_and_digital_is_unchanged()
    {
        // Regression guard: the blank-mode fallback must not have altered the real cases.
        TciRadioConnection.MapToTciMode("SSB", Freq40m).Should().Be("LSB");
        TciRadioConnection.MapToTciMode("SSB", Freq20m).Should().Be("USB");
        TciRadioConnection.MapToTciMode("FT8", Freq20m).Should().Be("DIGU");
        TciRadioConnection.MapToTciMode("ssb ", Freq40m).Should().Be("LSB"); // trim + case-insensitive

        HamlibService.MapToHamlibMode("SSB", Freq40m).Should().Be("LSB");
        HamlibService.MapToHamlibMode("FT8", Freq20m).Should().Be("PKTUSB");
    }
}
