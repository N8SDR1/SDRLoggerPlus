using FluentAssertions;
using SDRLoggerPlus.Server.Services.Adif;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services.Adif;

/// <summary>
/// Every value exercised here was taken from the live 24,544-record log. The audit that
/// prompted this class found 27 distinct modes and 13 distinct bands in it, 1,130 records
/// carrying a mode the ADIF enumeration does not define.
/// </summary>
[Trait("Category", "Unit")]
public class AdifFieldNormalizerTests
{
    #region Band

    [Theory]
    [InlineData("40M", "40m")]
    [InlineData("10M", "10m")]
    [InlineData("80M", "80m")]
    [InlineData("160M", "160m")]
    [InlineData("15M", "15m")]
    [InlineData("12M", "12m")]
    [InlineData("30M", "30m")]
    [InlineData("60M", "60m")]
    public void UpperCaseBandIsCorrectedToCanonicalCase(string stored, string expected)
    {
        // 13,106 of 24,544 records carry an upper-case band. Same band, different spelling —
        // safe to rewrite, and it is what made the edit form's Band dropdown render blank.
        var (value, issue) = AdifFieldNormalizer.NormalizeBand(stored);

        value.Should().Be(expected);
        issue!.Action.Should().Be(AdifFieldAction.Corrected);
        issue.Original.Should().Be(stored);
    }

    [Theory]
    [InlineData("20m")]
    [InlineData("17m")]
    [InlineData("6m")]
    [InlineData("2m")]
    [InlineData("70cm")]
    public void AlreadyCanonicalBandReportsNothing(string band)
    {
        var (value, issue) = AdifFieldNormalizer.NormalizeBand(band);

        value.Should().Be(band);
        issue.Should().BeNull("a value that needed no change must not clutter the report");
    }

    [Fact]
    public void UnknownBandIsKeptAndFlagged()
    {
        // The junk bands that started this: "6mm" and "5581m" were real records in this log.
        var (value, issue) = AdifFieldNormalizer.NormalizeBand("6mm");

        value.Should().Be("6mm", "an unrecognised value is still evidence — never discard it");
        issue!.Action.Should().Be(AdifFieldAction.Flagged);
    }

    [Fact]
    public void BandWhitespaceIsTrimmed()
    {
        AdifFieldNormalizer.NormalizeBand("  40M  ").Value.Should().Be("40m");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void EmptyBandIsLeftAloneWithoutComplaint(string? band)
    {
        var (_, issue) = AdifFieldNormalizer.NormalizeBand(band);
        issue.Should().BeNull();
    }

    #endregion

    #region Mode

    [Fact]
    public void PhIsRewrittenToSsb()
    {
        // The one alias earned by evidence rather than assumption: all 340 "PH" records in
        // this log have an SSB twin at the same callsign, date and second.
        var (value, issue) = AdifFieldNormalizer.NormalizeMode("PH");

        value.Should().Be("SSB");
        issue!.Action.Should().Be(AdifFieldAction.Corrected);
    }

    [Theory]
    [InlineData("FT8")]
    [InlineData("SSB")]
    [InlineData("FT4")]
    [InlineData("CW")]
    [InlineData("JT65")]
    [InlineData("MFSK")]
    [InlineData("PSK31")]
    [InlineData("RTTY")]
    [InlineData("PKT")]
    [InlineData("JT9")]
    [InlineData("FM")]
    [InlineData("HELL")]
    [InlineData("DIGITALVOICE")]
    [InlineData("AM")]
    [InlineData("OLIVIA")]
    [InlineData("MSK144")]
    [InlineData("PSK")]
    [InlineData("JT65B")]
    [InlineData("VARA HF")]
    // SSB submodes, but loggers write them into MODE and they carry which-sideband info.
    [InlineData("USB")]
    [InlineData("LSB")]
    public void ModesThisLogLegitimatelyContainsPassThroughUntouched(string mode)
    {
        var (value, issue) = AdifFieldNormalizer.NormalizeMode(mode);

        value.Should().Be(mode);
        issue.Should().BeNull();
    }

    [Theory]
    [InlineData("FT2")]
    [InlineData("29")]
    [InlineData("PSK3")]
    public void CorruptModesAreKeptVerbatimAndFlagged_NotGuessedAt(string mode)
    {
        // These are truncations — "PSK3" of PSK31, and "FT2"/"29" of whatever sat at 14084 kHz.
        // Tempting to map, but the mapping would be our guess, and a confidently wrong mode is
        // worse than a flagged one. The operator decides; we surface it.
        var (value, issue) = AdifFieldNormalizer.NormalizeMode(mode);

        value.Should().Be(mode);
        issue!.Action.Should().Be(AdifFieldAction.Flagged);
        issue.Result.Should().Be(mode, "flagged means untouched");
    }

    [Fact]
    public void DataIsFlaggedRatherThanResolvedFromFrequency()
    {
        // 477 records carry the generic "DATA". It could be resolved from frequency, but 422
        // of them are duplicates of an MFSK record — the duplicate key fix removes the harm,
        // so there is no reason to gamble on the mode.
        var (value, issue) = AdifFieldNormalizer.NormalizeMode("DATA");

        value.Should().Be("DATA");
        issue!.Action.Should().Be(AdifFieldAction.Flagged);
    }

    [Fact]
    public void LowerCaseModeIsCorrectedToCanonicalCase()
    {
        var (value, issue) = AdifFieldNormalizer.NormalizeMode("ft8");

        value.Should().Be("FT8");
        issue!.Action.Should().Be(AdifFieldAction.Corrected);
    }

    [Fact]
    public void FreeDvKeepsItsCanonicalSpelling()
    {
        AdifFieldNormalizer.NormalizeMode("FreeDV").Value.Should().Be("FREEDV");
    }

    #endregion

    #region Identity key

    [Fact]
    public void CanonicalBandKeyCollapsesCaseSoDuplicatesCompareEqual()
    {
        // The whole point: "40M" and "40m" must produce one key, or the same QSO imports twice.
        AdifFieldNormalizer.CanonicalBandKey("40M")
            .Should().Be(AdifFieldNormalizer.CanonicalBandKey("40m"));
    }

    [Fact]
    public void CanonicalBandKeyStillComparesUnknownBandsWithThemselves()
    {
        AdifFieldNormalizer.CanonicalBandKey("6mm")
            .Should().Be(AdifFieldNormalizer.CanonicalBandKey("6MM"));
    }

    [Fact]
    public void CanonicalBandKeyOfNothingIsEmpty()
    {
        AdifFieldNormalizer.CanonicalBandKey(null).Should().BeEmpty();
        AdifFieldNormalizer.CanonicalBandKey("  ").Should().BeEmpty();
    }

    [Theory]
    [InlineData("FT8", true)]
    [InlineData("PH", true)]
    [InlineData("FT2", false)]
    [InlineData("29", false)]
    [InlineData(null, false)]
    public void IsKnownModeAgreesWithWhatNormalizeFlags(string? mode, bool known)
    {
        AdifFieldNormalizer.IsKnownMode(mode).Should().Be(known);
    }

    #endregion
}
