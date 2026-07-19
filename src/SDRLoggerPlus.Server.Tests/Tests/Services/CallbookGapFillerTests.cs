using FluentAssertions;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// The callbook must only fill gaps in an auto-logged QSO, never overwrite what
/// the logging source reported. WSJT-X sends the grid the station actually
/// transmitted, which beats a stale/portable-wrong callbook entry.
/// </summary>
[Trait("Category", "Unit")]
public class CallbookGapFillerTests
{
    private static CreateQsoRequest Request(
        string? name = null, string? grid = null, string? country = null, string? qth = null) =>
        new(Callsign: "N9BC", QsoDate: new DateTime(2026, 7, 18), TimeOn: "1200",
            Band: "20m", Mode: "FT8", Name: name, Grid: grid, Country: country, Qth: qth);

    [Fact]
    public void Fill_KeepsSourceGrid_EvenWhenCallbookDisagrees()
    {
        var result = CallbookGapFiller.Fill(Request(grid: "EN54"), grid: "FN31PR");

        result.Grid.Should().Be("EN54", "the grid WSJT-X sent is what the station transmitted");
    }

    [Fact]
    public void Fill_UsesCallbookGrid_OnlyWhenSourceSentNone()
    {
        var result = CallbookGapFiller.Fill(Request(grid: null), grid: "FN31PR");

        result.Grid.Should().Be("FN31PR");
    }

    [Fact]
    public void Fill_ComposesQthFromCityAndState()
    {
        var result = CallbookGapFiller.Fill(Request(), city: "Newington", state: "CT");

        result.Qth.Should().Be("Newington, CT");
    }

    [Theory]
    [InlineData("Green Bay", null, "Green Bay")]
    [InlineData(null, "WI", "WI")]
    [InlineData(null, null, null)]
    public void Fill_ComposesQthFromWhicheverPartsExist(string? city, string? state, string? expected)
    {
        CallbookGapFiller.Fill(Request(), city: city, state: state).Qth.Should().Be(expected);
    }

    [Fact]
    public void Fill_KeepsSourceQth_WhenAlreadyPresent()
    {
        var result = CallbookGapFiller.Fill(Request(qth: "Portable /R"), city: "Newington", state: "CT");

        result.Qth.Should().Be("Portable /R");
    }

    [Fact]
    public void Fill_TreatsWhitespaceAsMissing()
    {
        var result = CallbookGapFiller.Fill(Request(name: "   "), name: "Hiram Maxim");

        result.Name.Should().Be("Hiram Maxim");
    }

    [Fact]
    public void Fill_LeavesUnrelatedFieldsAlone()
    {
        var original = Request(grid: "EN54");
        var result = CallbookGapFiller.Fill(original, name: "Hiram", city: "Newington", state: "CT");

        result.Callsign.Should().Be(original.Callsign);
        result.Band.Should().Be(original.Band);
        result.Mode.Should().Be(original.Mode);
        result.TimeOn.Should().Be(original.TimeOn);
    }

    [Fact]
    public void HasGap_IsFalse_WhenEverythingAlreadyPopulated()
    {
        var full = Request(name: "Hiram", grid: "FN31", country: "United States", qth: "Newington, CT");

        CallbookGapFiller.HasGap(full).Should().BeFalse("a complete QSO needs no callbook call");
    }

    [Fact]
    public void HasGap_IsTrue_ForATypicalWsjtxQso()
    {
        // WSJT-X sends grid + country but has no protocol field for QTH.
        CallbookGapFiller.HasGap(Request(grid: "EN54", country: "United States"))
            .Should().BeTrue();
    }
}
