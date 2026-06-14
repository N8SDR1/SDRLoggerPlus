using FluentAssertions;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Tests for the SDRLogger+-ported award trackers (WAS/WAZ/WPX/WAC/5BWAS/5BDXCC).
/// All counting is worked-based — any logged QSO counts.
/// </summary>
[Trait("Category", "Unit")]
public class AwardsServiceAwardsTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly AwardsService _service;

    public AwardsServiceAwardsTests()
    {
        _service = new AwardsService(_repo.Object);
    }

    private void SetupQsos(params Qso[] qsos) =>
        _repo.Setup(r => r.GetAllAsync()).ReturnsAsync(qsos);

    private static Qso MakeQso(string call, string band = "20m", string mode = "SSB",
        string? country = null, string? state = null, string? qth = null,
        string? continent = null, int? cqZone = null)
        => new()
        {
            Id = Guid.NewGuid().ToString(),
            Callsign = call,
            QsoDate = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc),
            TimeOn = "1200",
            Band = band,
            Mode = mode,
            Country = country,
            Continent = continent,
            Station = new StationInfo { State = state, Qth = qth, CqZone = cqZone },
        };

    // ─── WAS ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Was_CountsStatesViaResolutionChain()
    {
        SetupQsos(
            MakeQso("W8ABC", country: "United States", state: "OH"),
            MakeQso("KL7XYZ", country: "Alaska"),                                  // entity → AK
            MakeQso("W5DEF", country: "United States", qth: "Dallas, TX"),         // QTH → TX
            MakeQso("JA1ABC", country: "Japan", state: "TX"),                      // guard: non-US
            MakeQso("W8GHI", country: "United States", state: "OH", band: "40m")); // same state, 2nd band

        var stats = await _service.GetWasStatisticsAsync();

        stats.TotalWorked.Should().Be(3);
        stats.TotalNeeded.Should().Be(50);
        var oh = stats.States.Single(s => s.State == "OH");
        oh.Bands.Keys.Should().BeEquivalentTo("20m", "40m");
        oh.QsoCount.Should().Be(2);
    }

    [Fact]
    public async Task Was_BandFilter_LimitsCounting()
    {
        SetupQsos(
            MakeQso("W8ABC", country: "United States", state: "OH", band: "20m"),
            MakeQso("W5DEF", country: "United States", state: "TX", band: "40m"));

        var stats = await _service.GetWasStatisticsAsync(new StatisticsFilters(Band: "20m"));

        stats.TotalWorked.Should().Be(1);
        stats.States.Single().State.Should().Be("OH");
    }

    // ─── WAZ ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Waz_PrefersStoredZone_FallsBackToCty()
    {
        SetupQsos(
            MakeQso("W1AW", country: "United States", cqZone: 5),
            MakeQso("JA1ABC", country: "Japan"));   // no stored zone → cty → 25

        var stats = await _service.GetWazStatisticsAsync();

        stats.TotalWorked.Should().Be(2);
        stats.TotalNeeded.Should().Be(40);
        stats.Zones.Select(z => z.Zone).Should().BeEquivalentTo(new[] { 5, 25 });
        stats.Zones.Single(z => z.Zone == 25).Entities.Should().Contain("Japan");
    }

    // ─── WPX ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Wpx_GroupsByPrefix()
    {
        SetupQsos(
            MakeQso("N8AAA"),
            MakeQso("N8BBB", band: "40m"),
            MakeQso("VK9/N8SDR"));

        var stats = await _service.GetWpxStatisticsAsync();

        stats.TotalWorked.Should().Be(2);
        var n8 = stats.Prefixes.Single(p => p.Prefix == "N8");
        n8.Calls.Should().BeEquivalentTo("N8AAA", "N8BBB");
        n8.BandCount.Should().Be(2);
        stats.Prefixes.Should().Contain(p => p.Prefix == "VK9");
    }

    // ─── WAC ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Wac_SixContinents_Achieved_AntarcticaIsExtra()
    {
        SetupQsos(
            MakeQso("W1AW", continent: "NA", country: "United States"),
            MakeQso("PY2ABC", continent: "SA", country: "Brazil"),
            MakeQso("DL1ABC", continent: "EU", country: "Germany"),
            MakeQso("JA1ABC", continent: "AS", country: "Japan"),
            MakeQso("ZS1ABC", continent: "AF", country: "South Africa"),
            MakeQso("VK2ABC", country: "Australia"),          // continent null → cty → OC
            MakeQso("KC4AAA", continent: "AN", country: "Antarctica"));

        var stats = await _service.GetWacStatisticsAsync();

        stats.BaseWorked.Should().Be(6);
        stats.ExtraWorked.Should().Be(1);
        stats.Achieved.Should().BeTrue();
        stats.Continents.Single(c => c.Code == "AN").IsExtra.Should().BeTrue();
        stats.Continents.Single(c => c.Code == "OC").Entities.Should().Contain("Australia");
    }

    [Fact]
    public async Task Wac_FiveContinents_NotAchieved()
    {
        SetupQsos(
            MakeQso("W1AW", continent: "NA"),
            MakeQso("PY2ABC", continent: "SA"),
            MakeQso("DL1ABC", continent: "EU"),
            MakeQso("JA1ABC", continent: "AS"),
            MakeQso("ZS1ABC", continent: "AF"));

        var stats = await _service.GetWacStatisticsAsync();

        stats.BaseWorked.Should().Be(5);
        stats.Achieved.Should().BeFalse();
    }

    // ─── 5BWAS ───────────────────────────────────────────────────────────

    [Fact]
    public async Task FiveBandWas_CountsPerBand_IgnoresNonAwardBands()
    {
        SetupQsos(
            MakeQso("W8ABC", country: "United States", state: "OH", band: "20m"),
            MakeQso("W8ABC", country: "United States", state: "OH", band: "40m"),
            MakeQso("W5DEF", country: "United States", state: "TX", band: "20m"),
            MakeQso("W6GHI", country: "United States", state: "CA", band: "6m")); // not a 5B band

        var stats = await _service.Get5BWasStatisticsAsync();

        stats.Bands.Should().HaveCount(5);
        stats.Bands.Single(b => b.Band == "20m").Count.Should().Be(2);
        stats.Bands.Single(b => b.Band == "40m").Count.Should().Be(1);
        stats.Bands.Single(b => b.Band == "10m").Count.Should().Be(0);
        stats.Bands.All(b => b.Threshold == 50).Should().BeTrue();
        stats.Achieved.Should().BeFalse();
        stats.UnionCount.Should().Be(2); // OH + TX (CA was on 6m)
    }

    // ─── 5BDXCC ──────────────────────────────────────────────────────────

    [Fact]
    public async Task FiveBandDxcc_CountsEntitiesPerBand()
    {
        SetupQsos(
            MakeQso("JA1ABC", country: "Japan", band: "20m"),
            MakeQso("DL1ABC", country: "Germany", band: "20m"),
            MakeQso("JA2DEF", country: "Japan", band: "15m"));

        var stats = await _service.Get5BDxccStatisticsAsync();

        stats.Bands.Single(b => b.Band == "20m").Count.Should().Be(2);
        stats.Bands.Single(b => b.Band == "15m").Count.Should().Be(1);
        stats.Bands.All(b => b.Threshold == 100).Should().BeTrue();
        stats.UnionCount.Should().Be(2);
        stats.Achieved.Should().BeFalse();
    }

    [Fact]
    public async Task FiveBandDxcc_ModeFilter_Applies()
    {
        SetupQsos(
            MakeQso("JA1ABC", country: "Japan", band: "20m", mode: "CW"),
            MakeQso("DL1ABC", country: "Germany", band: "20m", mode: "SSB"));

        var stats = await _service.Get5BDxccStatisticsAsync("CW");

        stats.Bands.Single(b => b.Band == "20m").Count.Should().Be(1);
        stats.Bands.Single(b => b.Band == "20m").Items.Should().Contain("Japan");
    }
}
