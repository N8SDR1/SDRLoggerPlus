using FluentAssertions;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Issue #46: the GridTracker map painted grids confirmed off eQSL/QRZ-Logbook
/// confirmations while FFMA (correctly, per ARRL rules) did not. These tests pin
/// the shared <see cref="ConfirmationPolicy"/> and prove the grid map and VUCC
/// honor a requested <see cref="ConfirmationRule"/>. They also pin the VUCC
/// grid-source fix: grids carried only on Station.Grid (every live-logged QSO)
/// must count — the same class of bug as issue #42.
/// </summary>
[Trait("Category", "Unit")]
public class GridConfirmationRuleTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly AwardsService _service;

    public GridConfirmationRuleTests()
    {
        _service = new AwardsService(_repo.Object);
    }

    private void SetupQsos(params Qso[] qsos) =>
        _repo.Setup(r => r.GetAllAsync()).ReturnsAsync(qsos);

    private static Qso Q(string? grid = null, string? stationGrid = null, string band = "6m",
        string? lotw = null, string? paper = null, string? eqsl = null, string? qrz = null)
        => new()
        {
            Id = Guid.NewGuid().ToString(),
            Callsign = "TEST",
            Grid = grid,
            Band = band,
            Mode = "FT8",
            QsoDate = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc),
            TimeOn = "1200",
            Station = stationGrid != null ? new StationInfo { Grid = stationGrid } : null,
            Qsl = new QslStatus
            {
                Rcvd = paper,
                Lotw = new LotwStatus { Rcvd = lotw },
                Eqsl = new EqslStatus { Rcvd = eqsl },
                Qrz = new QrzStatus { Rcvd = qrz },
            },
        };

    // ─── ConfirmationPolicy ──────────────────────────────────────────────

    [Theory]
    [InlineData("Y", null, null, null)] // LoTW
    [InlineData(null, "Y", null, null)] // paper card
    [InlineData(null, null, "Y", null)] // eQSL
    [InlineData(null, null, null, "Y")] // QRZ Logbook
    public void AnyChannelCountsUnderTheAnyRule(string? lotw, string? paper, string? eqsl, string? qrz)
    {
        ConfirmationPolicy.Any(Q(grid: "EN82", lotw: lotw, paper: paper, eqsl: eqsl, qrz: qrz))
            .Should().BeTrue();
    }

    [Theory]
    [InlineData("Y", null, true)]  // LoTW counts
    [InlineData(null, "Y", true)]  // paper counts
    [InlineData(null, null, false)]
    public void AwardRulesAcceptOnlyLotwAndPaper(string? lotw, string? paper, bool expected)
    {
        ConfirmationPolicy.AwardRules(Q(grid: "EN82", lotw: lotw, paper: paper))
            .Should().Be(expected);
    }

    [Fact]
    public void EqslAndQrzConfirmationsDoNotSatisfyAwardRules()
    {
        // The exact QSO shape behind issue #46: confirmed everywhere except
        // where ARRL looks.
        var qso = Q(grid: "EN82", eqsl: "Y", qrz: "Y");

        ConfirmationPolicy.AwardRules(qso).Should().BeFalse();
        ConfirmationPolicy.Any(qso).Should().BeTrue();
    }

    [Fact]
    public void OnlyTheLiteralYesFlagCounts()
    {
        // ADIF also uses N/R/I/V in these fields; none of them is a confirmation.
        ConfirmationPolicy.Any(Q(grid: "EN82", lotw: "N", paper: "R", eqsl: "I", qrz: "V"))
            .Should().BeFalse();
        ConfirmationPolicy.Any(Q(grid: "EN82")).Should().BeFalse();
    }

    // ─── Grid map honors the rule ────────────────────────────────────────

    [Fact]
    public async Task GridMapDefaultsToAnyChannel()
    {
        SetupQsos(Q(grid: "EN82", eqsl: "Y"));

        var map = await _service.GetGridMapAsync(new StatisticsFilters(Band: "6m"));

        map.Grids.Single(g => g.Grid == "EN82").Confirmed.Should().BeTrue();
        map.ConfirmedGrids.Should().Be(1);
    }

    [Fact]
    public async Task GridMapUnderAwardRulesRejectsEqslOnlyGrids()
    {
        // eQSL-only EN82 stays worked; LoTW-confirmed EN74 counts. This is the
        // #46 report: the map must agree with FFMA about what green means.
        SetupQsos(Q(grid: "EN82", eqsl: "Y", qrz: "Y"), Q(grid: "EN74", lotw: "Y"));

        var map = await _service.GetGridMapAsync(
            new StatisticsFilters(Band: "6m", Confirmations: ConfirmationRule.AwardRules));

        map.Grids.Single(g => g.Grid == "EN82").Confirmed.Should().BeFalse();
        map.Grids.Single(g => g.Grid == "EN74").Confirmed.Should().BeTrue();
        map.ConfirmedGrids.Should().Be(1);
    }

    // ─── VUCC honors the rule ────────────────────────────────────────────

    [Fact]
    public async Task VuccUnderAwardRulesRejectsEqslOnlyGrids()
    {
        SetupQsos(Q(grid: "EN82", eqsl: "Y"), Q(grid: "EN74", lotw: "Y"));

        var stats = await _service.GetVuccStatisticsAsync(
            new StatisticsFilters(Confirmations: ConfirmationRule.AwardRules));

        stats.Grids.Single(g => g.Grid == "EN82").Confirmed.Should().BeFalse();
        stats.Grids.Single(g => g.Grid == "EN74").Confirmed.Should().BeTrue();
        stats.BandSummaries["6m"].ConfirmedGrids.Should().Be(1);
    }

    [Fact]
    public async Task VuccStillDefaultsToAnyChannel()
    {
        // REST callers that never heard of the parameter keep today's numbers.
        SetupQsos(Q(grid: "EN82", qrz: "Y"));

        var stats = await _service.GetVuccStatisticsAsync();

        stats.Grids.Single(g => g.Grid == "EN82").Confirmed.Should().BeTrue();
    }

    // ─── VUCC sees Station.Grid (issue #42's class of bug) ───────────────

    [Fact]
    public async Task VuccCountsALiveLoggedQsoWhoseGridIsOnlyOnStation()
    {
        // Live-logged QSOs (manual entry, WSJT-X auto-log) store their grid on
        // Station.Grid only. VUCC used to read just the legacy top-level field,
        // so these never appeared — the same blind spot FFMA had in #42.
        SetupQsos(Q(stationGrid: "EN82dk", lotw: "Y"));

        var stats = await _service.GetVuccStatisticsAsync();

        var row = stats.Grids.Single(g => g.Grid == "EN82");
        row.Confirmed.Should().BeTrue();
        row.Band.Should().Be("6m");
    }

    [Fact]
    public async Task VuccPrefersStationGridOverTheLegacyTopLevelField()
    {
        // When both exist, Station.Grid is the maintained value — same
        // precedence as FFMA and the grid map.
        SetupQsos(Q(grid: "AA00", stationGrid: "EN82"));

        var stats = await _service.GetVuccStatisticsAsync();

        stats.Grids.Should().ContainSingle(g => g.Grid == "EN82");
        stats.Grids.Should().NotContain(g => g.Grid == "AA00");
    }
}
