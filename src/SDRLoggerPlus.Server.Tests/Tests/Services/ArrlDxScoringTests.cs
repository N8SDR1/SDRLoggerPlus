using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Exercises the ARRL DX W/VE↔DX role split (sub-project C) end-to-end against the
/// real seeded definition: a W/VE operator scores only DX stations and counts DXCC
/// multipliers; a DX operator scores only W/VE stations and counts state mults.
/// </summary>
[Trait("Category", "Unit")]
public class ArrlDxScoringTests
{
    private static ContestDefinition ArrlDxCw() =>
        SeedContests.All.First(d => d.Id == "arrl-dx-cw");

    private static readonly MyExchange WveOp = new() { Country = "United States", Continent = "NA", State = "OH" };
    private static readonly MyExchange DxOp = new() { Country = "Germany", Continent = "EU" };

    private static Qso Dx(string call) => new()
    {
        Callsign = call, Band = "20M", Mode = "CW",
        Country = "Germany", Continent = "EU",
        Station = new StationInfo { Country = "Germany", Continent = "EU" },
        Contest = new ContestInfo { RcvdPower = "100" },
    };

    private static Qso Usa(string call, string state) => new()
    {
        Callsign = call, Band = "20M", Mode = "CW",
        Country = "United States", Continent = "NA",
        Station = new StationInfo { Country = "United States", Continent = "NA", State = state },
        Contest = new ContestInfo { RcvdState = state },
    };

    [Fact]
    public void WveOperator_ScoresDxStation()
    {
        var eval = ContestScoringEngine.Evaluate(ArrlDxCw(), WveOp, Array.Empty<Qso>(), Dx("DL1ABC"));
        eval.Points.Should().Be(3);
        eval.Mults.Should().ContainSingle(); // one DXCC entity
    }

    [Fact]
    public void WveOperator_DoesNotScoreAnotherWveStation()
    {
        var eval = ContestScoringEngine.Evaluate(ArrlDxCw(), WveOp, Array.Empty<Qso>(), Usa("W1AW", "CT"));
        eval.Points.Should().Be(0);
        eval.Mults.Should().BeEmpty();
    }

    [Fact]
    public void DxOperator_ScoresWveStation_AndCountsStateMult()
    {
        var eval = ContestScoringEngine.Evaluate(ArrlDxCw(), DxOp, Array.Empty<Qso>(), Usa("W1AW", "CT"));
        eval.Points.Should().Be(3);
        eval.Mults.Should().ContainSingle(m => m.Contains("CT"));
    }

    [Fact]
    public void DxOperator_DoesNotScoreAnotherDxStation()
    {
        var eval = ContestScoringEngine.Evaluate(ArrlDxCw(), DxOp, Array.Empty<Qso>(), Dx("F5XYZ"));
        eval.Points.Should().Be(0);
    }

    // -- point/band corrections in the CQ/ARRL majors -----------------------

    private static ContestDefinition Def(string id) => SeedContests.All.First(d => d.Id == id);

    [Fact]
    public void CqWwRtty_HasFiveBands_No160()
    {
        Def("cq-ww-rtty").Bands.Should().NotContain("160M").And.HaveCount(5);
    }

    [Fact]
    public void Cq160_ScoresOwnCountryTwo_OtherContinentTen()
    {
        var pts = Def("cq-160-cw").QsoPoints;
        pts.SameCountry.Should().Be(2);
        pts.OtherContinent.Should().Be(10);
        pts.Default.Should().Be(5); // same continent, different country
    }

    [Fact]
    public void Arrl10m_ScoresCwDoublePhone()
    {
        var pts = Def("arrl-10m").QsoPoints;
        pts.ByMode!["PH"].Should().Be(2);
        pts.ByMode["CW"].Should().Be(4);
    }

    [Fact]
    public void FieldDay_ScoresCwAndDigitalDoublePhone()
    {
        var pts = Def("arrl-field-day").QsoPoints;
        pts.ByMode!["PH"].Should().Be(1);
        pts.ByMode["CW"].Should().Be(2);
        pts.ByMode["DIGI"].Should().Be(2);
    }
}
