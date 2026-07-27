using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// NAQP and NA Sprint multipliers are US states + VE provinces + North American countries. USA
/// and Canada must NOT also count as country mults (they're the states/provinces), and a non-NA
/// (European/Asian) DX contact must count for QSO points only — never a multiplier. Regression
/// guard for the blanket-Dxcc over-count replaced by MultSource.NaCountryExceptHome.
/// </summary>
[Trait("Category", "Unit")]
public class NaqpSprintScoringTests
{
    private static ContestDefinition Naqp() => SeedContests.All.First(d => d.Id == "naqp-cw");
    private static ContestDefinition Sprint() => SeedContests.All.First(d => d.Id == "na-sprint-cw");
    private static readonly MyExchange UsOp = new() { Country = "United States", Continent = "NA", State = "OH", Name = "RICK" };

    private static Qso Usa(string call, string state) => new()
    {
        Callsign = call, Band = "20M", Mode = "CW",
        Country = "United States", Continent = "NA",
        Station = new StationInfo { Country = "United States", Continent = "NA", State = state },
        Contest = new ContestInfo { RcvdState = state, RcvdName = "OP", SerialRcvd = "1" },
    };

    private static Qso Canada(string call, string prov) => new()
    {
        Callsign = call, Band = "20M", Mode = "CW",
        Country = "Canada", Continent = "NA",
        Station = new StationInfo { Country = "Canada", Continent = "NA", State = prov },
        Contest = new ContestInfo { RcvdState = prov, RcvdName = "OP", SerialRcvd = "1" },
    };

    // A North American country that is neither USA nor Canada — the country IS the mult.
    private static Qso Mexico(string call) => new()
    {
        Callsign = call, Band = "20M", Mode = "CW",
        Country = "Mexico", Continent = "NA",
        Station = new StationInfo { Country = "Mexico", Continent = "NA" },
        Contest = new ContestInfo { RcvdName = "OP", SerialRcvd = "1" },
    };

    // A non-NA DX station — QSO points only, no multiplier of any kind.
    private static Qso Dx(string call) => new()
    {
        Callsign = call, Band = "20M", Mode = "CW",
        Country = "Germany", Continent = "EU",
        Station = new StationInfo { Country = "Germany", Continent = "EU" },
        Contest = new ContestInfo { RcvdName = "OP", SerialRcvd = "1" },
    };

    [Theory]
    [InlineData("naqp-cw")]
    [InlineData("na-sprint-cw")]
    public void UsaStation_CountsExactlyOneMult_TheState_NotAlsoUsaCountry(string id)
    {
        var def = SeedContests.All.First(d => d.Id == id);
        var eval = ContestScoringEngine.Evaluate(def, UsOp, Array.Empty<Qso>(), Usa("W1AW", "CT"));
        eval.Mults.Should().ContainSingle().Which.Should().Contain("CT");
        eval.Mults.Should().NotContain(m => m.Contains("United States"));
    }

    [Theory]
    [InlineData("naqp-cw")]
    [InlineData("na-sprint-cw")]
    public void MexicanStation_CountsAsOneNaCountryMult(string id)
    {
        var def = SeedContests.All.First(d => d.Id == id);
        var eval = ContestScoringEngine.Evaluate(def, UsOp, Array.Empty<Qso>(), Mexico("XE1ABC"));
        eval.Mults.Should().ContainSingle(); // Mexico, the country
    }

    [Theory]
    [InlineData("naqp-cw")]
    [InlineData("na-sprint-cw")]
    public void NonNaDxStation_ScoresPointsButNoMultiplier(string id)
    {
        var def = SeedContests.All.First(d => d.Id == id);
        var eval = ContestScoringEngine.Evaluate(def, UsOp, Array.Empty<Qso>(), Dx("DL1ABC"));
        eval.Mults.Should().BeEmpty();       // Europe = no NAQP/Sprint multiplier
        eval.Points.Should().BeGreaterThan(0); // …but the QSO still counts for points
    }

    [Theory]
    [InlineData("naqp-cw")]
    [InlineData("na-sprint-cw")]
    public void UsaCanadaMexicoAndDx_YieldThreeMults_NotFour(string id)
    {
        var def = SeedContests.All.First(d => d.Id == id);
        var usa = Usa("W1AW", "CT");
        var can = Canada("VE3ABC", "ON");
        var mex = Mexico("XE1ABC");
        var dx = Dx("DL1ABC");

        var m1 = ContestScoringEngine.Evaluate(def, UsOp, Array.Empty<Qso>(), usa).Mults;
        var m2 = ContestScoringEngine.Evaluate(def, UsOp, new[] { usa }, can).Mults;
        var m3 = ContestScoringEngine.Evaluate(def, UsOp, new[] { usa, can }, mex).Mults;
        var m4 = ContestScoringEngine.Evaluate(def, UsOp, new[] { usa, can, mex }, dx).Mults;

        // CT + ON + Mexico = 3; Germany adds none. Old blanket Dxcc would have given 4+.
        (m1.Count + m2.Count + m3.Count + m4.Count).Should().Be(3);
    }
}
