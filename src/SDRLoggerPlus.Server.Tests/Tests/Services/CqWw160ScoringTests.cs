using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// CQ WW 160 multipliers are US states + VE provinces + DX countries. USA and Canada are the
/// state/province mults and must NOT also count as DX-country mults — the DxccExceptHome fix.
/// Regression guard for the ~1-3% claimed-score inflation the audit found.
/// </summary>
[Trait("Category", "Unit")]
public class CqWw160ScoringTests
{
    private static ContestDefinition Cq160Cw() => SeedContests.All.First(d => d.Id == "cq-160-cw");
    private static readonly MyExchange WveOp = new() { Country = "United States", Continent = "NA", State = "OH" };

    private static Qso Usa(string call, string state) => new()
    {
        Callsign = call, Band = "160M", Mode = "CW",
        Country = "United States", Continent = "NA",
        Station = new StationInfo { Country = "United States", Continent = "NA", State = state },
        Contest = new ContestInfo { RcvdState = state },
    };

    private static Qso Canada(string call, string prov) => new()
    {
        Callsign = call, Band = "160M", Mode = "CW",
        Country = "Canada", Continent = "NA",
        Station = new StationInfo { Country = "Canada", Continent = "NA", State = prov },
        Contest = new ContestInfo { RcvdState = prov },
    };

    private static Qso Dx(string call, string country) => new()
    {
        Callsign = call, Band = "160M", Mode = "CW",
        Country = country, Continent = "EU",
        Station = new StationInfo { Country = country, Continent = "EU", CqZone = 14 },
        Contest = new ContestInfo { RcvdZone = "14" },
    };

    [Fact]
    public void UsaStation_CountsExactlyOneMult_TheState_NotAlsoUsaAsACountry()
    {
        var eval = ContestScoringEngine.Evaluate(Cq160Cw(), WveOp, Array.Empty<Qso>(), Usa("W1AW", "CT"));
        // Only the state mult — NOT a second "USA" DXCC country mult (the old double-count).
        eval.Mults.Should().ContainSingle().Which.Should().Contain("CT");
        eval.Mults.Should().NotContain(m => m.Contains("United States") || m.Contains("DxccExceptHome"));
    }

    [Fact]
    public void CanadianStation_CountsExactlyOneMult_TheProvince_NotCanadaAsACountry()
    {
        var eval = ContestScoringEngine.Evaluate(Cq160Cw(), WveOp, Array.Empty<Qso>(), Canada("VE3ABC", "ON"));
        eval.Mults.Should().ContainSingle().Which.Should().Contain("ON");
        eval.Mults.Should().NotContain(m => m.Contains("Canada") || m.Contains("DxccExceptHome"));
    }

    [Fact]
    public void DxStation_StillCountsAsACountryMult()
    {
        var eval = ContestScoringEngine.Evaluate(Cq160Cw(), WveOp, Array.Empty<Qso>(), Dx("DL1ABC", "Germany"));
        eval.Mults.Should().ContainSingle(m => m.Contains("DxccExceptHome"));
    }

    [Fact]
    public void WorkingUsa_Canada_AndDx_YieldsThreeMults_NotFive()
    {
        var usa = Usa("W1AW", "CT");
        var can = Canada("VE3ABC", "ON");
        var dx = Dx("DL1ABC", "Germany");

        var m1 = ContestScoringEngine.Evaluate(Cq160Cw(), WveOp, Array.Empty<Qso>(), usa).Mults;
        var m2 = ContestScoringEngine.Evaluate(Cq160Cw(), WveOp, new[] { usa }, can).Mults;
        var m3 = ContestScoringEngine.Evaluate(Cq160Cw(), WveOp, new[] { usa, can }, dx).Mults;

        // CT + ON + Germany = 3. The old Dxcc rule would have added USA + Canada = 5.
        (m1.Count + m2.Count + m3.Count).Should().Be(3);
    }
}
