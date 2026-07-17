using FluentAssertions;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Guards the per-contest encoding of the state QSO parties (sub-project C):
/// exact bands/modes, the role split, and per-mode points, so the entry form and
/// scoring engine honor each sponsor's rules.
/// </summary>
[Trait("Category", "Unit")]
public class SeedContestsQpTests
{
    private static ContestDefinition Def(string id) =>
        SeedContests.All.First(d => d.Id == id);

    private static readonly string[] WarcBands = { "30M", "17M", "12M", "60M" };

    [Fact]
    public void NoSeedContest_OffersWarcBands()
    {
        foreach (var def in SeedContests.All)
            def.Bands.Should().NotContain(WarcBands, $"'{def.Id}' must not allow WARC/60m bands");
    }

    [Fact]
    public void Wisconsin_IsCwSsbDigital_NoFt8()
    {
        var wi = Def("qp-wisconsin");
        wi.Modes.Should().BeEquivalentTo(new[] { "CW", "SSB", "RTTY" });
        wi.Modes.Should().NotContain("FT8").And.NotContain("FT4");
        wi.Bands.Should().Contain("160M");
    }

    [Fact]
    public void Florida_HasOnlyFourBands()
    {
        Def("qp-florida").Bands.Should().BeEquivalentTo(new[] { "40M", "20M", "15M", "10M" });
    }

    [Fact]
    public void Hawaii_AllowsFt8()
    {
        Def("qp-hawaii").Modes.Should().Contain("FT8").And.Contain("FT4");
    }

    [Fact]
    public void California_UsesSerialAllBand()
    {
        var ca = Def("qp-california");
        ca.Serial.Should().Be(SerialMode.AllBand);
        ca.SentExchange.Should().Contain(f => f.Type == ContestFieldType.Serial);
    }

    [Fact]
    public void Ohio_HasHomeAreaAndRoleSplit()
    {
        var oh = Def("qp-ohio");
        oh.HomeArea!.Kind.Should().Be(HomeAreaKind.StateCounty);
        oh.HomeArea.States.Should().Equal("OH");

        oh.Roles.Should().ContainKeys(ContestRole.InArea, ContestRole.OutArea);
        // In-area operator sends its county and works everyone.
        var inArea = oh.Roles![ContestRole.InArea];
        inArea.SentExchange.Should().Contain(f => f.Key == "county");
        inArea.WorksForPoints.Should().Be(WorkTarget.Everyone);
        // Out-of-area operator sends its state and works only in-area stations.
        var outArea = oh.Roles[ContestRole.OutArea];
        outArea.SentExchange.Should().Contain(f => f.Type == ContestFieldType.State);
        outArea.WorksForPoints.Should().Be(WorkTarget.InAreaOnly);
    }

    [Fact]
    public void Ohio_ScoresCwDoublePhone()
    {
        var pts = Def("qp-ohio").QsoPoints;
        pts.ByMode!["CW"].Should().Be(2);
        pts.ByMode["PH"].Should().Be(1);
    }

    [Fact]
    public void MarylandDc_HomeAreaCoversBothMdAndDc()
    {
        Def("qp-maryland-dc").HomeArea!.States.Should().BeEquivalentTo(new[] { "MD", "DC" });
    }

    [Fact]
    public void Regionals_HaveMultiStateHomeArea()
    {
        Def("qp-7qp").HomeArea!.States.Should().Contain(new[] { "WA", "OR", "ID", "MT", "WY", "NV", "UT" });
        Def("qp-neqp").HomeArea!.States.Should().Contain(new[] { "CT", "ME", "MA", "NH", "RI", "VT" });
    }
}
