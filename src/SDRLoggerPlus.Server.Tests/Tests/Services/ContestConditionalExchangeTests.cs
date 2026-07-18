using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Per-QSO exchange branching (RTTY Roundup / ARRL 10 m / ARRL 160 m): a W/VE
/// station sends a state (or section), a DX station sends a serial (or just RST).
/// The entry window switches fields from <see cref="ContestScoringEngine.ClassifyWorked"/>,
/// so these tests pin both the classification and that the seeded definitions tag
/// their conditional fields correctly. Field Day's power multiplier is verified here
/// too since it's the flagship power-multiplier case.
/// </summary>
[Trait("Category", "Unit")]
public class ContestConditionalExchangeTests
{
    private static ContestDefinition Def(string id) => SeedContests.All.First(d => d.Id == id);

    private static Qso Us(string call) => new()
    {
        Callsign = call, Band = "20M", Mode = "RTTY",
        Country = "United States", Continent = "NA",
        Station = new StationInfo { Country = "United States", Continent = "NA", State = "OH" },
    };

    private static Qso Dx(string call) => new()
    {
        Callsign = call, Band = "20M", Mode = "RTTY",
        Country = "Germany", Continent = "EU",
        Station = new StationInfo { Country = "Germany", Continent = "EU" },
    };

    [Theory]
    [InlineData("arrl-rtty-roundup")]
    [InlineData("arrl-10m")]
    [InlineData("arrl-160m")]
    public void WveContest_ClassifiesUsAsInArea_DxAsDx(string id)
    {
        var def = Def(id);
        ContestScoringEngine.ClassifyWorked(def, Us("W1AW")).Should().Be(ContestRole.InArea);
        ContestScoringEngine.ClassifyWorked(def, Dx("DL1ABC")).Should().Be(ContestRole.Dx);
    }

    [Fact]
    public void GlobalContest_HasNoHomeArea_ClassifiesAll()
    {
        // CQ WW has no W/VE split — every station is 'All' (no field branching).
        ContestScoringEngine.ClassifyWorked(Def("cq-ww-cw"), Dx("DL1ABC"))
            .Should().Be(ContestRole.All);
    }

    [Fact]
    public void RttyRoundup_BranchesStateForInArea_SerialForDx()
    {
        var rr = Def("arrl-rtty-roundup");
        rr.HomeArea!.Kind.Should().Be(HomeAreaKind.WVE);
        rr.RcvdExchange.Should().Contain(f =>
            f.Type == ContestFieldType.State && f.AppliesTo == ContestRole.InArea);
        rr.RcvdExchange.Should().Contain(f =>
            f.Type == ContestFieldType.Serial && f.AppliesTo == ContestRole.Dx);
    }

    [Fact]
    public void Arrl10m_BranchesStateForInArea_SerialForDx()
    {
        var t = Def("arrl-10m");
        t.HomeArea!.Kind.Should().Be(HomeAreaKind.WVE);
        t.RcvdExchange.Should().Contain(f =>
            f.Type == ContestFieldType.State && f.AppliesTo == ContestRole.InArea);
        t.RcvdExchange.Should().Contain(f =>
            f.Type == ContestFieldType.Serial && f.AppliesTo == ContestRole.Dx);
    }

    [Fact]
    public void Arrl160m_SectionForInArea_RstOnlyForDx()
    {
        var m = Def("arrl-160m");
        m.HomeArea!.Kind.Should().Be(HomeAreaKind.WVE);
        m.RcvdExchange.Should().Contain(f =>
            f.Type == ContestFieldType.Section && f.AppliesTo == ContestRole.InArea);
        // DX stations send only a signal report — no non-RST field applies to them.
        m.RcvdExchange.Where(f => f.Type != ContestFieldType.Rst)
            .Should().OnlyContain(f => f.AppliesTo == ContestRole.InArea);
    }

    // Branching is a UI concern; scoring reads whatever value lands on the QSO, so a
    // conditional-field contest still scores points/mults for both station classes.
    [Fact]
    public void RttyRoundup_ScoresDxStation_ViaDxccMult()
    {
        var rr = Def("arrl-rtty-roundup");
        var me = new MyExchange { Country = "United States", Continent = "NA", State = "OH" };
        var dx = Dx("DL1ABC");
        dx.Dxcc = 230; // Germany
        var eval = ContestScoringEngine.Evaluate(rr, me, Array.Empty<Qso>(), dx);
        eval.Points.Should().Be(1);
        eval.Mults.Should().Contain(m => m.StartsWith("Dxcc:"));
    }

    // -- Field Day power multiplier ----------------------------------------

    private static Qso Fd(string call) => new()
    {
        Callsign = call, Band = "20M", Mode = "CW",
        Contest = new ContestInfo { RcvdSection = "OH" },
    };

    [Fact]
    public void FieldDay_QrpMultipliesFinalScoreByFive()
    {
        var fd = Def("arrl-field-day");
        fd.PowerMultipliers.Should().ContainKey("QRP").WhoseValue.Should().Be(5);

        var qsos = new[] { Fd("W1AW"), Fd("K1ABC") }; // two CW QSOs = 2 pts each
        var qrp = ContestScoringEngine.Recompute(fd, new MyExchange { Power = "QRP" }, qsos);
        var high = ContestScoringEngine.Recompute(fd, new MyExchange { Power = "HIGH" }, qsos);
        var none = ContestScoringEngine.Recompute(fd, new MyExchange(), qsos);

        // No multiplier rules ⇒ score = points, then scaled by the power factor.
        qrp.Points.Should().Be(4);
        qrp.Score.Should().Be(20); // 4 × 5
        high.Score.Should().Be(4);  // 4 × 1
        none.Score.Should().Be(4);  // unlisted/blank class ⇒ ×1
    }
}
