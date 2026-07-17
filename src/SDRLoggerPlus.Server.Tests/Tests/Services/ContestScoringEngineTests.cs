using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class ContestScoringEngineTests
{
    private static Qso MakeQso(
        string call, string band = "20M", string mode = "CW",
        int? dxcc = null, string? cont = null, int? cqZone = null,
        string? section = null, string? state = null,
        string? rcvdState = null, string? country = null)
    {
        return new Qso
        {
            Callsign = call,
            Band = band,
            Mode = mode,
            Dxcc = dxcc,
            Continent = cont,
            Country = country,
            Station = new StationInfo { CqZone = cqZone, State = state },
            Contest = new ContestInfo { RcvdSection = section, RcvdState = rcvdState },
        };
    }

    private static readonly MyExchange Me = new()
    {
        Dxcc = 291, // USA
        Continent = "NA",
        CqZone = 5,
    };

    // ---- Dupe rules -------------------------------------------------------

    [Fact]
    public void Dupe_PerBand_SameCallSameBand_IsDupe()
    {
        var def = new ContestDefinition { DupeRule = DupeRule.PerBand };
        var prior = new[] { MakeQso("N8SDR", band: "20M") };

        var eval = ContestScoringEngine.Evaluate(def, Me, prior, MakeQso("N8SDR", band: "20M"));

        eval.IsDupe.Should().BeTrue();
        eval.Points.Should().Be(0);
    }

    [Fact]
    public void Dupe_PerBand_SameCallDifferentBand_NotDupe()
    {
        var def = new ContestDefinition { DupeRule = DupeRule.PerBand };
        var prior = new[] { MakeQso("N8SDR", band: "20M") };

        var eval = ContestScoringEngine.Evaluate(def, Me, prior, MakeQso("N8SDR", band: "40M"));

        eval.IsDupe.Should().BeFalse();
    }

    [Fact]
    public void Dupe_PerBandMode_SameBandDifferentMode_NotDupe()
    {
        var def = new ContestDefinition { DupeRule = DupeRule.PerBandMode };
        var prior = new[] { MakeQso("N8SDR", band: "20M", mode: "CW") };

        var eval = ContestScoringEngine.Evaluate(def, Me, prior, MakeQso("N8SDR", band: "20M", mode: "SSB"));

        eval.IsDupe.Should().BeFalse();
    }

    [Fact]
    public void Dupe_PerContest_SameCallAnyBand_IsDupe()
    {
        var def = new ContestDefinition { DupeRule = DupeRule.PerContest };
        var prior = new[] { MakeQso("N8SDR", band: "20M") };

        var eval = ContestScoringEngine.Evaluate(def, Me, prior, MakeQso("N8SDR", band: "40M"));

        eval.IsDupe.Should().BeTrue();
    }

    // ---- Points by relation ----------------------------------------------

    private static ContestDefinition CqWwLikePoints() => new()
    {
        DupeRule = DupeRule.PerBandMode,
        QsoPoints = new PointsRule { SameCountry = 0, SameContinent = 1, OtherContinent = 3, Default = 1 },
    };

    [Fact]
    public void Points_SameCountry_UsesSameCountryValue()
    {
        var def = CqWwLikePoints();
        // Worked USA station (same DXCC as Me)
        var eval = ContestScoringEngine.Evaluate(def, Me, Array.Empty<Qso>(),
            MakeQso("W1AW", dxcc: 291, cont: "NA"));

        eval.Points.Should().Be(0);
    }

    [Fact]
    public void Points_SameContinentDifferentCountry_UsesContinentValue()
    {
        var def = CqWwLikePoints();
        // Worked Canada (NA, different DXCC)
        var eval = ContestScoringEngine.Evaluate(def, Me, Array.Empty<Qso>(),
            MakeQso("VE3XYZ", dxcc: 1, cont: "NA"));

        eval.Points.Should().Be(1);
    }

    [Fact]
    public void Points_OtherContinent_UsesOtherContinentValue()
    {
        var def = CqWwLikePoints();
        // Worked Germany (EU)
        var eval = ContestScoringEngine.Evaluate(def, Me, Array.Empty<Qso>(),
            MakeQso("DL1ABC", dxcc: 230, cont: "EU"));

        eval.Points.Should().Be(3);
    }

    // ---- Multipliers ------------------------------------------------------

    [Fact]
    public void Mult_WpxPrefix_DistinctPrefixesEachCount()
    {
        var def = new ContestDefinition
        {
            DupeRule = DupeRule.PerContest,
            MultiplierRules = { new MultRule { Source = MultSource.WpxPrefix } },
        };
        var summary = ContestScoringEngine.Recompute(def, Me, new[]
        {
            MakeQso("K1AA"),   // K1
            MakeQso("K2BB"),   // K2
            MakeQso("K1CC"),   // K1 again -> no new mult
        });

        summary.Multipliers.Should().Be(2);
    }

    [Fact]
    public void Mult_CqZonePerBand_SameZoneCountsOncePerBand()
    {
        var def = new ContestDefinition
        {
            DupeRule = DupeRule.PerBandMode,
            MultiplierRules = { new MultRule { Source = MultSource.CqZone, PerBand = true } },
        };
        var summary = ContestScoringEngine.Recompute(def, Me, new[]
        {
            MakeQso("DL1A", band: "20M", cqZone: 14),
            MakeQso("DL2B", band: "40M", cqZone: 14), // same zone, different band -> new mult
            MakeQso("DL3C", band: "20M", cqZone: 14), // same zone+band -> no new mult
        });

        summary.Multipliers.Should().Be(2);
    }

    // ---- Recompute totals -------------------------------------------------

    [Fact]
    public void EvaluateBatch_FlagsDupeAndNewMult_AgainstPriorLog()
    {
        var def = new ContestDefinition
        {
            DupeRule = DupeRule.PerBandMode,
            MultiplierRules = { new MultRule { Source = MultSource.WpxPrefix } },
        };
        var prior = new[] { MakeQso("K1AA", band: "20M") }; // K1 prefix + K1AA worked on 20M/CW

        var results = ContestScoringEngine.EvaluateBatch(def, Me, prior, new[]
        {
            MakeQso("K1AA", band: "20M"),  // [0] dupe
            MakeQso("K1BB", band: "20M"),  // [1] K1 already a mult -> not new mult, not dupe
            MakeQso("W7XX", band: "20M"),  // [2] new prefix -> new mult
        });

        results[0].IsDupe.Should().BeTrue();       // K1AA
        results[1].IsDupe.Should().BeFalse();      // K1BB
        results[1].Mults.Should().BeEmpty();
        results[2].Mults.Should().NotBeEmpty();    // W7XX
    }

    [Fact]
    public void Recompute_ExcludesDupes_AndScoreIsPointsTimesMults()
    {
        var def = new ContestDefinition
        {
            DupeRule = DupeRule.PerBandMode,
            QsoPoints = new PointsRule { OtherContinent = 3, SameContinent = 1, SameCountry = 0, Default = 1 },
            MultiplierRules = { new MultRule { Source = MultSource.Dxcc, PerBand = true } },
        };
        var summary = ContestScoringEngine.Recompute(def, Me, new[]
        {
            MakeQso("DL1A", band: "20M", dxcc: 230, cont: "EU"), // 3 pts, mult DL@20
            MakeQso("G3B",  band: "20M", dxcc: 223, cont: "EU"), // 3 pts, mult G@20
            MakeQso("DL1A", band: "20M", dxcc: 230, cont: "EU"), // dupe -> 0, no mult
        });

        summary.Qsos.Should().Be(2);
        summary.Dupes.Should().Be(1);
        summary.Points.Should().Be(6);
        summary.Multipliers.Should().Be(2);
        summary.Score.Should().Be(12);
    }

    // ---- roles: in-state / out-of-state (QSO parties) ---------------------

    // An Ohio-QSO-Party-shaped definition: in-state ops work everyone and count
    // state/prov + DXCC mults; out-of-state ops work only Ohio stations and count
    // Ohio county mults.
    private static ContestDefinition OhioQsoParty() => new()
    {
        DupeRule = DupeRule.PerBandMode,
        HomeArea = new HomeArea { Kind = HomeAreaKind.StateCounty, States = { "OH" } },
        QsoPoints = new PointsRule { Default = 2 },
        MultiplierRules = { new MultRule { Source = MultSource.State } },
        Roles = new()
        {
            [ContestRole.InArea] = new RoleRules
            {
                WorksForPoints = WorkTarget.Everyone,
                MultiplierRules = new()
                {
                    new MultRule { Source = MultSource.State },
                    new MultRule { Source = MultSource.Dxcc },
                },
            },
            [ContestRole.OutArea] = new RoleRules
            {
                WorksForPoints = WorkTarget.InAreaOnly,
                MultiplierRules = new() { new MultRule { Source = MultSource.State } },
            },
        },
    };

    private static readonly MyExchange OhioOp = new() { Country = "United States", Continent = "NA", State = "OH" };
    private static readonly MyExchange CalifOp = new() { Country = "United States", Continent = "NA", State = "CA" };

    [Fact]
    public void DetermineRole_NoHomeArea_IsAll()
    {
        var def = new ContestDefinition();
        ContestScoringEngine.DetermineRole(def, OhioOp).Should().Be(ContestRole.All);
    }

    [Fact]
    public void DetermineRole_OperatorInHomeState_IsInArea()
    {
        ContestScoringEngine.DetermineRole(OhioQsoParty(), OhioOp).Should().Be(ContestRole.InArea);
    }

    [Fact]
    public void DetermineRole_OperatorOutsideHomeState_IsOutArea()
    {
        ContestScoringEngine.DetermineRole(OhioQsoParty(), CalifOp).Should().Be(ContestRole.OutArea);
    }

    [Fact]
    public void OutOfStateOp_ScoresInStateStation_ButNotAnotherOutOfStateStation()
    {
        var def = OhioQsoParty();

        // Working an Ohio station (sends a county code) counts; mult is the county.
        var ohio = ContestScoringEngine.Evaluate(def, CalifOp, Array.Empty<Qso>(),
            MakeQso("W8XYZ", rcvdState: "FRA", country: "United States"));
        ohio.Points.Should().Be(2);
        ohio.Mults.Should().Contain("State:FRA");

        // Working another out-of-state station (sends a 2-letter S/P) scores nothing.
        var texas = ContestScoringEngine.Evaluate(def, CalifOp, Array.Empty<Qso>(),
            MakeQso("K5AAA", rcvdState: "TX", country: "United States"));
        texas.Points.Should().Be(0);
        texas.Mults.Should().BeEmpty();
    }

    [Fact]
    public void InStateOp_ScoresEveryone_AndCountsStateAndDxccMults()
    {
        var def = OhioQsoParty();
        var summary = ContestScoringEngine.Recompute(def, OhioOp, new[]
        {
            MakeQso("W8AAA", rcvdState: "ALL", country: "United States"),         // OH county
            MakeQso("K5BBB", rcvdState: "TX", country: "United States"),          // out-of-state S/P
            MakeQso("DL1CCC", rcvdState: "DX", dxcc: 230, cont: "EU", country: "Germany"), // DX
        });

        summary.Qsos.Should().Be(3);   // in-state op works everyone
        summary.Points.Should().Be(6); // 3 QSOs * 2 pts
        // State mults from every worked station's location; DXCC mults per country.
        summary.MultsBySource["State"].Should().BeEquivalentTo("ALL", "TX", "DX");
        summary.MultsBySource["Dxcc"].Should().Contain("230"); // Germany counts as a DXCC mult
    }

    [Fact]
    public void ByMode_Points_UseModeClass()
    {
        var def = new ContestDefinition
        {
            QsoPoints = new PointsRule { Default = 1, ByMode = new() { ["CW"] = 2, ["PH"] = 1 } },
        };

        ContestScoringEngine.Evaluate(def, Me, Array.Empty<Qso>(), MakeQso("N8SDR", mode: "CW"))
            .Points.Should().Be(2);
        ContestScoringEngine.Evaluate(def, Me, Array.Empty<Qso>(), MakeQso("N8SDR", mode: "SSB"))
            .Points.Should().Be(1);
    }
}
