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

    // ---- band-weighted points + NA exception (audit #23) — real seed defs ---

    private static ContestDefinition Seed(string id) =>
        System.Linq.Enumerable.First(SeedContests.All, d => d.Id == id);

    private static readonly MyExchange UsOp = new()
    { Dxcc = 291, Continent = "NA", CqZone = 5, Country = "United States" };

    private static Qso Worked(string call, string band, int? dxcc, string cont, string country) =>
        new()
        {
            Callsign = call, Band = band, Mode = "CW", Dxcc = dxcc, Continent = cont, Country = country,
            Station = new StationInfo(), Contest = new ContestInfo(),
        };

    private static int Pt(string id, Qso q) =>
        ContestScoringEngine.Evaluate(Seed(id), UsOp, Array.Empty<Qso>(), q).Points;

    [Fact]
    public void CqWwDx_NorthAmericaExceptionCountsTwo()
    {
        Pt("cq-ww-cw", Worked("VE3X", "20M", 1, "NA", "Canada")).Should().Be(2);        // NA↔NA
        Pt("cq-ww-cw", Worked("DL1A", "20M", 230, "EU", "Germany")).Should().Be(3);     // diff continent
        Pt("cq-ww-cw", Worked("W1AW", "20M", 291, "NA", "United States")).Should().Be(0); // same country
    }

    [Fact]
    public void CqWpx_LowBandsDouble_AndNaException_ButNotSameCountry()
    {
        Pt("cq-wpx-cw", Worked("DL1A", "20M", 230, "EU", "Germany")).Should().Be(3);   // DX high
        Pt("cq-wpx-cw", Worked("DL1A", "40M", 230, "EU", "Germany")).Should().Be(6);   // DX low → ×2
        Pt("cq-wpx-cw", Worked("VE3X", "20M", 1, "NA", "Canada")).Should().Be(2);      // NA high
        Pt("cq-wpx-cw", Worked("VE3X", "40M", 1, "NA", "Canada")).Should().Be(4);      // NA low → ×2
        Pt("cq-wpx-cw", Worked("W1AW", "40M", 291, "NA", "United States")).Should().Be(1); // same country flat
    }

    [Fact]
    public void Vhf_PerBandPoints()
    {
        Pt("cq-vhf", Worked("W9X", "6M", 291, "NA", "United States")).Should().Be(1);
        Pt("cq-vhf", Worked("W9X", "2M", 291, "NA", "United States")).Should().Be(2);
        Pt("arrl-vhf", Worked("W9X", "6M", 291, "NA", "United States")).Should().Be(1);
        Pt("arrl-vhf", Worked("W9X", "70CM", 291, "NA", "United States")).Should().Be(2);
    }

    [Fact]
    public void Arrl160m_AnyDxCountsFive_NotJustOtherContinent()
    {
        Pt("arrl-160m", Worked("XE1X", "160M", 50, "NA", "Mexico")).Should().Be(5);       // same-continent DX
        Pt("arrl-160m", Worked("DL1A", "160M", 230, "EU", "Germany")).Should().Be(5);     // trans-Atlantic DX
        Pt("arrl-160m", Worked("W1AW", "160M", 291, "NA", "United States")).Should().Be(2); // W/VE
        Pt("arrl-160m", Worked("VE3X", "160M", 1, "NA", "Canada")).Should().Be(2);          // VE = home area
    }

    [Fact]
    public void TenTen_MemberScoresTwo_NonMemberOne()
    {
        Qso WithNr(string? nr)
        {
            var q = Worked("W5X", "10M", 291, "NA", "United States");
            q.Contest = new ContestInfo { RcvdFields = new() { ["nr"] = nr ?? "" } };
            return q;
        }
        Pt("ten-ten", WithNr("5678")).Should().Be(2); // member (non-zero 10-10 number)
        Pt("ten-ten", WithNr("0")).Should().Be(1);    // non-member logs 0
        Pt("ten-ten", WithNr("")).Should().Be(1);     // no number given
    }

    [Fact]
    public void Distance_ScalesWithGridDistance()
    {
        var op = new MyExchange { Grid = "EN80", Continent = "NA", Country = "United States", Dxcc = 291 };
        int Pts160(string grid) => ContestScoringEngine.Evaluate(
            Seed("stew-perry"), op, Array.Empty<Qso>(),
            new Qso
            {
                Callsign = "W7X", Band = "160M", Mode = "CW",
                Station = new StationInfo(), Contest = new ContestInfo { RcvdGrid = grid },
            }).Points;

        Pts160("EN80").Should().Be(1);                          // same grid → minimum 1
        Pts160("DM43").Should().BeGreaterThan(3);               // ~2600 km → several points
        Pts160("JO31").Should().BeGreaterThan(Pts160("DM43"));  // Germany, farther → more
    }

    [Fact]
    public void WinterFieldDay_BandModeMults_PowerFactor_AndDeclaredBonus()
    {
        var op = new MyExchange { Country = "United States", Continent = "NA", Power = "QRP", BonusPoints = 1500 };
        Qso Q(string call, string band, string mode) => new()
        {
            Callsign = call, Band = band, Mode = mode,
            Station = new StationInfo(), Contest = new ContestInfo(),
        };

        var summary = ContestScoringEngine.Recompute(Seed("winter-field-day"), op, new[]
        {
            Q("W1A", "40M", "CW"),    // 2 pts, mult CW@40
            Q("W2B", "20M", "CW"),    // 2 pts, mult CW@20
            Q("W3C", "20M", "SSB"),   // 1 pt,  mult PH@20
            Q("W4D", "20M", "USB"),   // 1 pt,  PH@20 already claimed (USB/LSB = one Phone mult)
            Q("W5E", "20M", "FT8"),   // 2 pts, mult DIGI@20
            Q("W6F", "20M", "RTTY"),  // 2 pts, DIGI@20 already claimed (RTTY folds into Digital)
        });

        summary.Qsos.Should().Be(6);
        summary.Points.Should().Be(2 + 2 + 1 + 1 + 2 + 2);         // 10
        summary.Multipliers.Should().Be(4);                        // CW@40, CW@20, PH@20, DIGI@20
        summary.BonusPoints.Should().Be(1500);
        // (points × mults) × QRP(×4) + declared bonus = (10 × 4) × 4 + 1500
        summary.Score.Should().Be(10 * 4 * 4 + 1500);
    }

    [Fact]
    public void WinterFieldDay_LowPower_NoBonusEnteredScoresPointsTimesMults()
    {
        var op = new MyExchange { Country = "United States", Continent = "NA", Power = "LOW" };
        Qso Q(string call, string band, string mode) => new()
        {
            Callsign = call, Band = band, Mode = mode,
            Station = new StationInfo(), Contest = new ContestInfo(),
        };

        var summary = ContestScoringEngine.Recompute(Seed("winter-field-day"), op, new[]
        {
            Q("W1A", "40M", "CW"),   // 2 pts, CW@40
            Q("W2B", "40M", "SSB"),  // 1 pt,  PH@40
        });

        summary.Points.Should().Be(3);
        summary.Multipliers.Should().Be(2);
        summary.BonusPoints.Should().Be(0);
        summary.Score.Should().Be(3 * 2 * 2);   // × LOW(×2)
    }

    [Fact]
    public void Arrl10m_MultipliersCountOncePerMode()
    {
        var op = new MyExchange { Country = "United States", Continent = "NA", State = "OH", Dxcc = 291 };
        // DX (Germany) so only the DXCC country multiplier applies — isolates the
        // per-mode behaviour from the state multiplier.
        Qso Q(string call, string mode) => new()
        {
            Callsign = call, Band = "10M", Mode = mode,
            Dxcc = 230, Continent = "EU", Country = "Germany",
            Station = new StationInfo(), Contest = new ContestInfo(),
        };

        // Same country (DL) on both CW and phone → two multipliers, not one; USB and
        // LSB collapse to a single Phone mult.
        var summary = ContestScoringEngine.Recompute(Seed("arrl-10m"), op, new[]
        {
            Q("DL1A", "CW"),    // CW pt 4, mult Dxcc:230+CW
            Q("DL2B", "USB"),   // phone pt 2, mult Dxcc:230+PH
            Q("DL3C", "LSB"),   // phone pt 2, Dxcc:230+PH already claimed
        });

        summary.Points.Should().Be(4 + 2 + 2);   // 8
        summary.Multipliers.Should().Be(2);       // DL on CW + DL on phone
    }

    [Fact]
    public void WinterFieldDay_DeclaredBonusIgnoredWithNoQsos()
    {
        var op = new MyExchange { Country = "United States", Continent = "NA", Power = "LOW", BonusPoints = 1500 };
        var summary = ContestScoringEngine.Recompute(Seed("winter-field-day"), op, Array.Empty<Qso>());
        summary.BonusPoints.Should().Be(0);   // no valid QSO → no bonus credited
        summary.Score.Should().Be(0);
    }
}
