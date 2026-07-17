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
        string? section = null, string? state = null)
    {
        return new Qso
        {
            Callsign = call,
            Band = band,
            Mode = mode,
            Dxcc = dxcc,
            Continent = cont,
            Station = new StationInfo { CqZone = cqZone, State = state },
            Contest = new ContestInfo { RcvdSection = section },
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
}
