using FluentAssertions;
using MongoDB.Bson;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Satellites;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Satellite stats and the VUCC Satellite award. The interesting cases are the
/// ones where satellite differs from every other award: LoTW counts here, a
/// satellite QSO is credited whatever band it was on, and a bird-less
/// PROP_MODE=SAT QSO still earns grid credit.
/// </summary>
[Trait("Category", "Unit")]
public class AwardsServiceSatellitesTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly AwardsService _service;

    public AwardsServiceSatellitesTests() => _service = new AwardsService(_repo.Object);

    private void SetupQsos(params Qso[] qsos) => _repo.Setup(r => r.GetAllAsync()).ReturnsAsync(qsos);

    private static Qso Qso(string call, string? satName, string? grid = "EN61",
        string? propMode = "SAT", string band = "70cm", string mode = "SSB",
        string? state = null, string? country = "United States",
        string? lotwRcvd = null, string? qslRcvd = null, DateTime? date = null)
    {
        var q = new Qso
        {
            Id = Guid.NewGuid().ToString(),
            Callsign = call,
            QsoDate = date ?? new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc),
            TimeOn = "1200",
            Band = band,
            Mode = mode,
            Grid = grid,
            Country = country,
            Station = new StationInfo { State = state, Country = country, Grid = grid },
            Qsl = new QslStatus
            {
                Rcvd = qslRcvd,
                Lotw = lotwRcvd != null ? new LotwStatus { Rcvd = lotwRcvd } : null,
            },
        };
        if (satName != null || propMode != null)
        {
            q.AdifExtra = new BsonDocument();
            if (satName != null) q.AdifExtra["SAT_NAME"] = satName;
            if (propMode != null) q.AdifExtra["PROP_MODE"] = propMode;
        }
        return q;
    }

    [Fact]
    public async Task GroupsQsosByBird()
    {
        SetupQsos(
            Qso("W1AW", "IO-117"),
            Qso("K2XY", "IO-117", grid: "FN31"),
            Qso("N0CALL", "AO-7", grid: "EM48"),
            Qso("W9ABC", null, propMode: null, grid: "EN52"));  // terrestrial

        var stats = await _service.GetSatelliteStatisticsAsync();

        stats.TotalSatellites.Should().Be(2);
        stats.TotalQsos.Should().Be(3, "the terrestrial QSO is not a satellite contact");
        stats.Satellites[0].Satellite.Should().Be("IO-117", "birds are ordered by QSO count");
        stats.Satellites[0].QsoCount.Should().Be(2);
        stats.Satellites[0].UniqueGrids.Should().Be(2);
    }

    [Fact]
    public async Task TreatsBirdNameCaseInsensitively()
    {
        // "io-117" and "IO-117" are one satellite, not two rows in the table.
        SetupQsos(
            Qso("W1AW", "IO-117"),
            Qso("K2XY", "io-117", grid: "FN31"));

        var stats = await _service.GetSatelliteStatisticsAsync();

        stats.TotalSatellites.Should().Be(1);
        stats.Satellites.Single().QsoCount.Should().Be(2);
    }

    [Fact]
    public async Task CountsAGridOnceHoweverManyQsosOrBirds()
    {
        SetupQsos(
            Qso("W1AW", "IO-117", grid: "EN61"),
            Qso("K2XY", "AO-7", grid: "EN61"),
            Qso("N0CALL", "IO-117", grid: "FN31"));

        var stats = await _service.GetSatelliteStatisticsAsync();

        stats.UniqueGrids.Should().Be(2, "VUCC counts distinct grids, not contacts");
        stats.VuccThreshold.Should().Be(100, "ARRL VUCC Satellite is 100 grids");
    }

    [Fact]
    public async Task AcceptsLotwAsConfirmation()
    {
        // Deliberately unlike USA-CA, which excludes LoTW because it does not
        // carry CNTY. LoTW does carry satellite credit and is the usual
        // confirmation path for VUCC Satellite.
        SetupQsos(Qso("W1AW", "IO-117", lotwRcvd: "Y"));

        var stats = await _service.GetSatelliteStatisticsAsync();

        stats.ConfirmedGrids.Should().Be(1);
        stats.Satellites.Single().ConfirmedQsos.Should().Be(1);
    }

    [Fact]
    public async Task AGridStaysConfirmedWhenALaterContactIsNot()
    {
        SetupQsos(
            Qso("W1AW", "IO-117", grid: "EN61", lotwRcvd: "Y"),
            Qso("K2XY", "IO-117", grid: "EN61"));

        var stats = await _service.GetSatelliteStatisticsAsync();

        stats.UniqueGrids.Should().Be(1);
        stats.ConfirmedGrids.Should().Be(1, "an unconfirmed later QSO does not take the credit away");
    }

    [Fact]
    public async Task CountsStatesAndEntitiesWorkedViaSatellite()
    {
        SetupQsos(
            Qso("W1AW", "IO-117", state: "MN"),
            Qso("K2XY", "IO-117", state: "MN", grid: "FN31"),
            Qso("VE3ABC", "IO-117", state: null, country: "Canada", grid: "FN03"),
            Qso("W9ABC", null, propMode: null, state: "TX", grid: "EM12"));  // terrestrial

        var stats = await _service.GetSatelliteStatisticsAsync();

        stats.UniqueStates.Should().Be(1, "TX was worked terrestrially, not via satellite");
        stats.UniqueEntities.Should().Be(2);
    }

    [Fact]
    public async Task CreditsASatelliteQsoThatNamesNoBird()
    {
        // PROP_MODE=SAT with no SAT_NAME: the grid still counts toward VUCC
        // Satellite, it just cannot say which bird carried it.
        SetupQsos(Qso("W1AW", satName: null, propMode: "SAT", grid: "EN61"));

        var stats = await _service.GetSatelliteStatisticsAsync();

        stats.UniqueGrids.Should().Be(1);
        stats.TotalSatellites.Should().Be(0);
        stats.TotalQsos.Should().Be(0, "TotalQsos sums the per-bird rows");
    }

    [Fact]
    public async Task VuccReportsSatelliteAsItsOwnCategory()
    {
        SetupQsos(
            Qso("W1AW", "IO-117", grid: "EN61", band: "70cm"),
            Qso("K2XY", "IO-117", grid: "FN31", band: "70cm"),
            Qso("N0CALL", null, propMode: null, grid: "EM48", band: "6m"));

        var stats = await _service.GetVuccStatisticsAsync();

        var sat = stats.BandSummaries["sat"];
        sat.UniqueGrids.Should().Be(2);
        sat.AwardThreshold.Should().Be(100);

        stats.BandSummaries["70cm"].UniqueGrids.Should()
            .Be(2, "a satellite QSO still counts on the band it was worked");
        stats.BandSummaries["6m"].UniqueGrids.Should().Be(1);
    }

    [Fact]
    public async Task VuccSatelliteCreditsBandsThatAreNotVuccBands()
    {
        // A satellite QSO on 10m earns satellite credit even though 10m is not
        // a VUCC band — the award is the satellite, not the band.
        SetupQsos(Qso("W1AW", "XW-2A", grid: "EN61", band: "10m"));

        var stats = await _service.GetVuccStatisticsAsync();

        stats.BandSummaries["sat"].UniqueGrids.Should().Be(1);
        stats.BandSummaries.Should().NotContainKey("10m");
    }

    [Fact]
    public async Task IgnoresSatelliteQsosWithoutAUsableGrid()
    {
        SetupQsos(
            Qso("W1AW", "IO-117", grid: null),
            Qso("K2XY", "IO-117", grid: "XX"));

        var stats = await _service.GetSatelliteStatisticsAsync();

        stats.UniqueGrids.Should().Be(0);
        stats.TotalQsos.Should().Be(2, "the QSOs still count as satellite contacts");
    }
}

/// <summary>
/// Reading satellite identity out of AdifExtra, where it actually lives.
/// </summary>
[Trait("Category", "Unit")]
public class SatelliteResolverTests
{
    private static Qso Qso(string? propMode, string? satName)
    {
        var q = new Qso { Callsign = "W1AW", Band = "70cm", Mode = "SSB" };
        if (propMode != null || satName != null)
        {
            q.AdifExtra = new BsonDocument();
            if (propMode != null) q.AdifExtra["PROP_MODE"] = propMode;
            if (satName != null) q.AdifExtra["SAT_NAME"] = satName;
        }
        return q;
    }

    [Fact]
    public void RecognisesPropModeSat()
    {
        SatelliteResolver.IsSatellite(Qso("SAT", null)).Should().BeTrue();
    }

    [Fact]
    public void RecognisesALowerCasedFieldName()
    {
        // Imported rows carry ADIF names lower-cased; the spec says field names
        // are case-insensitive, so an exact-match lookup would find nothing.
        var q = new Qso { Callsign = "W1AW", Band = "70cm", Mode = "SSB", AdifExtra = new BsonDocument() };
        q.AdifExtra["prop_mode"] = "sat";
        q.AdifExtra["sat_name"] = "IO-117";

        SatelliteResolver.IsSatellite(q).Should().BeTrue();
        SatelliteResolver.Name(q).Should().Be("IO-117");
    }

    [Fact]
    public void ANamedBirdIsASatelliteQsoEvenWithoutPropMode()
    {
        SatelliteResolver.IsSatellite(Qso(null, "SO-50")).Should().BeTrue();
    }

    [Fact]
    public void DoesNotMistakeATerrestrialQsoForASatelliteOne()
    {
        SatelliteResolver.IsSatellite(Qso(null, null)).Should().BeFalse();
        SatelliteResolver.IsSatellite(Qso("F2", null)).Should().BeFalse();
    }

    [Fact]
    public void UppercasesTheBirdName()
    {
        SatelliteResolver.Name(Qso("SAT", "io-117")).Should().Be("IO-117");
    }

    [Fact]
    public void TreatsABlankSatNameAsNoBird()
    {
        SatelliteResolver.Name(Qso("SAT", "   ")).Should().BeNull();
    }
}
