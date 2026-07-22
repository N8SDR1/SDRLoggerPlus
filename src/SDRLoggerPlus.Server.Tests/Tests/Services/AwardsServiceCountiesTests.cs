using FluentAssertions;
using MongoDB.Bson;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// USA-CA counting. Unlike the other ported awards this one gates on
/// confirmation, so worked and confirmed are asserted separately throughout.
/// </summary>
[Trait("Category", "Unit")]
public class AwardsServiceCountiesTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly AwardsService _service;

    public AwardsServiceCountiesTests() => _service = new AwardsService(_repo.Object);

    private void SetupQsos(params Qso[] qsos) => _repo.Setup(r => r.GetAllAsync()).ReturnsAsync(qsos);

    private static Qso Qso(string call, string? state, string? county = null, string? adifCnty = null,
        string? country = "United States", string band = "20m", string mode = "SSB",
        string? qslRcvd = null, string? eqslRcvd = null, string? lotwRcvd = null,
        string? propMode = null, DateTime? date = null)
    {
        var q = new Qso
        {
            Id = Guid.NewGuid().ToString(),
            Callsign = call,
            QsoDate = date ?? new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc),
            TimeOn = "1200",
            Band = band,
            Mode = mode,
            Country = country,
            Station = new StationInfo { State = state, County = county, Country = country },
            Qsl = new QslStatus
            {
                Rcvd = qslRcvd,
                Eqsl = eqslRcvd != null ? new EqslStatus { Rcvd = eqslRcvd } : null,
                Lotw = lotwRcvd != null ? new LotwStatus { Rcvd = lotwRcvd } : null,
            },
        };
        if (adifCnty != null || propMode != null)
        {
            q.AdifExtra = new BsonDocument();
            if (adifCnty != null) q.AdifExtra["cnty"] = adifCnty;
            if (propMode != null) q.AdifExtra["PROP_MODE"] = propMode;
        }
        return q;
    }

    [Fact]
    public async Task CountsADistinctCountyOncePerStateHoweverManyQsos()
    {
        SetupQsos(
            Qso("W1AW", "MN", county: "Hennepin"),
            Qso("W1AW", "MN", county: "Hennepin", band: "40m", mode: "CW"),
            Qso("K2XY", "MN", county: "Ramsey"));

        var stats = await _service.GetCountiesStatisticsAsync();

        stats.TotalWorked.Should().Be(2, "a county counts once regardless of band or mode");
        var mn = stats.States.Single(s => s.State == "MN");
        mn.Worked.Should().Be(2);
        mn.QsoCount.Should().Be(3);
    }

    [Fact]
    public async Task ReadsCountiesThatOnlyExistInTheAdifExtra()
    {
        // The real-world case: imported QSOs carry CNTY in AdifExtra because
        // ADIF import never mapped the field.
        SetupQsos(Qso("W1AW", "MN", adifCnty: "MN,Hennepin"));

        var stats = await _service.GetCountiesStatisticsAsync();

        stats.TotalWorked.Should().Be(1);
    }

    [Fact]
    public async Task ExcludesNonUsQsosEvenWhenACountyIsPopulated()
    {
        SetupQsos(Qso("G0ABC", "MN", county: "Hennepin", country: "England"));

        (await _service.GetCountiesStatisticsAsync()).TotalWorked.Should().Be(0);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ExcludesQsosWithNoCounty(string? county)
    {
        SetupQsos(Qso("W1AW", "MN", county: county));

        (await _service.GetCountiesStatisticsAsync()).TotalWorked.Should().Be(0);
    }

    [Fact]
    public async Task ExcludesSatelliteQsos()
    {
        SetupQsos(Qso("W1AW", "MN", county: "Hennepin", propMode: "SAT"));

        (await _service.GetCountiesStatisticsAsync()).TotalWorked.Should().Be(0);
    }

    [Fact]
    public async Task ConfirmsOnCardsAndEqslButNotLotw()
    {
        SetupQsos(
            Qso("W1AW", "MN", county: "Hennepin", qslRcvd: "Y"),
            Qso("K2XY", "MN", county: "Ramsey", eqslRcvd: "Y"),
            Qso("K3ZZ", "MN", county: "Anoka", lotwRcvd: "Y"));

        var stats = await _service.GetCountiesStatisticsAsync();

        stats.TotalWorked.Should().Be(3);
        stats.TotalConfirmed.Should().Be(2, "LoTW does not carry county reliably");
    }

    [Fact]
    public async Task KeepsACountyConfirmedWhenALaterQsoToItIsNot()
    {
        SetupQsos(
            Qso("W1AW", "MN", county: "Hennepin", qslRcvd: "Y"),
            Qso("W1AW", "MN", county: "Hennepin", band: "40m"));

        (await _service.GetCountiesStatisticsAsync()).TotalConfirmed.Should().Be(1);
    }

    [Fact]
    public async Task ReportsEveryStateWithItsTargetEvenWithNoQsos()
    {
        SetupQsos(Qso("W1AW", "MN", county: "Hennepin"));

        var stats = await _service.GetCountiesStatisticsAsync();

        stats.States.Should().HaveCount(50);
        var texas = stats.States.Single(s => s.State == "TX");
        texas.Worked.Should().Be(0);
        texas.Target.Should().Be(254, "the target comes from the reference data, not from the log");
        stats.TotalTarget.Should().BeInRange(3000, 3200);
    }

    [Fact]
    public async Task FoldsDifferentSpellingsOfOneCountyTogether()
    {
        SetupQsos(
            Qso("W1AW", "CA", county: "Los Angeles"),
            Qso("K2XY", "CA", county: "LOS ANGELES"),
            Qso("K3ZZ", "CA", adifCnty: "CA,Los Angeles County"));

        var stats = await _service.GetCountiesStatisticsAsync();

        stats.TotalWorked.Should().Be(1);
        stats.States.Single(s => s.State == "CA").QsoCount.Should().Be(3);
    }

    [Fact]
    public async Task DetailsListEveryCountyInTheStateSoTheGapsAreVisible()
    {
        SetupQsos(
            Qso("W1AW", "DE", county: "Kent", qslRcvd: "Y", date: new DateTime(2026, 3, 1)),
            Qso("W1AW", "DE", county: "Kent", date: new DateTime(2026, 5, 1)));

        var details = await _service.GetCountyDetailsAsync("de");

        details.Should().HaveCount(3, "Delaware has three counties, worked or not");
        var kent = details.Single(d => d.County == "Kent");
        kent.QsoCount.Should().Be(2);
        kent.Confirmed.Should().BeTrue();
        kent.FirstWorked.Should().Be(new DateTime(2026, 3, 1));
        kent.LastWorked.Should().Be(new DateTime(2026, 5, 1));
        details.Where(d => d.County != "Kent").Should().OnlyContain(d => d.QsoCount == 0 && !d.Confirmed);
    }

    [Fact]
    public async Task DetailsForAnUnknownStateAreEmptyRatherThanAnError()
    {
        SetupQsos(Qso("W1AW", "MN", county: "Hennepin"));

        (await _service.GetCountyDetailsAsync("ZZ")).Should().BeEmpty();
    }

    [Fact]
    public async Task BandFilterNarrowsTheCount()
    {
        SetupQsos(
            Qso("W1AW", "MN", county: "Hennepin", band: "20m"),
            Qso("K2XY", "MN", county: "Ramsey", band: "40m"));

        var stats = await _service.GetCountiesStatisticsAsync(
            new SDRLoggerPlus.Contracts.Api.StatisticsFilters(Band: "20m"));

        stats.TotalWorked.Should().Be(1);
    }
}
