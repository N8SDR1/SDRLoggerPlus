using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Log Entry DXCC glance. In-memory QSOs only — no QRZ and no HamQTH.
/// </summary>
[Trait("Category", "Unit")]
public class AwardsServiceDxccGlanceTests
{
    private static readonly string[] AllBands =
    [
        "2200m", "630m", "160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m",
        "6m", "2m", "1.25m", "70cm", "33cm", "23cm",
    ];

    private readonly Mock<IQsoRepository> _repo = new();
    private readonly AwardsService _service;

    public AwardsServiceDxccGlanceTests()
    {
        _repo.Setup(r => r.GetAllAsync()).ReturnsAsync(Array.Empty<Qso>());
        _service = new AwardsService(_repo.Object);
    }

    private void Setup(params Qso[] qsos) =>
        _repo.Setup(r => r.GetAllAsync()).ReturnsAsync(qsos);

    private static Qso Qso(
        string call,
        string band = "20m",
        string mode = "USB",
        string? country = "United States",
        Action<Qso>? tweak = null)
    {
        var qso = new Qso
        {
            Id = Guid.NewGuid().ToString(),
            Callsign = call,
            QsoDate = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc),
            TimeOn = "120000",
            Band = band,
            Mode = mode,
            Country = country,
            Station = country is null ? new StationInfo() : new StationInfo { Country = country },
        };
        tweak?.Invoke(qso);
        return qso;
    }

    private static DxccGlanceCell At(DxccEntityGlance glance, string group, string band)
    {
        var row = glance.Rows.Single(r => r.ModeGroup == group);
        var index = glance.Bands.ToList().IndexOf(band);
        index.Should().BeGreaterOrEqualTo(0, $"band {band} should be a column");
        return row.Cells[index];
    }

    private static void ShouldBeEmpty(DxccGlanceCell cell)
    {
        cell.Worked.Should().BeFalse();
        cell.Confirmed.Should().BeFalse();
        cell.QsoCount.Should().Be(0);
        cell.CallsignWorked.Should().BeFalse();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("K")]
    [InlineData("3D2")]
    [InlineData("VP8")]
    [InlineData("GM")]
    [InlineData("FX/W1AW")]
    [InlineData("QQ0ZZZ")]
    public async Task AmbiguousOrUnknown_ReturnsNoGrid(string call)
    {
        var glance = await _service.GetDxccEntityGlanceAsync(call);
        glance.Should().BeNull();
    }

    [Fact]
    public async Task ConwayReefCall_DoesNotPaintAsFiji()
    {
        // Cluster lookup is unchanged: the longest prefix of 3D2CCC is still Fiji.
        CtyService.GetEntityFromCallsign("3D2CCC").Should().Be(("Fiji", "OC", (int?)32));
        CtyService.GetEntityFromCallsign("3D2C").Country.Should().Be("Fiji");

        var partial = await _service.GetDxccEntityGlanceAsync("3D2C");
        partial.Should().NotBeNull();
        partial!.EntityName.Should().Be("Conway Reef");
        partial.PrimaryPrefix.Should().Be("3D2/c");

        var full = await _service.GetDxccEntityGlanceAsync("3D2CCC");
        full!.EntityName.Should().Be("Conway Reef");
        full.PrimaryPrefix.Should().Be("3D2/c");
        full.EntityName.Should().NotBe("Fiji");
    }

    [Fact]
    public async Task ExactCallOverride_SelectsItsEntity()
    {
        // =KC4AAA is Antarctica. The prefix map has no KC4 key, so the cluster
        // lookup falls through to K (United States). The glance keeps the override.
        CtyService.GetEntityFromCallsign("KC4AAA").Country.Should().Be("United States");

        var glance = await _service.GetDxccEntityGlanceAsync("KC4AAA");
        glance.Should().NotBeNull();
        glance!.EntityName.Should().Be("Antarctica");
    }

    [Fact]
    public async Task NeverWorkedEntity_DrawsEveryCellEmpty()
    {
        var glance = await _service.GetDxccEntityGlanceAsync("KH6");

        glance.Should().NotBeNull();
        glance!.EntityName.Should().Be("Hawaii");
        glance.PrimaryPrefix.Should().Be("KH6");
        glance.Bands.Should().Equal(AllBands);
        glance.Rows.Select(r => r.ModeGroup).Should().Equal("phone", "cw", "digital");
        foreach (var row in glance.Rows)
        {
            row.Cells.Should().HaveCount(AllBands.Length);
            foreach (var cell in row.Cells)
                ShouldBeEmpty(cell);
        }
    }

    [Fact]
    public async Task W1aw_IsUnitedStates_NotAPartialGuess()
    {
        var glance = await _service.GetDxccEntityGlanceAsync("w1aw");
        glance!.EntityName.Should().Be("United States");
        glance.PrimaryPrefix.Should().Be("K");
    }

    [Theory]
    [InlineData("HB9GUX/P")]
    [InlineData("HB9GUX/MM")]
    [InlineData("HB9GUX/QRP")]
    public async Task KnownSuffix_StaysOnTheBaseEntity(string call)
    {
        var glance = await _service.GetDxccEntityGlanceAsync(call);
        var plain = await _service.GetDxccEntityGlanceAsync("HB9GUX");
        glance!.EntityName.Should().Be(plain!.EntityName).And.Be("Switzerland");
        glance.PrimaryPrefix.Should().Be(plain.PrimaryPrefix).And.Be("HB");
    }

    [Fact]
    public async Task PrefixForm_France_DoesNotCountASwitzerlandQso()
    {
        Setup(
            Qso("HB9GUX", country: "Switzerland", mode: "USB"),
            Qso("F5ABC", country: "France", mode: "USB"));

        var glance = await _service.GetDxccEntityGlanceAsync("F/HB9GUX");
        glance!.EntityName.Should().Be("France");
        glance.PrimaryPrefix.Should().Be("F");
        At(glance, "phone", "20m").QsoCount.Should().Be(1);
        At(glance, "cw", "20m").Worked.Should().BeFalse();
        At(glance, "digital", "20m").Worked.Should().BeFalse();
    }

    [Theory]
    [InlineData("USB")]
    [InlineData("LSB")]
    [InlineData("SSB")]
    [InlineData("AM")]
    [InlineData("FM")]
    [InlineData("DSB")]
    public async Task PhoneModes_FillOnlyThePhoneCell(string mode)
    {
        Setup(Qso("W1AW", mode: mode));
        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        At(glance, "phone", "20m").Worked.Should().BeTrue();
        ShouldBeEmpty(At(glance, "cw", "20m"));
        ShouldBeEmpty(At(glance, "digital", "20m"));
    }

    [Theory]
    [InlineData("CW")]
    [InlineData("CWU")]
    [InlineData("CWL")]
    [InlineData("CWR")]
    [InlineData("CW-R")]
    public async Task CwModes_FillOnlyTheCwCell(string mode)
    {
        Setup(Qso("W1AW", mode: mode));
        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        At(glance, "cw", "20m").Worked.Should().BeTrue();
        ShouldBeEmpty(At(glance, "phone", "20m"));
        ShouldBeEmpty(At(glance, "digital", "20m"));
    }

    [Theory]
    [InlineData("FT8")]
    [InlineData("FT4")]
    [InlineData("RTTY")]
    [InlineData("PSK31")]
    [InlineData("PSK")]
    [InlineData("PSK63")]
    [InlineData("PSK125")]
    [InlineData("DATA-U")]
    public async Task DigitalModes_FillOnlyTheDigitalCell(string mode)
    {
        Setup(Qso("W1AW", mode: mode));
        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        At(glance, "digital", "20m").Worked.Should().BeTrue();
        ShouldBeEmpty(At(glance, "phone", "20m"));
        ShouldBeEmpty(At(glance, "cw", "20m"));
    }

    [Theory]
    [InlineData("SPEC")]
    [InlineData("WIDGET")]
    [InlineData("")]
    [InlineData("   ")]
    public async Task UnknownOrBlankMode_FillsNoCell(string mode)
    {
        Setup(Qso("W1AW", mode: mode));
        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        ShouldBeEmpty(At(glance, "phone", "20m"));
        ShouldBeEmpty(At(glance, "cw", "20m"));
        ShouldBeEmpty(At(glance, "digital", "20m"));
    }

    [Fact]
    public async Task BlankBand_FillsNoCell()
    {
        Setup(Qso("W1AW", band: "  ", mode: "USB"));
        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        glance.Bands.Should().BeEmpty();
        foreach (var row in glance.Rows)
            row.Cells.Should().BeEmpty();
    }

    [Theory]
    [InlineData("lotw")]
    [InlineData("card")]
    [InlineData("eqsl")]
    [InlineData("qrz")]
    public async Task ConfirmationPolicyAny_MakesTheCellGreen(string channel)
    {
        Setup(Qso("W1AW", tweak: q => q.Qsl = Confirmed(channel)));
        var cell = At((await _service.GetDxccEntityGlanceAsync("W1AW"))!, "phone", "20m");
        cell.Worked.Should().BeTrue();
        cell.Confirmed.Should().BeTrue();
    }

    [Fact]
    public async Task SentOnly_OrRcvdN_StaysAmber()
    {
        Setup(
            Qso("W1AW", tweak: q => q.Qsl = new QslStatus { Lotw = new LotwStatus { Sent = "Y", Rcvd = "N" }, Sent = "Y" }),
            Qso("K1JT", band: "40m", tweak: q => q.Qsl = new QslStatus { Rcvd = "N", Eqsl = new EqslStatus { Sent = "Y" } }));

        var twenty = At((await _service.GetDxccEntityGlanceAsync("W1AW"))!, "phone", "20m");
        twenty.Worked.Should().BeTrue();
        twenty.Confirmed.Should().BeFalse();

        var forty = At((await _service.GetDxccEntityGlanceAsync("K1JT"))!, "phone", "40m");
        forty.Worked.Should().BeTrue();
        forty.Confirmed.Should().BeFalse();
    }

    [Fact]
    public async Task TwoQsos_ConfirmedIfEitherIs_CountIsTwo()
    {
        Setup(
            Qso("W1AW"),
            Qso("K1JT", tweak: q => q.Qsl = Confirmed("lotw")));

        var cell = At((await _service.GetDxccEntityGlanceAsync("W1AW"))!, "phone", "20m");
        cell.Worked.Should().BeTrue();
        cell.Confirmed.Should().BeTrue();
        cell.QsoCount.Should().Be(2);
        cell.CallsignWorked.Should().BeFalse();
    }

    [Fact]
    public async Task PersonalContestQso_Counts_OtherStationCall_DoesNot()
    {
        var mine = Qso("W1AW", tweak: q => q.Contest = new ContestInfo { ContestId = "CQ-WW-SSB" });
        var club = Qso("K1JT", band: "40m", tweak: q => q.Contest = new ContestInfo
        {
            ContestId = "CQ-WW-SSB",
            StationCallsign = "W1AW",
        });
        Setup(mine, club);

        var settings = new Mock<ISettingsService>();
        settings.Setup(s => s.GetSettingsAsync(It.IsAny<string>()))
            .ReturnsAsync(new UserSettings { Station = new StationSettings { Callsign = "N8SDR" } });
        var service = new AwardsService(_repo.Object, settings: settings.Object);

        var glance = (await service.GetDxccEntityGlanceAsync("W1AW"))!;
        At(glance, "phone", "20m").QsoCount.Should().Be(1);
        glance.Bands.Should().NotContain("40m");

        var sameCall = Qso("K1JT", band: "15m", tweak: q => q.Contest = new ContestInfo
        {
            ContestId = "CQ-WW-SSB",
            StationCallsign = "n8sdr",
        });
        Setup(mine, club, sameCall);
        var again = (await service.GetDxccEntityGlanceAsync("W1AW"))!;
        At(again, "phone", "15m").Worked.Should().BeTrue();
        again.Bands.Should().NotContain("40m");
    }

    [Fact]
    public async Task Create_CopiesStationCountry_AndStatisticsCountsIt()
    {
        Qso? created = null;
        var repo = new Mock<IQsoRepository>();
        repo.Setup(r => r.CreateAsync(It.IsAny<Qso>()))
            .Callback<Qso>(q => created = q)
            .ReturnsAsync((Qso q) => q);
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);

        var logging = new QsoService(repo.Object, hub.Object);
        await logging.CreateAsync(new CreateQsoRequest(
            Callsign: "W1AW",
            QsoDate: new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc),
            TimeOn: "120000",
            Band: "20m",
            Mode: "USB",
            Country: "United States"));

        created.Should().NotBeNull();
        created!.Station!.Country.Should().Be("United States");
        created.Country.Should().Be("United States");

        repo.Setup(r => r.GetAllAsync()).ReturnsAsync([created]);
        var awards = new AwardsService(repo.Object);

        var cell = At((await awards.GetDxccEntityGlanceAsync("W1AW"))!, "phone", "20m");
        cell.Worked.Should().BeTrue();
        cell.QsoCount.Should().Be(1);

        var stats = await awards.GetDxccStatisticsAsync();
        stats.Entities.Should().ContainSingle(e => e.EntityName == "United States");
        stats.TotalEntitiesWorked.Should().Be(1);
    }

    [Fact]
    public async Task OlderRow_EmptyQsoCountry_StationCountryStillPaints()
    {
        var older = Qso("W1AW", country: null, mode: "USB");
        older.Country.Should().BeNull();
        older.Station!.Country = "United States";

        var neither = Qso("K1JT", band: "40m", country: null, mode: "CW");
        neither.Station!.Country = null;

        var unresolved = Qso("QQ0ZZZ", band: "15m", country: null, mode: "FT8");
        unresolved.Station!.Country = "United States";

        Setup(older, neither, unresolved);

        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        glance.Bands.Should().Equal("40m", "20m", "15m");
        At(glance, "phone", "20m").Worked.Should().BeTrue();
        At(glance, "digital", "15m").Worked.Should().BeTrue();
        ShouldBeEmpty(At(glance, "cw", "40m"));
        ShouldBeEmpty(At(glance, "cw", "20m"));

        var stats = await _service.GetDxccStatisticsAsync();
        stats.Entities.Should().BeEmpty("statistics still drops a row until Qso.Country is copied on create");
    }

    [Fact]
    public async Task BandOrder_MatchesAllBands_AndUnusedBandsAreAbsent()
    {
        Setup(
            Qso("W1AW", band: "10m"),
            Qso("K1JT", band: "80m"),
            Qso("N5DX", band: "13cm", mode: "FM"));

        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        glance.Bands.Should().Equal("80m", "10m");
        At(glance, "phone", "80m").Worked.Should().BeTrue();
        At(glance, "phone", "10m").Worked.Should().BeTrue();
    }

    [Fact]
    public async Task FortyMeters_IgnoresCase()
    {
        Setup(
            Qso("W1AW", band: "40M"),
            Qso("K1JT", band: "40m"));

        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        glance.Bands.Should().Equal("40m");
        At(glance, "phone", "40m").QsoCount.Should().Be(2);
    }

    [Fact]
    public async Task DxccCode_FromAPrefixMatch_IncludesACallsignThatNoLongerResolves()
    {
        Setup(
            Qso("W1AW", tweak: q => q.Dxcc = 291),
            Qso("QQ0ZZZ", band: "40m", mode: "CW", country: "United States", tweak: q => q.Dxcc = 291),
            Qso("QQ0YYY", band: "15m", mode: "CW", country: "United States", tweak: q => q.Dxcc = 999));

        var glance = (await _service.GetDxccEntityGlanceAsync("W1AW"))!;
        At(glance, "cw", "40m").Worked.Should().BeTrue();
        ShouldBeEmpty(At(glance, "cw", "15m"));
    }

    private static QslStatus Confirmed(string channel) => channel switch
    {
        "lotw" => new QslStatus { Lotw = new LotwStatus { Rcvd = "Y" } },
        "card" => new QslStatus { Rcvd = "Y" },
        "eqsl" => new QslStatus { Eqsl = new EqslStatus { Rcvd = "Y" } },
        "qrz" => new QslStatus { Qrz = new QrzStatus { Rcvd = "Y" } },
        _ => throw new ArgumentOutOfRangeException(nameof(channel)),
    };
}
