using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class SpotStatusServiceTests
{
    private readonly Mock<IQsoRepository> _qsoRepository;
    private readonly SpotStatusService _service;

    public SpotStatusServiceTests()
    {
        _qsoRepository = new Mock<IQsoRepository>();
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>());

        var serviceProvider = BuildServiceProvider(_qsoRepository.Object);
        var logger = NullLogger<SpotStatusService>.Instance;
        _service = new SpotStatusService(serviceProvider, logger);
    }

    private static IServiceProvider BuildServiceProvider(IQsoRepository qsoRepository)
    {
        var services = new ServiceCollection();
        services.AddSingleton(qsoRepository);
        return services.BuildServiceProvider();
    }

    #region GetSpotStatus — cache-not-ready guard

    [Fact]
    public void GetSpotStatus_BeforeCacheBuilt_ReturnsNull()
    {
        // StartAsync not yet called — the initial background cache build hasn't run,
        // so every country would erroneously look "unworked". The guard must return
        // null so spots arriving during startup don't get stamped with a wrong status.
        _service.GetSpotStatus("YU7DX", "Serbia", 3700.0, "SSB")
            .Should().BeNull();
    }

    #endregion

    #region GetSpotStatus — null country

    [Fact]
    public async Task GetSpotStatus_NullCountry_ReturnsNull()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W1ABC", null, 14000.0, "SSB")
            .Should().BeNull();
    }

    [Fact]
    public async Task GetSpotStatus_EmptyCountry_ReturnsNull()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W1ABC", "", 14000.0, "SSB")
            .Should().BeNull();
    }

    #endregion

    #region GetSpotStatus — Unknown band

    [Fact]
    public async Task GetSpotStatus_UnknownBand_ReturnsNull()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // 500 kHz is not an amateur band
        _service.GetSpotStatus("W1ABC", "United States", 500.0, "SSB")
            .Should().BeNull();
    }

    #endregion

    #region GetSpotStatus — newDxcc

    [Fact]
    public async Task GetSpotStatus_NeverWorkedCountry_ReturnsNewDxcc()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W1ABC", "United States", 14000.0, "SSB")
            .Should().Be("newDxcc");
    }

    [Fact]
    public async Task GetSpotStatus_DifferentCountry_ReturnsNewDxcc()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("DL1ABC", country: "Germany", band: "20m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // United States was never worked, only Germany
        _service.GetSpotStatus("W1ABC", "United States", 14000.0, "SSB")
            .Should().Be("newDxcc");
    }

    #endregion

    #region GetSpotStatus — newBand

    [Fact]
    public async Task GetSpotStatus_WorkedCountryDifferentBand_ReturnsNewBand()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "40m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Same country but on 20m instead of 40m
        _service.GetSpotStatus("W2XYZ", "United States", 14000.0, "SSB")
            .Should().Be("newBand");
    }

    #endregion

    #region GetZoneStatus — WAZ

    [Fact]
    public void GetZoneStatus_BeforeCacheBuilt_ReturnsNull()
    {
        var (zone, status) = _service.GetZoneStatus("DL1ABC", 14000.0);
        zone.Should().BeNull();
        status.Should().BeNull();
    }

    [Fact]
    public async Task GetZoneStatus_NeverWorkedZone_ReturnsNewZone()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Empty log — Germany's CQ zone 14 has never been worked.
        var (zone, status) = _service.GetZoneStatus("DL1ABC", 14000.0);
        zone.Should().Be(14);
        status.Should().Be("newZone");
    }

    [Fact]
    public async Task GetZoneStatus_WorkedZoneSameBand_ReturnsNull()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("DL1ABC", country: "Germany", band: "20m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Another zone-14 station on the same band — already worked.
        var (_, status) = _service.GetZoneStatus("DL2XYZ", 14000.0);
        status.Should().BeNull();
    }

    [Fact]
    public async Task GetZoneStatus_WorkedZoneDifferentBand_ReturnsNewZoneBand()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("DL1ABC", country: "Germany", band: "40m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Zone 14 worked, but on 40m — 20m is a new band-slot for the zone.
        var (_, status) = _service.GetZoneStatus("DL2XYZ", 14000.0);
        status.Should().Be("newZoneBand");
    }

    #endregion

    #region GetSpotStatus — worked

    [Fact]
    public async Task GetSpotStatus_SameCountryBandMode_ReturnsWorked()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "20m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Same country+band+mode — "worked" regardless of specific callsign
        _service.GetSpotStatus("W1ABC", "United States", 14000.0, "SSB")
            .Should().Be("worked");
    }

    [Fact]
    public async Task GetSpotStatus_DifferentCallSameCountryBandMode_ReturnsWorked()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "20m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Different callsign but same country+band+mode → still "worked"
        _service.GetSpotStatus("W9XYZ", "United States", 14000.0, "SSB")
            .Should().Be("worked");
    }

    [Fact]
    public async Task GetSpotStatus_CaseInsensitive_ReturnsWorked()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("w1abc", country: "united states", band: "20m", mode: "ssb"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W1ABC", "United States", 14000.0, "SSB")
            .Should().Be("worked");
    }

    #endregion

    #region GetSpotStatus — mode normalization

    [Theory]
    [InlineData("USB", "SSB")]
    [InlineData("LSB", "SSB")]
    [InlineData("PSK31", "PSK")]
    [InlineData("PSK63", "PSK")]
    [InlineData("PSK125", "PSK")]
    public async Task GetSpotStatus_ModeNormalization_MatchesNormalized(string loggedMode, string spotMode)
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "20m", mode: loggedMode),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W1ABC", "United States", 14000.0, spotMode)
            .Should().Be("worked");
    }

    #endregion

    #region GetSpotStatus — null mode returns null (not worked)

    [Fact]
    public async Task GetSpotStatus_NullMode_WorkedCountryAndBand_ReturnsNull()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "20m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Same country+band but no mode in spot — can't determine worked status, returns null
        _service.GetSpotStatus("W1ABC", "United States", 14000.0, null)
            .Should().BeNull();
    }

    #endregion

    #region OnQsoLogged — incremental updates

    [Fact]
    public async Task OnQsoLogged_UpdatesCache_NewDxccBecomesWorked()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W1ABC", "United States", 14000.0, "SSB")
            .Should().Be("newDxcc");

        _service.OnQsoLogged("W1ABC", "United States", "20m", "SSB");

        _service.GetSpotStatus("W1ABC", "United States", 14000.0, "SSB")
            .Should().Be("worked");
    }

    [Fact]
    public async Task OnQsoLogged_UpdatesCache_NewBandBecomesWorked()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "40m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W2XYZ", "United States", 14000.0, "SSB")
            .Should().Be("newBand");

        _service.OnQsoLogged("W2XYZ", "United States", "20m", "SSB");

        _service.GetSpotStatus("W2XYZ", "United States", 14000.0, "SSB")
            .Should().Be("worked");
    }

    #endregion

    #region InvalidateCacheAsync

    [Fact]
    public async Task InvalidateCacheAsync_RebuildsCacheFromDatabase()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W1ABC", "United States", 14000.0, "SSB")
            .Should().Be("newDxcc");

        // Now the repository returns a QSO
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "20m", mode: "SSB"),
        });

        await _service.InvalidateCacheAsync();

        _service.GetSpotStatus("W1ABC", "United States", 14000.0, "SSB")
            .Should().Be("worked");
    }

    #endregion

    #region GetSpotStatus — country name normalization

    [Theory]
    [InlineData("United Arab Emirates", "UAE")]
    [InlineData("Trinidad and Tobago", "Trinidad & Tobago")]
    [InlineData("Cote d'Ivoire", "Ivory Coast")]
    public async Task GetSpotStatus_CountryAliasNormalization_MatchesWorked(string qsoCountry, string spotCountry)
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: qsoCountry, band: "20m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Spot uses alias name, QSO uses ADIF name — should still match
        _service.GetSpotStatus("W1ABC", spotCountry, 14000.0, "SSB")
            .Should().Be("worked");
    }

    [Fact]
    public async Task GetSpotStatus_EnglandSpot_EnglandInLog_ReturnsWorked()
    {
        // CtyService returns "England" (ADIF entity name) directly — no alias needed
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("G4ABC", country: "England", band: "20m", mode: "CW"),
            MakeQso("G4ABC", country: "England", band: "40m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("M7JTV", "England", 14000.0, "CW")
            .Should().Be("worked");

        // Different band → newBand (not newDxcc)
        _service.GetSpotStatus("M7JTV", "England", 28000.0, "CW")
            .Should().Be("newBand");
    }

    [Fact]
    public async Task OnQsoLogged_SameEntityName_MatchesSpotLookup()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Log a QSO with ADIF entity name
        _service.OnQsoLogged("G4ABC", "England", "20m", "SSB");

        // Spot lookup now also uses "England" from CtyService
        _service.GetSpotStatus("M0ABC", "England", 14000.0, "SSB")
            .Should().Be("worked");
    }

    [Fact]
    public async Task GetSpotStatus_CtyDatNameMismatch_GermanyWorked_FedRepNotNewDxcc()
    {
        // QSO log stores "Germany" but CtyService returns "Fed. Rep. of Germany" for DL callsigns.
        // Both names should be indexed during cache build via CtyService callsign lookup.
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("DL1ABC", country: "Germany", band: "20m", mode: "SSB"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Spot arrives with cty.dat name "Fed. Rep. of Germany" — should NOT be newDxcc
        _service.GetSpotStatus("DL2RH", "Fed. Rep. of Germany", 14000.0, "SSB")
            .Should().Be("worked");
    }

    [Fact]
    public async Task OnQsoLogged_CtyDatNameMismatch_AlsoIndexesCtyName()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Log a QSO with common name "Germany" for a DL callsign
        _service.OnQsoLogged("DL1ABC", "Germany", "20m", "CW");

        // Spot lookup uses cty.dat name "Fed. Rep. of Germany" — should still match
        _service.GetSpotStatus("DL2RH", "Fed. Rep. of Germany", 14000.0, "CW")
            .Should().Be("worked");
    }

    #endregion

    #region GetGridStatus — VUCC / grid chasing

    [Fact]
    public void GetGridStatus_BeforeCacheBuilt_ReturnsNull()
        => _service.GetGridStatus("FN42", 14000.0).Should().BeNull();

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("FN")]      // too short
    [InlineData("4242")]    // not a valid locator
    public async Task GetGridStatus_MissingOrInvalidGrid_ReturnsNull(string? grid)
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetGridStatus(grid, 14000.0).Should().BeNull();
    }

    [Fact]
    public async Task GetGridStatus_NeverWorkedGrid_ReturnsNewGrid()
    {
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Empty log — FN42 has never been worked.
        _service.GetGridStatus("FN42", 14000.0).Should().Be("newGrid");
    }

    [Fact]
    public async Task GetGridStatus_WorkedGridSameBand_ReturnsNull()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "20m", mode: "FT8", grid: "FN42"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetGridStatus("FN42", 14000.0).Should().BeNull();
    }

    [Fact]
    public async Task GetGridStatus_WorkedGridDifferentBand_ReturnsNewGridBand()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "40m", mode: "FT8", grid: "FN42"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // FN42 worked on 40m, seen now on 20m → new band-slot for the grid.
        _service.GetGridStatus("FN42", 14000.0).Should().Be("newGridBand");
    }

    [Fact]
    public async Task GetGridStatus_SixCharAndLowercase_NormalizedToField()
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: "United States", band: "20m", mode: "FT8", grid: "FN42dx"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        // Stored as 6-char "FN42dx"; a lowercase 4-char "fn42" on the same band is worked.
        _service.GetGridStatus("fn42", 14000.0).Should().BeNull();
    }

    #endregion

    #region Frequency to band mapping

    [Theory]
    [InlineData(14000.0, "United States", "20m")]
    [InlineData(7000.0, "Germany", "40m")]
    [InlineData(21000.0, "Japan", "15m")]
    [InlineData(28000.0, "Brazil", "10m")]
    public async Task GetSpotStatus_CorrectBandFromFrequency(double freqKhz, string country, string expectedBand)
    {
        _qsoRepository.Setup(r => r.GetAllAsync()).ReturnsAsync(new List<Qso>
        {
            MakeQso("W1ABC", country: country, band: expectedBand, mode: "CW"),
        });
        await _service.StartAsync(CancellationToken.None);
        await _service.CacheReady;

        _service.GetSpotStatus("W1ABC", country, freqKhz, "CW")
            .Should().Be("worked");
    }

    #endregion

    #region Helpers

    private static Qso MakeQso(string callsign, string? country, string band, string mode, string? grid = null)
    {
        return new Qso
        {
            Callsign = callsign,
            Country = country,
            Band = band,
            Mode = mode,
            Grid = grid,
        };
    }

    #endregion
}
