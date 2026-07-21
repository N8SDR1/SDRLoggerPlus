using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Weather;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WeatherAlertServiceTests
{
    private readonly Mock<INwsClient> _nws = new();
    private readonly Mock<IAmbientWeatherClient> _ambient = new();
    private readonly Mock<IEcowittClient> _ecowitt = new();
    private readonly Mock<IBlitzortungClient> _blitzortung = new();
    private readonly UserSettings _settings = new();
    private readonly WeatherAlertService _service;

    public WeatherAlertServiceTests()
    {
        _settings.Station.Latitude = 39.1;
        _settings.Station.Longitude = -84.5;
        _settings.Weather.Credentials.AmbientApiKey = "key";
        _settings.Weather.Credentials.AmbientAppKey = "app";
        _settings.Weather.Credentials.EcowittAppKey = "app";
        _settings.Weather.Credentials.EcowittApiKey = "key";
        _settings.Weather.Credentials.EcowittMac = "AA:BB";

        var settingsService = new Mock<ISettingsService>();
        settingsService.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(_settings);
        var services = new ServiceCollection();
        services.AddScoped(_ => settingsService.Object);

        _service = new WeatherAlertService(
            services.BuildServiceProvider(),
            _nws.Object, _ambient.Object, _ecowitt.Object, _blitzortung.Object,
            NullLogger<WeatherAlertService>.Instance);
    }

    // ─── Lightning ───────────────────────────────────────────────────────

    [Fact]
    public async Task Lightning_ClosestAcrossSources_AndStrikeTotal()
    {
        _settings.Weather.Lightning.Enabled = true;
        _settings.Weather.Lightning.UseAmbient = true;
        _settings.Weather.Lightning.UseEcowitt = true;
        _settings.Weather.Lightning.UseBlitzortung = true;
        _settings.Weather.Lightning.Range = 50;       // miles → ~80 km

        _blitzortung.Setup(b => b.GetStrikesAsync(It.IsAny<double>(), It.IsAny<double>(), It.IsAny<double>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([new StrikeInfo(40, 90), new StrikeInfo(60, 180)]);
        _ambient.Setup(a => a.GetLightningAsync(It.IsAny<AmbientCredentials>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LightningReading(25.0, 12));
        _ecowitt.Setup(e => e.GetLightningAsync(It.IsAny<EcowittCredentials>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LightningReading(70.0, 3));

        await _service.PollLightningAsync(_settings, CancellationToken.None);

        var status = _service.GetLightningStatus();
        status.Active.Should().BeTrue();
        status.ClosestKm.Should().Be(25.0);            // Ambient was closest
        status.StrikeCount.Should().Be(2 + 12 + 3);
        status.Sources.Should().BeEquivalentTo("blitzortung", "ambient", "ecowitt");
    }

    [Fact]
    public async Task Lightning_NothingInRange_Inactive()
    {
        _settings.Weather.Lightning.Enabled = true;
        _settings.Weather.Lightning.UseBlitzortung = true;
        _blitzortung.Setup(b => b.GetStrikesAsync(It.IsAny<double>(), It.IsAny<double>(), It.IsAny<double>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([]);

        await _service.PollLightningAsync(_settings, CancellationToken.None);

        _service.GetLightningStatus().Active.Should().BeFalse();
    }

    [Fact]
    public async Task Lightning_NwsWarningAlone_Activates()
    {
        _settings.Weather.Lightning.Enabled = true;
        _settings.Weather.Lightning.UseBlitzortung = false;
        _settings.Weather.Lightning.UseNws = true;
        _nws.Setup(n => n.GetThunderstormWarningAsync(It.IsAny<double>(), It.IsAny<double>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync("Severe Thunderstorm Warning");

        await _service.PollLightningAsync(_settings, CancellationToken.None);

        var status = _service.GetLightningStatus();
        status.Active.Should().BeTrue();
        status.NwsWarning.Should().Be("Severe Thunderstorm Warning");
    }

    // ─── Wind ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Wind_TakesMaxAcrossSources_AndPublishesBothUnits()
    {
        _settings.Weather.Wind.Enabled = true;
        _settings.Weather.Wind.UseNwsMetar = true;
        _settings.Weather.Wind.MetarStation = "KLUK";
        _settings.Weather.Wind.UseAmbient = true;

        _nws.Setup(n => n.GetMetarWindAsync("KLUK", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new WindReading(20, 35, 270));
        _ambient.Setup(a => a.GetWindAsync(It.IsAny<AmbientCredentials>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new WindReading(32, 30, 180));

        await _service.PollWindAsync(_settings, CancellationToken.None);

        var status = _service.GetWindStatus();
        status.SustainedMph.Should().Be(32);           // max sustained (ambient)
        status.GustMph.Should().Be(35);                // max gust (metar)
        status.Direction.Should().Be("S");             // from source providing max sustained
        status.SustainedKph.Should().BeApproximately(51.5, 0.1);
        status.Severity.Should().Be(WindSeverity.High); // 32 >= 30 default threshold
        status.Active.Should().BeTrue();
    }

    [Fact]
    public async Task Wind_ElevatedDuringCooldown_Suppressed()
    {
        _settings.Weather.Wind.Enabled = true;
        _settings.Weather.Wind.UseAmbient = true;
        _settings.Weather.Wind.CooldownMinutes = 20;

        // First poll: calm → records "clear" timestamp
        _ambient.Setup(a => a.GetWindAsync(It.IsAny<AmbientCredentials>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new WindReading(5, 8, 0));
        await _service.PollWindAsync(_settings, CancellationToken.None);
        _service.GetWindStatus().Active.Should().BeFalse();

        // Second poll right after: elevated-only reading → suppressed by cooldown
        _ambient.Setup(a => a.GetWindAsync(It.IsAny<AmbientCredentials>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new WindReading(22, 10, 0));
        await _service.PollWindAsync(_settings, CancellationToken.None);
        _service.GetWindStatus().Active.Should().BeFalse();

        // But a HIGH severity reading is never suppressed
        _ambient.Setup(a => a.GetWindAsync(It.IsAny<AmbientCredentials>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new WindReading(35, 20, 0));
        await _service.PollWindAsync(_settings, CancellationToken.None);
        _service.GetWindStatus().Severity.Should().Be(WindSeverity.High);
    }

    [Fact]
    public async Task Wind_FailingSource_OthersStillAggregate()
    {
        _settings.Weather.Wind.Enabled = true;
        _settings.Weather.Wind.UseNwsMetar = true;
        _settings.Weather.Wind.MetarStation = "KLUK";
        _settings.Weather.Wind.UseAmbient = true;

        _nws.Setup(n => n.GetMetarWindAsync("KLUK", It.IsAny<CancellationToken>()))
            .ReturnsAsync((WindReading?)null);         // source down
        _ambient.Setup(a => a.GetWindAsync(It.IsAny<AmbientCredentials>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new WindReading(12, 18, 90));

        await _service.PollWindAsync(_settings, CancellationToken.None);

        var status = _service.GetWindStatus();
        status.Sources.Should().BeEquivalentTo("ambient");
        status.SustainedMph.Should().Be(12);
    }
}

[Trait("Category", "Unit")]
public class NwsClientUnitTests
{
    [Theory]
    [InlineData(100, "wmoUnit:km_h-1", 62.1371)]
    [InlineData(10, "wmoUnit:m_s-1", 22.3694)]
    [InlineData(30, "wmoUnit:mph", 30)]
    public void NormalizeToMph_HandlesUnitCodes(double value, string unitCode, double expectedMph)
        => NwsClient.NormalizeToMph(value, unitCode).Should().BeApproximately(expectedMph, 0.001);
}

[Trait("Category", "Unit")]
public class GeoMathTests
{
    [Fact]
    public void BearingToCompass_CardinalPoints()
    {
        GeoMath.BearingToCompass(0).Should().Be("N");
        GeoMath.BearingToCompass(90).Should().Be("E");
        GeoMath.BearingToCompass(180).Should().Be("S");
        GeoMath.BearingToCompass(247.5).Should().Be("WSW");
        GeoMath.BearingToCompass(359).Should().Be("N");
    }

    [Fact]
    public void GridToLatLon_KnownGrid()
    {
        // EN82 covers Michigan/Ohio area: lon -84..-82, lat 42..43
        var result = GeoMath.GridToLatLon("EN82")!.Value;
        result.Lat.Should().BeApproximately(42.5, 0.01);
        result.Lon.Should().BeApproximately(-83.0, 0.01);
    }

    [Fact]
    public void GridToLatLon_SixCharGrid_Moreprecise()
    {
        var four = GeoMath.GridToLatLon("EN82")!.Value;
        var six = GeoMath.GridToLatLon("EN82bm")!.Value;
        Math.Abs(six.Lat - four.Lat).Should().BeLessThan(1);
        Math.Abs(six.Lon - four.Lon).Should().BeLessThan(2);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("E1")]
    [InlineData("1234")]
    public void GridToLatLon_Invalid_ReturnsNull(string? grid)
        => GeoMath.GridToLatLon(grid).Should().BeNull();
}
