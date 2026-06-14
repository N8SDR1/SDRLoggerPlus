using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Sat;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class SatControllerServiceTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly Mock<IAdifService> _adif = new();
    private readonly Mock<IHubContext<LogHub, ILogHubClient>> _hub = new();
    private readonly SatControllerService _service;
    private readonly List<Qso> _saved = new();

    public SatControllerServiceTests()
    {
        _repo.Setup(r => r.CreateAsync(It.IsAny<Qso>()))
            .Callback<Qso>(q => { q.Id = Guid.NewGuid().ToString(); _saved.Add(q); })
            .ReturnsAsync((Qso q) => q);

        var hubClient = new Mock<ILogHubClient>();
        _hub.Setup(h => h.Clients.All).Returns(hubClient.Object);

        var settingsService = new Mock<ISettingsService>();
        settingsService.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(new UserSettings());

        var services = new ServiceCollection();
        services.AddScoped(_ => _repo.Object);
        services.AddScoped(_ => _adif.Object);
        services.AddScoped(_ => settingsService.Object);

        _service = new SatControllerService(
            services.BuildServiceProvider(),
            _hub.Object,
            Mock.Of<IHttpClientFactory>(),
            NullLogger<SatControllerService>.Instance);
    }

    [Fact]
    public async Task StateMachine_TrackAosQsoLos()
    {
        await _service.HandleSatMessageAsync("SAT,BOOT,SN1,2.45");
        _service.GetState().Firmware.Should().Be("2.45");

        await _service.HandleSatMessageAsync("SAT,START TRACK,SO-50,27607");
        var state = _service.GetState();
        state.Status.Should().Be("tracking");
        state.Satellite.Should().Be("SO-50");

        await _service.HandleSatMessageAsync("SAT,AOS,212.5");
        state = _service.GetState();
        state.Status.Should().Be("aos");
        state.AosTimeUtc.Should().NotBeNull();

        await _service.HandleSatMessageAsync(
            "SAT,QSO,SO-50,K5P,EM12,FM,nice,59,57,145850000,436795000,Bob");
        state = _service.GetState();
        state.PassQsos.Should().ContainSingle(q => q.Callsign == "K5P");

        await _service.HandleSatMessageAsync("SAT,LOS,33.0");
        _service.GetState().Status.Should().Be("los");
    }

    [Fact]
    public async Task SatQso_AutoLogged_WithSatAdifFields()
    {
        await _service.HandleSatMessageAsync(
            "SAT,QSO,SO-50,K5P,EM12,FM,nice pass,59,57,145850000,436795000,Bob");

        _saved.Should().ContainSingle();
        var qso = _saved[0];
        qso.Callsign.Should().Be("K5P");
        qso.Mode.Should().Be("FM");
        qso.Band.Should().Be("70cm");                  // band from downlink 436.795 MHz
        qso.Frequency.Should().BeApproximately(436.795, 0.001);
        qso.AdifExtra!["SAT_NAME"].AsString.Should().Be("SO-50");
        qso.AdifExtra["PROP_MODE"].AsString.Should().Be("SAT");
    }

    [Fact]
    public async Task DuplicateSatQso_LoggedOnce()
    {
        var msg = "SAT,QSO,SO-50,K5P,EM12,FM,x,59,57,145850000,436795000,Bob";
        await _service.HandleSatMessageAsync(msg);
        await _service.HandleSatMessageAsync(msg);
        _saved.Should().ContainSingle();
    }

    [Fact]
    public async Task StartTrack_ClearsPassState()
    {
        await _service.HandleSatMessageAsync(
            "SAT,QSO,SO-50,K5P,EM12,FM,x,59,57,145850000,436795000,Bob");
        await _service.HandleSatMessageAsync("SAT,TRANSPONDER,V/U,145950000,FM,436795000,FM");
        await _service.HandleSatMessageAsync("SAT,START TRACK,AO-91,43017");

        var state = _service.GetState();
        state.PassQsos.Should().BeEmpty();
        state.Transponder.Should().BeNull();
        state.Satellite.Should().Be("AO-91");
    }

    [Fact]
    public async Task GarbageMessage_Ignored()
    {
        await _service.HandleSatMessageAsync("not a sat message");
        _service.GetState().Status.Should().Be("idle");
        _saved.Should().BeEmpty();
    }

    [Fact]
    public async Task AdifRecord_ParsedAndLogged_WithPropModeEnsured()
    {
        var parsed = new Qso
        {
            Id = "x",
            Callsign = "VP8XYZ",
            QsoDate = DateTime.UtcNow,
            TimeOn = "1200",
            Band = "2m",
            Mode = "FM",
        };
        _adif.Setup(a => a.ParseAdif(It.IsAny<string>())).Returns([parsed]);

        await _service.HandleAdifRecordAsync("<call:6>VP8XYZ ... <eor>");

        _saved.Should().ContainSingle();
        _saved[0].AdifExtra!["PROP_MODE"].AsString.Should().Be("SAT");
    }
}
