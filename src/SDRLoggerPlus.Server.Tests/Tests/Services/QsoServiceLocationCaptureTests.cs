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
/// Live-logged QSOs used to capture no state or county at all —
/// CreateQsoRequest had neither field and CreateAsync set neither. These pin
/// the capture path that feeds WAS (state) and USA-CA (county).
/// </summary>
[Trait("Category", "Unit")]
public class QsoServiceLocationCaptureTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly QsoService _service;
    private Qso? _created;

    public QsoServiceLocationCaptureTests()
    {
        _repo.Setup(r => r.CreateAsync(It.IsAny<Qso>()))
            .Callback<Qso>(q => _created = q)
            .ReturnsAsync((Qso q) => q);

        // CreateAsync broadcasts on the hub; a bare mock returns a null
        // Clients.All and throws, so stub the chain.
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);

        _service = new QsoService(_repo.Object, hub.Object);
    }

    private static CreateQsoRequest Request(string? state = null, string? county = null) => new(
        Callsign: "W1AW",
        QsoDate: new DateTime(2026, 7, 22, 12, 0, 0, DateTimeKind.Utc),
        TimeOn: "120000",
        Band: "20m",
        Mode: "CW",
        Country: "United States",
        State: state,
        County: county);

    [Fact]
    public async Task CreateCarriesStateAndCountyOntoTheStation()
    {
        await _service.CreateAsync(Request(state: "MN", county: "Hennepin"));

        _created!.Station!.State.Should().Be("MN");
        _created.Station.County.Should().Be("Hennepin");
    }

    [Fact]
    public async Task CreateStripsAnAdifStyleStatePrefixFromTheCounty()
    {
        // Callers normally send the bare name, but the ADIF "ST,County" form
        // must not end up stored with its prefix.
        await _service.CreateAsync(Request(state: "MN", county: "MN,Hennepin"));

        _created!.Station!.County.Should().Be("Hennepin");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task CreateLeavesCountyNullWhenNothingUsableWasSent(string? county)
    {
        await _service.CreateAsync(Request(state: "MN", county: county));

        _created!.Station!.County.Should().BeNull();
    }

    [Fact]
    public async Task ACreatedQsoWithStateAndCountyCountsTowardUsaCa()
    {
        // The point of the capture: log at the radio → county tracked.
        await _service.CreateAsync(Request(state: "MN", county: "Hennepin"));

        Server.Services.Counties.CountyResolver.Resolve(_created!, "MN")
            .Should().NotBeNull();
    }
}
