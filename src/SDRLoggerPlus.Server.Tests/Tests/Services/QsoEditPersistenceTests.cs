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
/// Issue #34 — editing a QSO's grid appeared to do nothing.
///
/// Name, Grid and Country exist twice on a Qso: at the top level and again on
/// Station. MapToResponse reads the top-level one first, and ADIF import
/// populates both, so an update that touched only Station was written to the
/// database and then shadowed on every read by the stale top-level value.
///
/// The bug needed an IMPORTED QSO to show itself, which is why it survived the
/// existing tests: a QSO created through the API has no top-level Grid, so the
/// nested value wins by default and the edit looks fine. Every fixture here
/// therefore populates both fields, the way import leaves them.
/// </summary>
[Trait("Category", "Unit")]
public class QsoEditPersistenceTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly QsoService _service;
    private Qso _stored = null!;

    public QsoEditPersistenceTests()
    {
        // As ADIF import leaves it: the same value in both places.
        _stored = new Qso
        {
            Id = "qso-1",
            Callsign = "N9GRID",
            QsoDate = new DateTime(2026, 7, 26, 12, 0, 0, DateTimeKind.Utc),
            TimeOn = "120000",
            Band = "20m",
            Mode = "SSB",
            Name = "OrigName",
            Grid = "AA00",
            Country = "United States",
            Station = new StationInfo
            {
                Name = "OrigName",
                Grid = "AA00",
                Country = "United States",
            },
        };

        _repo.Setup(r => r.GetByIdAsync("qso-1")).ReturnsAsync(() => _stored);
        _repo.Setup(r => r.UpdateAsync("qso-1", It.IsAny<Qso>()))
            .Callback<string, Qso>((_, q) => _stored = q)
            .ReturnsAsync(true);

        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);

        _service = new QsoService(_repo.Object, hub.Object);
    }

    [Fact]
    public async Task EditingTheGridOfAnImportedQsoSticks()
    {
        var response = await _service.UpdateAsync("qso-1", new UpdateQsoRequest(Grid: "BB11"));

        response!.Station!.Grid.Should().Be("BB11", "the edit must be what the caller sees back");
        _stored.Station!.Grid.Should().Be("BB11");
        _stored.Grid.Should().Be("BB11", "the top-level copy is what MapToResponse reads first");
    }

    [Fact]
    public async Task TheEditSurvivesAReReadRatherThanRevertingOnDisplay()
    {
        // The actual symptom: save looked fine, then re-opening the edit pencil
        // showed the original value again.
        await _service.UpdateAsync("qso-1", new UpdateQsoRequest(Grid: "BB11"));

        var reread = await _service.GetByIdAsync("qso-1");

        reread!.Station!.Grid.Should().Be("BB11");
    }

    [Fact]
    public async Task EditingNameAndCountryOfAnImportedQsoSticks()
    {
        // Same shadowing, same fix — reported for grid, but these two shared it.
        var response = await _service.UpdateAsync("qso-1",
            new UpdateQsoRequest(Name: "NewName", Country: "Canada"));

        response!.Station!.Name.Should().Be("NewName");
        response.Station.Country.Should().Be("Canada");
        _stored.Name.Should().Be("NewName");
        _stored.Country.Should().Be("Canada");
    }

    [Fact]
    public async Task StateAndCountyStillSave()
    {
        // These only ever lived on Station, so they were never shadowed. Pinned
        // so the fix above cannot regress them.
        var response = await _service.UpdateAsync("qso-1",
            new UpdateQsoRequest(State: "MN", County: "Hennepin"));

        response!.Station!.State.Should().Be("MN");
        response.Station.County.Should().Be("Hennepin");
    }

    [Fact]
    public async Task AnUneditedFieldIsLeftAlone()
    {
        // null still means "not edited" — editing the grid must not blank the
        // name just because the request didn't mention it.
        await _service.UpdateAsync("qso-1", new UpdateQsoRequest(Grid: "BB11"));

        _stored.Name.Should().Be("OrigName");
        _stored.Station!.Name.Should().Be("OrigName");
        _stored.Country.Should().Be("United States");
    }

    [Fact]
    public async Task EditingAQsoThatOnlyHasTheNestedFieldStillWorks()
    {
        // A QSO logged through the app carries no top-level Grid. Writing both
        // must not depend on the top-level one already existing.
        _stored.Grid = null;
        _stored.Name = null;
        _stored.Country = null;

        var response = await _service.UpdateAsync("qso-1", new UpdateQsoRequest(Grid: "CC22"));

        response!.Station!.Grid.Should().Be("CC22");
        _stored.Grid.Should().Be("CC22");
    }
}
