using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Bulk delete backs the Log History multi-select. It exists as its own
/// repository call rather than a loop over DeleteAsync so a 50-row page costs
/// one database checkpoint instead of fifty.
/// </summary>
[Trait("Category", "Unit")]
public class QsoBulkDeleteTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly QsoService _service;

    public QsoBulkDeleteTests()
    {
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);

        _service = new QsoService(_repo.Object, hub.Object);
    }

    [Fact]
    public async Task DeletesEveryRequestedQsoInOneRepositoryCall()
    {
        IEnumerable<string>? passed = null;
        _repo.Setup(r => r.DeleteManyAsync(It.IsAny<IEnumerable<string>>()))
            .Callback<IEnumerable<string>>(ids => passed = ids.ToList())
            .ReturnsAsync(3);

        var deleted = await _service.DeleteManyAsync(["a", "b", "c"]);

        deleted.Should().Be(3);
        passed.Should().BeEquivalentTo(["a", "b", "c"]);
        _repo.Verify(r => r.DeleteManyAsync(It.IsAny<IEnumerable<string>>()), Times.Once,
            "the point of the bulk path is a single round trip, not a loop");
        _repo.Verify(r => r.DeleteAsync(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ReportsTheCountThatActuallyExisted()
    {
        // A QSO already deleted in another window is not a failure — the caller
        // compares Deleted against Requested and can say so.
        _repo.Setup(r => r.DeleteManyAsync(It.IsAny<IEnumerable<string>>())).ReturnsAsync(2);

        (await _service.DeleteManyAsync(["a", "b", "gone"])).Should().Be(2);
    }
}

/// <summary>
/// Storage-level behaviour of the batch delete, against a real LiteDB file.
/// </summary>
[Trait("Category", "Unit")]
public class LiteQsoRepositoryBulkDeleteTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture = new();
    private readonly LiteQsoRepository _repo;

    public LiteQsoRepositoryBulkDeleteTests()
    {
        _repo = new LiteQsoRepository(_fixture.Context);
    }

    private async Task<Qso> Add(string call)
    {
        var qso = new Qso
        {
            Callsign = call,
            QsoDate = new DateTime(2026, 7, 22, 12, 0, 0, DateTimeKind.Utc),
            TimeOn = "120000",
            Band = "20m",
            Mode = "CW",
        };
        return await _repo.CreateAsync(qso);
    }

    [Fact]
    public async Task RemovesOnlyTheRequestedQsos()
    {
        var a = await Add("W1AW");
        var b = await Add("K2XY");
        var keep = await Add("N0CALL");

        var deleted = await _repo.DeleteManyAsync([a.Id, b.Id]);

        deleted.Should().Be(2);
        (await _repo.GetByIdAsync(a.Id)).Should().BeNull();
        (await _repo.GetByIdAsync(b.Id)).Should().BeNull();
        (await _repo.GetByIdAsync(keep.Id)).Should().NotBeNull("only the selected rows go");
    }

    [Fact]
    public async Task CountsOnlyRowsThatExisted()
    {
        var a = await Add("W1AW");

        (await _repo.DeleteManyAsync([a.Id, "nonexistent-id"])).Should().Be(1);
    }

    [Fact]
    public async Task IgnoresDuplicateIdsInTheRequest()
    {
        // The same id twice must not report two deletions for one QSO.
        var a = await Add("W1AW");

        (await _repo.DeleteManyAsync([a.Id, a.Id])).Should().Be(1);
    }

    [Fact]
    public async Task AnEmptyRequestDeletesNothing()
    {
        await Add("W1AW");

        (await _repo.DeleteManyAsync([])).Should().Be(0);
        (await _repo.GetCountAsync()).Should().Be(1);
    }

    public void Dispose() => _fixture.Dispose();
}
