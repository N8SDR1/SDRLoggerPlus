using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

/// <summary>
/// Every mutation must drop the shared award snapshot — the cache's whole design
/// (see QsoSnapshotCache) hangs on writers being unable to skip invalidation.
/// DeleteManyAsync used a bare Checkpoint instead of Commit, so the awards kept
/// serving bulk-deleted QSOs until the next unrelated write.
/// </summary>
[Trait("Category", "Integration")]
public class LiteQsoRepositorySnapshotInvalidationTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture;
    private readonly QsoSnapshotCache _snapshots = new();
    private readonly LiteQsoRepository _repo;

    public LiteQsoRepositorySnapshotInvalidationTests()
    {
        _fixture = new LiteDbTestFixture();
        _repo = new LiteQsoRepository(_fixture.Context, _snapshots);
    }

    public void Dispose() => _fixture.Dispose();

    private Task<IReadOnlyList<Qso>> Snapshot() => _snapshots.GetAsync(() => _repo.GetAllAsync());

    [Fact]
    public async Task DeleteManyInvalidatesTheAwardSnapshot()
    {
        var a = await _repo.CreateAsync(new Qso { Callsign = "W1AW", Band = "20m", Mode = "SSB", QsoDate = DateTime.UtcNow.Date, TimeOn = "1200" });
        var b = await _repo.CreateAsync(new Qso { Callsign = "K5XYZ", Band = "40m", Mode = "CW", QsoDate = DateTime.UtcNow.Date, TimeOn = "1300" });

        (await Snapshot()).Should().HaveCount(2, "snapshot is primed before the delete");

        var deleted = await _repo.DeleteManyAsync(new[] { a.Id, b.Id });
        deleted.Should().Be(2);

        (await Snapshot()).Should().BeEmpty(
            "a bulk delete must not leave the awards serving the deleted QSOs");
    }

    [Fact]
    public async Task DeleteManyOfNothingLeavesTheSnapshotAlone()
    {
        await _repo.CreateAsync(new Qso { Callsign = "W1AW", Band = "20m", Mode = "SSB", QsoDate = DateTime.UtcNow.Date, TimeOn = "1200" });
        var primed = await Snapshot();

        await _repo.DeleteManyAsync(new[] { "no-such-id" });

        (await Snapshot()).Should().BeSameAs(primed, "nothing changed, so the cache should survive");
    }
}
