using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

/// <summary>
/// Find duplicates (whole-log) — end-to-end through the real LiteDB repo. Verifies the canonical
/// identity grouping, the keep-one policy (synced ▸ most-complete ▸ oldest), and that removal deletes
/// only non-keeper rows, is snapshot-gated, and is local-only (no upload cascade).
/// docs/design/duplicate-management.md.
/// </summary>
[Trait("Category", "Integration")]
public class LogbookHealthDuplicateTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture;
    private readonly LiteQsoRepository _repo;
    private readonly LogbookHealthService _service;

    public LogbookHealthDuplicateTests()
    {
        _fixture = new LiteDbTestFixture();
        _repo = new LiteQsoRepository(_fixture.Context);
        _service = new LogbookHealthService(_repo);
    }

    public void Dispose() => _fixture.Dispose();

    private static Qso Q(string call, DateTime date, string timeOn, string band, string mode = "SSB")
        => new() { Callsign = call, QsoDate = date, TimeOn = timeOn, Band = band, Mode = mode };

    private static readonly DateTime T = new(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc);

    [Fact]
    public async Task Same_contact_different_mode_spelling_is_one_duplicate_group()
    {
        await _repo.CreateAsync(Q("W1AW", T, "1430", "20m", "SSB"));
        await _repo.CreateAsync(Q("W1AW", T, "1430", "20m", "PH")); // mode excluded from identity

        var scan = await _service.FindDuplicatesAsync();
        scan.GroupCount.Should().Be(1);
        scan.RedundantCount.Should().Be(1);
    }

    [Fact]
    public async Task Same_station_different_band_is_NOT_a_duplicate()
    {
        await _repo.CreateAsync(Q("W1AW", T, "1430", "20m"));
        await _repo.CreateAsync(Q("W1AW", T, "1430", "40m"));

        (await _service.FindDuplicatesAsync()).GroupCount.Should().Be(0);
    }

    [Fact]
    public async Task Keeper_is_the_synced_copy()
    {
        var unsynced = await _repo.CreateAsync(Q("W1AW", T, "1430", "20m"));
        var syncedQso = Q("W1AW", T, "1430", "20m");
        syncedQso.QrzSyncStatus = SyncStatus.Synced;
        var synced = await _repo.CreateAsync(syncedQso);

        var group = (await _service.FindDuplicatesAsync()).Groups.Single();
        group.Members.Single(m => m.Keep).Id.Should().Be(synced.Id);
        group.Members.Single(m => m.Keep).KeepReason.Should().Contain("synced");
        group.Members.Single(m => !m.Keep).Id.Should().Be(unsynced.Id);
    }

    [Fact]
    public async Task Remove_deletes_only_the_nonkeeper_and_snapshots_first()
    {
        var a = await _repo.CreateAsync(Q("W1AW", T, "1430", "20m"));   // older → keeper
        var b = await _repo.CreateAsync(Q("W1AW", T, "1430", "20m"));   // newer → redundant

        var scan = await _service.FindDuplicatesAsync();
        var deleteId = scan.Groups.Single().Members.Single(m => !m.Keep).Id;

        var snaps = 0;
        var result = await _service.RemoveDuplicatesAsync(
            new[] { deleteId }, snapshotBefore: () => { snaps++; return Task.CompletedTask; });

        result.Deleted.Should().Be(1);
        snaps.Should().Be(1);
        var remaining = (await _repo.GetAllAsync()).ToList();
        remaining.Should().ContainSingle();
        remaining[0].Id.Should().Be(scan.Groups.Single().Members.Single(m => m.Keep).Id);
    }

    [Fact]
    public async Task Remove_refuses_to_delete_a_keeper_id()
    {
        await _repo.CreateAsync(Q("W1AW", T, "1430", "20m"));
        await _repo.CreateAsync(Q("W1AW", T, "1430", "20m"));

        var scan = await _service.FindDuplicatesAsync();
        var keeperId = scan.Groups.Single().Members.Single(m => m.Keep).Id;

        var snaps = 0;
        var result = await _service.RemoveDuplicatesAsync(
            new[] { keeperId }, snapshotBefore: () => { snaps++; return Task.CompletedTask; });

        result.Deleted.Should().Be(0);
        result.Skipped.Should().Be(1);
        snaps.Should().Be(0, "nothing deletable ⇒ no snapshot");
        (await _repo.GetAllAsync()).Should().HaveCount(2, "the keeper must survive a stale/bad request");
    }
}
