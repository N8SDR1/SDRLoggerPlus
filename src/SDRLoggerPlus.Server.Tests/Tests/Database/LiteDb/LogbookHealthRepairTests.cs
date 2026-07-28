using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

/// <summary>
/// "Verify QSO times" repair, end-to-end through the REAL LiteDB repository. The repair must fix the
/// lost time WITHOUT re-queuing a QRZ/LoTW upload — the same hard constraint as the timezone fix
/// (docs/design/timezone-architecture.md §5a): flag-preserving direct write, no UpdatedAt bump.
/// </summary>
[Trait("Category", "Integration")]
public class LogbookHealthRepairTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture;
    private readonly LiteQsoRepository _repo;
    private readonly LogbookHealthService _service;

    public LogbookHealthRepairTests()
    {
        _fixture = new LiteDbTestFixture();
        _repo = new LiteQsoRepository(_fixture.Context);
        _service = new LogbookHealthService(_repo);
    }

    public void Dispose() => _fixture.Dispose();

    // A SYNCED, flattened QSO: QsoDate lost its time (midnight), TimeOn kept 14:30.
    private static Qso FlattenedSynced() => new()
    {
        Callsign = "W1AW",
        QsoDate = new DateTime(2026, 3, 10, 0, 0, 0, DateTimeKind.Utc),
        TimeOn = "1430",
        Band = "20m",
        Mode = "SSB",
        QrzSyncStatus = SyncStatus.Synced,
        QrzLogId = "qrz-123",
    };

    [Fact]
    public async Task Repair_fixes_time_preserves_sync_flags_and_does_not_requeue_upload()
    {
        var created = await _repo.CreateAsync(FlattenedSynced());
        var updatedAtBefore = (await _repo.GetByIdAsync(created.Id))!.UpdatedAt;

        var snapshotCalls = 0;
        var result = await _service.RepairFixableTimesAsync(
            new[] { created.Id }, snapshotBefore: () => { snapshotCalls++; return Task.CompletedTask; });

        result.Repaired.Should().Be(1);
        snapshotCalls.Should().Be(1, "a snapshot must be taken before any write");

        var after = await _repo.GetByIdAsync(created.Id);
        after!.QsoDate.Should().Be(new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc),
            "the lost time is reconstructed from TimeOn");
        after.QsoDate.Kind.Should().Be(DateTimeKind.Utc);

        // Upload-safety: sync state untouched, so it is NOT re-queued to QRZ.
        after.QrzSyncStatus.Should().Be(SyncStatus.Synced);
        after.QrzLogId.Should().Be("qrz-123");
        after.UpdatedAt.Should().Be(updatedAtBefore, "a time repair must not bump UpdatedAt");
        (await _repo.GetUnsyncedToQrzAsync()).Should().BeEmpty();
    }

    [Fact]
    public async Task Repair_skips_a_row_that_is_no_longer_fixable_and_takes_no_snapshot()
    {
        // A consistent QSO (not fixable) requested by a stale client must be skipped, untouched.
        var consistent = await _repo.CreateAsync(new Qso
        {
            Callsign = "N9BC",
            QsoDate = new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc),
            TimeOn = "1430", Band = "20m", Mode = "CW",
        });

        var snapshotCalls = 0;
        var result = await _service.RepairFixableTimesAsync(
            new[] { consistent.Id }, snapshotBefore: () => { snapshotCalls++; return Task.CompletedTask; });

        result.Repaired.Should().Be(0);
        result.Skipped.Should().Be(1);
        snapshotCalls.Should().Be(0, "no snapshot when there is nothing to repair");
        (await _repo.GetByIdAsync(consistent.Id))!.QsoDate
            .Should().Be(new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc));
    }
}
