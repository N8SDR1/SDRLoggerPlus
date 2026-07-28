using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

/// <summary>
/// PHASE 1 GUARD — see docs/design/timezone-architecture.md.
///
/// The canonical-UTC read projection (LiteDbContext registers a DateTime serializer that returns
/// Kind=Utc) MUST NOT make already-synced QSOs look new/edited and re-queue them for upload — a
/// user's QRZ/LoTW logbook silently doubling is the catastrophic outcome we are guarding against.
///
/// Upload selection keys ONLY on sync-status flags, never on QsoDate (QRZ: GetUnsyncedToQrzAsync
/// selects NotSynced|Modified). Because the projection changes only how DateTimes DESERIALIZE — no
/// row is rewritten, no flag is touched, nothing routes through UpdateAsync — the unsynced set must
/// be identical, and sync flags + the exact QSO instant must survive the round-trip.
///
/// Runs correctly in any server time zone; assertions compare instants and flags, not wall-clock.
/// </summary>
[Trait("Category", "Integration")]
public class UtcProjectionUploadSafetyTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture;
    private readonly LiteQsoRepository _repo;

    // Near-midnight UTC instant — the evening-QSO case that read back on the prior local day.
    private static readonly DateTime UtcInstant = new(2026, 3, 10, 2, 30, 0, DateTimeKind.Utc);

    public UtcProjectionUploadSafetyTests()
    {
        _fixture = new LiteDbTestFixture();
        _repo = new LiteQsoRepository(_fixture.Context);
    }

    public void Dispose() => _fixture.Dispose();

    private static Qso SyncedQso() => new()
    {
        Callsign = "W1AW",
        QsoDate = UtcInstant,
        TimeOn = "0230",
        Band = "20m",
        Mode = "SSB",
        QrzSyncStatus = SyncStatus.Synced,
        QrzSyncedAt = new DateTime(2026, 3, 10, 3, 0, 0, DateTimeKind.Utc),
        QrzLogId = "qrz-123",
        LotwSyncStatus = SyncStatus.Synced,
        LotwSyncedAt = new DateTime(2026, 3, 10, 3, 5, 0, DateTimeKind.Utc),
        Qsl = new QslStatus { Lotw = new LotwStatus { Sent = "Y" } },
    };

    [Fact]
    public async Task Synced_qso_is_not_requeued_for_qrz_after_utc_projection()
    {
        await _repo.CreateAsync(SyncedQso());

        var unsynced = await _repo.GetUnsyncedToQrzAsync();
        unsynced.Should().BeEmpty("a Synced QSO must never be re-queued by a read-projection change");

        var pending = await _repo.GetPendingSyncCountAsync();
        pending.Should().Be(0);
    }

    [Fact]
    public async Task Genuinely_unsynced_qso_is_still_selected()
    {
        // The predicate must still work normally — the guard is "don't re-queue synced", not "never queue".
        var fresh = SyncedQso();
        fresh.QrzSyncStatus = SyncStatus.NotSynced;
        fresh.QrzSyncedAt = null;
        fresh.QrzLogId = null;
        await _repo.CreateAsync(fresh);

        (await _repo.GetUnsyncedToQrzAsync()).Should().ContainSingle();
    }

    [Fact]
    public async Task Sync_flags_and_exact_instant_survive_the_round_trip()
    {
        var created = await _repo.CreateAsync(SyncedQso());

        var read = await _repo.GetByIdAsync(created.Id);
        read.Should().NotBeNull();

        // Flags untouched.
        read!.QrzSyncStatus.Should().Be(SyncStatus.Synced);
        read.LotwSyncStatus.Should().Be(SyncStatus.Synced);
        read.Qsl!.Lotw!.Sent.Should().Be("Y");
        read.QrzLogId.Should().Be("qrz-123");

        // QsoDate: exact instant preserved AND now projected as Kind=Utc (the fix).
        read.QsoDate.Should().Be(UtcInstant);
        read.QsoDate.Kind.Should().Be(DateTimeKind.Utc,
            "the canonical projection returns Kind=Utc so consumers stop re-localizing");

        // Nullable DateTime projection also returns Kind=Utc (QrzSyncedAt) — proves the DateTime?
        // registration is honored, not just the non-nullable one.
        read.QrzSyncedAt.Should().NotBeNull();
        read.QrzSyncedAt!.Value.Kind.Should().Be(DateTimeKind.Utc);
        read.QrzSyncedAt.Value.Should().Be(new DateTime(2026, 3, 10, 3, 0, 0, DateTimeKind.Utc));
    }
}
