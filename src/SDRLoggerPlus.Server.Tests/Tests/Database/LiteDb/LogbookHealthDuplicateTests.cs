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
    public async Task Remove_always_keeps_one_survivor_when_the_whole_group_is_selected()
    {
        var a = await _repo.CreateAsync(Q("W1AW", T, "1430", "20m"));
        var b = await _repo.CreateAsync(Q("W1AW", T, "1430", "20m"));

        // Operator (or a bad request) selects BOTH members — one must always survive.
        var result = await _service.RemoveDuplicatesAsync(
            new[] { a.Id, b.Id }, snapshotBefore: () => Task.CompletedTask);

        result.Deleted.Should().Be(1);
        (await _repo.GetAllAsync()).Should().ContainSingle("a group is never fully erased");
    }

    [Fact]
    public async Task Remove_refuses_to_delete_a_unique_nonduplicate_qso()
    {
        var solo = await _repo.CreateAsync(Q("K1ABC", T, "1430", "15m"));

        var snaps = 0;
        var result = await _service.RemoveDuplicatesAsync(
            new[] { solo.Id }, snapshotBefore: () => { snaps++; return Task.CompletedTask; });

        result.Deleted.Should().Be(0);
        result.Skipped.Should().Be(1);
        snaps.Should().Be(0);
        (await _repo.GetAllAsync()).Should().ContainSingle("a unique QSO is never deletable");
    }

    [Fact]
    public async Task Different_modes_flag_the_group_and_default_to_keep_all()
    {
        // Same call/band/minute but CW vs FT8 — likely a wrong mode, not a plain double-entry.
        await _repo.CreateAsync(Q("K2B", T, "1430", "40m", "CW"));
        await _repo.CreateAsync(Q("K2B", T, "1430", "40m", "FT8"));

        var scan = await _service.FindDuplicatesAsync();
        var group = scan.Groups.Single();
        group.ModeMismatch.Should().BeTrue();
        group.Members.Should().OnlyContain(m => m.Keep, "mode-mismatch groups default to keep-all — the operator decides");
        scan.RedundantCount.Should().Be(0, "mode-mismatch extras are not auto-counted for removal");
    }

    [Fact]
    public async Task Phone_spelling_variants_are_not_a_mode_mismatch()
    {
        // SSB vs PH are the SAME mode spelled differently — should still auto-dedupe.
        await _repo.CreateAsync(Q("W1AW", T, "1430", "20m", "SSB"));
        await _repo.CreateAsync(Q("W1AW", T, "1430", "20m", "PH"));

        var scan = await _service.FindDuplicatesAsync();
        scan.Groups.Single().ModeMismatch.Should().BeFalse();
        scan.RedundantCount.Should().Be(1);
    }
}
