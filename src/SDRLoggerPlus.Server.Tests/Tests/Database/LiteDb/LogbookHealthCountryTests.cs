using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

/// <summary>
/// "Standardize country names" through the REAL LiteDB repo. Grouping is by DXCC number (provably the
/// same entity), and the rewrite must NOT re-queue an upload — flag-preserving direct write, no
/// UpdatedAt bump, never through UpdateAsync. That upload-safety invariant is the whole point.
/// </summary>
[Trait("Category", "Integration")]
public class LogbookHealthCountryTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture;
    private readonly LiteQsoRepository _repo;
    private readonly LogbookHealthService _service;

    public LogbookHealthCountryTests()
    {
        _fixture = new LiteDbTestFixture();
        _repo = new LiteQsoRepository(_fixture.Context);
        _service = new LogbookHealthService(_repo);
    }

    public void Dispose() => _fixture.Dispose();

    private Task<Qso> Add(string call, string country, SyncStatus qrz = SyncStatus.NotSynced) =>
        _repo.CreateAsync(new Qso
        {
            Callsign = call,
            Dxcc = 291,            // all United States
            Country = country,
            QsoDate = new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc),
            TimeOn = "1430", Band = "20m", Mode = "SSB",
            QrzSyncStatus = qrz,
            QrzLogId = qrz == SyncStatus.Synced ? "qrz-1" : null,
        });

    [Fact]
    public async Task Normalize_unifies_country_and_preserves_sync_flags()
    {
        // "United States" is both cty.dat's name and the most common spelling, so the canonical is
        // deterministic whether or not cty.dat is loaded in the test host.
        await Add("W1AW", "United States");
        await Add("K2ABC", "United States");
        var syncedUsa = await Add("N3DEF", "USA", SyncStatus.Synced);
        await Add("W4GHI", "UNITED STATES OF AMERICA");

        var updatedAtBefore = (await _repo.GetByIdAsync(syncedUsa.Id))!.UpdatedAt;

        var audit = await _service.AuditCountryNamesAsync();
        audit.GroupCount.Should().Be(1);
        audit.Groups[0].Dxcc.Should().Be(291);
        audit.Groups[0].Canonical.Should().Be("United States");
        audit.Groups[0].ChangeCount.Should().Be(2, "USA and UNITED STATES OF AMERICA differ from canonical");

        var snaps = 0;
        var result = await _service.NormalizeCountryNamesAsync(
            new[] { 291 }, snapshotBefore: () => { snaps++; return Task.CompletedTask; });

        result.Changed.Should().Be(2);
        snaps.Should().Be(1, "a backup must be taken before the first write");

        // Every US QSO now reads the same canonical name.
        (await _repo.GetAllAsync()).Select(q => q.Country).Distinct()
            .Should().ContainSingle().Which.Should().Be("United States");

        // UPLOAD-SAFETY: the synced row changed country but stayed Synced, kept its log id + UpdatedAt,
        // and was NOT added to the QRZ upload queue.
        var after = await _repo.GetByIdAsync(syncedUsa.Id);
        after!.Country.Should().Be("United States");
        after.QrzSyncStatus.Should().Be(SyncStatus.Synced);
        after.QrzLogId.Should().Be("qrz-1");
        after.UpdatedAt.Should().Be(updatedAtBefore, "a country rename must not bump UpdatedAt");
        (await _repo.GetUnsyncedToQrzAsync()).Select(q => q.Id).Should().NotContain(syncedUsa.Id);
    }

    [Fact]
    public async Task Audit_ignores_consistent_entities_and_skips_qsos_without_dxcc()
    {
        await Add("W1AW", "United States");
        await Add("K2ABC", "United States");   // same entity, same spelling — nothing to unify
        await _repo.CreateAsync(new Qso        // no DXCC number → skipped
        {
            Callsign = "XX0X", Country = "USA",
            QsoDate = new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc),
            TimeOn = "1430", Band = "20m", Mode = "CW",
        });

        var audit = await _service.AuditCountryNamesAsync();
        audit.GroupCount.Should().Be(0);
        audit.WithoutDxcc.Should().Be(1);
    }
}
