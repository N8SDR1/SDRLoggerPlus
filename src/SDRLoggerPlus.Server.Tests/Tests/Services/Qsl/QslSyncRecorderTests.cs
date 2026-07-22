using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Services.Qsl;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services.Qsl;

[Trait("Category", "Integration")]
public class QslSyncRecorderTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture = new();
    private readonly LiteQsoRepository _repo;
    private readonly QslSyncRecorder _recorder;

    public QslSyncRecorderTests()
    {
        _repo = new LiteQsoRepository(_fixture.Context);
        _recorder = new QslSyncRecorder(_repo, NullLogger<QslSyncRecorder>.Instance);
    }

    public void Dispose() => _fixture.Dispose();

    private async Task<Qso> LogQso(SyncStatus qrz = SyncStatus.Synced)
    {
        return await _repo.CreateAsync(new Qso
        {
            Callsign = "W1AW",
            Band = "20m",
            Mode = "FT8",
            QsoDate = DateTime.UtcNow.Date,
            TimeOn = "1200",
            QrzSyncStatus = qrz,
        });
    }

    private async Task<QslServiceSync?> Entry(string id, string service) =>
        (await _repo.GetByIdAsync(id))?.QslSync?.For(service);

    [Fact]
    public async Task RecordsASuccessfulUpload()
    {
        var qso = await LogQso();

        var written = await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey, QslUploadResult.Success());

        written.Should().BeTrue();
        var entry = await Entry(qso.Id, QslSyncLedger.ClubLogKey);
        entry!.Status.Should().Be(SyncStatus.Synced);
        entry.SyncedAt.Should().NotBeNull();
        entry.LastError.Should().BeNull();
        entry.Attempts.Should().Be(0);
    }

    [Fact]
    public async Task RecordsAFailureWithItsKindAndMessage()
    {
        var qso = await LogQso();

        await _recorder.RecordAsync(qso.Id, QslSyncLedger.EqslKey,
            QslUploadResult.Fail(QslFailureKind.Temporary, "eQSL: server error"));

        var entry = await Entry(qso.Id, QslSyncLedger.EqslKey);
        entry!.Status.Should().Be(SyncStatus.NotSynced);
        entry.FailureKind.Should().Be(QslFailureKind.Temporary);
        entry.LastError.Should().Be("eQSL: server error");
        entry.Attempts.Should().Be(1);
        entry.SyncedAt.Should().BeNull();
        entry.IsRetryable.Should().BeTrue();
    }

    [Fact]
    public async Task DoesNotRecordAnUnconfiguredService()
    {
        // Otherwise every QSO would carry failure marks for services the
        // operator deliberately never enabled.
        var qso = await LogQso();

        var written = await _recorder.RecordAsync(qso.Id, QslSyncLedger.HrdLogKey,
            QslUploadResult.NotConfigured("HRDLog upload not enabled"));

        written.Should().BeFalse();
        (await _repo.GetByIdAsync(qso.Id))!.QslSync.Should().BeNull(
            "an unconfigured service must leave the QSO untracked, not marked failed");
    }

    [Fact]
    public async Task AuthAndRejectedFailuresAreNotRetryable()
    {
        var qso = await LogQso();

        await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey,
            QslUploadResult.Fail(QslFailureKind.Auth, "403"));
        (await Entry(qso.Id, QslSyncLedger.ClubLogKey))!.IsRetryable.Should().BeFalse(
            "retrying a Club Log auth failure is what triggers an IP ban");

        await _recorder.RecordAsync(qso.Id, QslSyncLedger.EqslKey,
            QslUploadResult.Fail(QslFailureKind.Rejected, "ERROR: bad record"));
        (await Entry(qso.Id, QslSyncLedger.EqslKey))!.IsRetryable.Should().BeFalse(
            "an identical retry of a rejected QSO cannot succeed");
    }

    [Fact]
    public async Task RepeatedFailuresAccumulateAttempts()
    {
        var qso = await LogQso();

        for (var i = 0; i < 3; i++)
        {
            await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey,
                QslUploadResult.Fail(QslFailureKind.Temporary, "timeout"));
        }

        (await Entry(qso.Id, QslSyncLedger.ClubLogKey))!.Attempts.Should().Be(3);
    }

    [Fact]
    public async Task SuccessClearsAPreviousFailure()
    {
        var qso = await LogQso();
        await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey,
            QslUploadResult.Fail(QslFailureKind.Temporary, "timeout"));

        await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey, QslUploadResult.Success());

        var entry = await Entry(qso.Id, QslSyncLedger.ClubLogKey);
        entry!.Status.Should().Be(SyncStatus.Synced);
        entry.LastError.Should().BeNull();
        entry.FailureKind.Should().Be(QslFailureKind.None);
        entry.Attempts.Should().Be(0);
    }

    [Fact]
    public async Task AFailureAfterASuccessKeepsTheOriginalSyncedAt()
    {
        // "It did reach Club Log on this date" stays true even if a later
        // re-send fails.
        var qso = await LogQso();
        await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey, QslUploadResult.Success());
        var syncedAt = (await Entry(qso.Id, QslSyncLedger.ClubLogKey))!.SyncedAt;

        await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey,
            QslUploadResult.Fail(QslFailureKind.Temporary, "timeout"));

        var entry = await Entry(qso.Id, QslSyncLedger.ClubLogKey);
        entry!.SyncedAt.Should().Be(syncedAt);
        entry.Status.Should().Be(SyncStatus.NotSynced);
    }

    [Fact]
    public async Task ServicesAreRecordedIndependently()
    {
        var qso = await LogQso();

        await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey, QslUploadResult.Success());
        await _recorder.RecordAsync(qso.Id, QslSyncLedger.EqslKey,
            QslUploadResult.Fail(QslFailureKind.Temporary, "down"));

        var ledger = (await _repo.GetByIdAsync(qso.Id))!.QslSync!;
        ledger.ClubLog!.Status.Should().Be(SyncStatus.Synced);
        ledger.Eqsl!.Status.Should().Be(SyncStatus.NotSynced);
        ledger.HrdLog.Should().BeNull("a service that never reported must stay untracked");
    }

    [Fact]
    public async Task RecordingDoesNotFlipTheQrzSyncStatus()
    {
        // Writing the ledger must not look like a QSO edit: flipping Synced to
        // Modified would queue a pointless QRZ re-upload for every QSO every
        // time an unrelated service reported back.
        var qso = await LogQso(qrz: SyncStatus.Synced);
        var updatedAtBefore = (await _repo.GetByIdAsync(qso.Id))!.UpdatedAt;

        await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey, QslUploadResult.Success());

        var after = (await _repo.GetByIdAsync(qso.Id))!;
        after.QrzSyncStatus.Should().Be(SyncStatus.Synced);
        after.UpdatedAt.Should().Be(updatedAtBefore);
    }

    [Fact]
    public async Task ADeletedQsoIsHandledQuietly()
    {
        // Real race: the operator deletes a mis-logged QSO before the upload
        // callback lands.
        var qso = await LogQso();
        await _repo.DeleteAsync(qso.Id);

        var written = await _recorder.RecordAsync(qso.Id, QslSyncLedger.ClubLogKey, QslUploadResult.Success());

        written.Should().BeFalse();
    }

    [Fact]
    public async Task LongErrorBodiesAreTruncated()
    {
        // eQSL answers 200 with a full HTML page; the ledger is a status
        // record, not a response archive.
        var qso = await LogQso();

        await _recorder.RecordAsync(qso.Id, QslSyncLedger.EqslKey,
            QslUploadResult.Fail(QslFailureKind.Rejected, new string('x', 5000)));

        (await Entry(qso.Id, QslSyncLedger.EqslKey))!.LastError!.Length.Should().Be(300);
    }

    [Fact]
    public async Task FailuresQueryReturnsOnlyRetryableOnes()
    {
        var temporary = await LogQso();
        var auth = await LogQso();
        var ok = await LogQso();
        var untracked = await LogQso();

        await _recorder.RecordAsync(temporary.Id, QslSyncLedger.ClubLogKey,
            QslUploadResult.Fail(QslFailureKind.Temporary, "timeout"));
        await _recorder.RecordAsync(auth.Id, QslSyncLedger.ClubLogKey,
            QslUploadResult.Fail(QslFailureKind.Auth, "403"));
        await _recorder.RecordAsync(ok.Id, QslSyncLedger.ClubLogKey, QslUploadResult.Success());

        var failures = (await _repo.GetQslFailuresAsync(QslSyncLedger.ClubLogKey)).ToList();

        failures.Select(q => q.Id).Should().BeEquivalentTo(new[] { temporary.Id });
        failures.Should().NotContain(q => q.Id == untracked.Id,
            "QSOs logged before tracking existed must never be swept into a bulk re-send");
    }
}
