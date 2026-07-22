using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Services.Qsl;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services.Qsl;

/// <summary>
/// The three QSL uploads run as parallel tasks and report back within
/// milliseconds of each other. Each callback does read-QSO, build-ledger,
/// write-ledger — without serialization, the last writer's ledger (built from
/// a read that predates its rivals' writes) replaces theirs, and a QSO that
/// genuinely uploaded shows untracked forever.
/// </summary>
[Trait("Category", "Integration")]
[Collection("LiteDbMapper")]
public class QslSyncRecorderConcurrencyTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture = new();
    private readonly LiteQsoRepository _repo;

    public QslSyncRecorderConcurrencyTests()
    {
        _repo = new LiteQsoRepository(_fixture.Context);
    }

    public void Dispose() => _fixture.Dispose();

    [Fact]
    public async Task ConcurrentCallbacksAllSurviveInTheLedger()
    {
        var qso = await _repo.CreateAsync(new Qso
        {
            Callsign = "W1AW",
            Band = "20m",
            Mode = "FT8",
            QsoDate = DateTime.UtcNow.Date,
            TimeOn = "1200",
        });

        // Distinct recorder instances, as production has: each background task
        // resolves its own scope, so instance-level locking would not help.
        // Repeated because the interleaving is timing-dependent — one clean
        // pass proves little, 25 collisions in a row prove the lock.
        for (var round = 0; round < 25; round++)
        {
            await _repo.UpdateQslSyncAsync(qso.Id, new QslSyncLedger());

            await Task.WhenAll(
                Task.Run(() => new QslSyncRecorder(new LiteQsoRepository(_fixture.Context), NullLogger<QslSyncRecorder>.Instance)
                    .RecordAsync(qso.Id, QslSyncLedger.ClubLogKey, QslUploadResult.Success())),
                Task.Run(() => new QslSyncRecorder(new LiteQsoRepository(_fixture.Context), NullLogger<QslSyncRecorder>.Instance)
                    .RecordAsync(qso.Id, QslSyncLedger.HrdLogKey, QslUploadResult.Success())),
                Task.Run(() => new QslSyncRecorder(new LiteQsoRepository(_fixture.Context), NullLogger<QslSyncRecorder>.Instance)
                    .RecordAsync(qso.Id, QslSyncLedger.EqslKey,
                        QslUploadResult.Fail(QslFailureKind.Temporary, "busy"))));

            var ledger = (await _repo.GetByIdAsync(qso.Id))!.QslSync!;
            ledger.ClubLog.Should().NotBeNull($"round {round}: Club Log's entry was overwritten by a racing callback");
            ledger.HrdLog.Should().NotBeNull($"round {round}: HRDLog's entry was overwritten by a racing callback");
            ledger.Eqsl.Should().NotBeNull($"round {round}: eQSL's entry was overwritten by a racing callback");

            ledger.ClubLog!.Status.Should().Be(SyncStatus.Synced);
            ledger.Eqsl!.FailureKind.Should().Be(QslFailureKind.Temporary);
        }
    }
}
