using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services.Qsl;

/// <summary>
/// Writes the outcome of a QSL upload onto the QSO it belongs to.
///
/// Before this existed, QsoService fired the three uploads with
/// <c>_ = Task.Run(...)</c> and dropped the returned result on the floor. The
/// services had always reported precisely what happened; nothing listened. The
/// cost was proven by the June frequency incident: months of QSOs went to
/// three services with wrong frequencies and the damage was unrepairable,
/// because nothing recorded what had been sent.
/// </summary>
public class QslSyncRecorder
{
    private readonly IQsoRepository _repository;
    private readonly ILogger<QslSyncRecorder> _logger;

    public QslSyncRecorder(IQsoRepository repository, ILogger<QslSyncRecorder> logger)
    {
        _repository = repository;
        _logger = logger;
    }

    /// <summary>
    /// Record one attempt. Returns false when nothing was written, which is the
    /// normal case for a service the operator has not configured.
    /// </summary>
    public async Task<bool> RecordAsync(string qsoId, string service, QslUploadResult result)
    {
        // A disabled service is not a failure. Recording it would paint every
        // QSO with red marks for services the operator never turned on.
        if (!result.IsRecordable) return false;

        var qso = await _repository.GetByIdAsync(qsoId);
        if (qso == null)
        {
            // The QSO was deleted between upload and callback — a real race on
            // a mis-logged contact the operator removed immediately.
            _logger.LogDebug("QSL ledger: QSO {QsoId} no longer exists; {Service} result dropped", qsoId, service);
            return false;
        }

        var ledger = qso.QslSync ??= new QslSyncLedger();
        var entry = ledger.For(service) ?? new QslServiceSync();
        var now = DateTime.UtcNow;

        entry.LastAttemptAt = now;
        if (result.Ok)
        {
            entry.Status = SyncStatus.Synced;
            entry.SyncedAt = now;
            entry.LastError = null;
            entry.FailureKind = QslFailureKind.None;
            entry.Attempts = 0;
        }
        else
        {
            // SyncedAt is deliberately left alone: if this QSO did reach the
            // service last week, a failure today does not undo that.
            entry.Status = SyncStatus.NotSynced;
            entry.LastError = Truncate(result.Error);
            entry.FailureKind = result.Kind;
            entry.Attempts++;
        }

        ledger.Set(service, entry);
        await _repository.UpdateQslSyncAsync(qso.Id, ledger);

        if (result.Ok)
            _logger.LogDebug("QSL ledger: {Service} synced {Call}", service, qso.Callsign);
        else
            _logger.LogWarning("QSL ledger: {Service} failed for {Call} ({Kind}) — {Error}",
                service, qso.Callsign, result.Kind, result.Error);

        return true;
    }

    /// <summary>
    /// Error bodies can be whole HTML pages (eQSL answers 200 with markup).
    /// The ledger is a status record, not a response archive.
    /// </summary>
    private static string? Truncate(string? error) =>
        error is { Length: > 300 } ? error[..300] : error;
}
