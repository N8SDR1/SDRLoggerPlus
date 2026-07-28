using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Core.Database;

public interface IQsoRepository
{
    Task<Qso?> GetByIdAsync(string id);
    Task<IEnumerable<Qso>> GetRecentAsync(int limit = 100);
    Task<IEnumerable<Qso>> GetAllAsync();
    Task<IEnumerable<Qso>> GetUnsyncedToQrzAsync();
    Task<(IEnumerable<Qso> Items, int TotalCount)> SearchAsync(QsoSearchRequest criteria);
    Task<Qso> CreateAsync(Qso qso);
    Task<IEnumerable<Qso>> CreateBulkAsync(IEnumerable<Qso> qsos);
    Task<bool> UpdateAsync(string id, Qso qso);
    Task<bool> DeleteAsync(string id);

    /// <summary>
    /// Deletes several QSOs in one pass, returning how many actually existed.
    /// Separate from looping <see cref="DeleteAsync"/> so the batch costs one
    /// database checkpoint instead of one per row.
    /// </summary>
    Task<int> DeleteManyAsync(IEnumerable<string> ids);
    Task<QsoStatistics> GetStatisticsAsync(string? myCall = null);
    Task<int> GetCountAsync();
    Task<bool> ExistsAsync(string callsign, DateTime qsoDate, string timeOn, string band, string mode);
    Task<IEnumerable<Qso>> GetByIdsAsync(IEnumerable<string> ids);

    /// <summary>All QSOs logged under a contest session, oldest first (engine replay order).</summary>
    Task<List<Qso>> GetByContestSessionAsync(string sessionId);

    /// <summary>Distinct callsigns across the whole log (for SCP / call-history seeding).</summary>
    Task<List<string>> GetDistinctCallsignsAsync();

    /// <summary>Most recent QSO with a callsign (for exchange prefill), or null.</summary>
    Task<Qso?> GetMostRecentByCallsignAsync(string callsign);

    /// <summary>
    /// Most recent QSO with the same callsign + band + mode entered into the
    /// log since <paramref name="createdSinceUtc"/>, or null. Windows on
    /// CreatedAt (when the row was written), not the QSO's own date/time — a
    /// duplicate is by definition something the operator just entered, and
    /// CreatedAt is always UTC regardless of the QsoDate kind quirks.
    /// </summary>
    Task<Qso?> FindRecentDuplicateAsync(string callsign, string band, string mode, DateTime createdSinceUtc);
    Task<bool> UpdateQrzSyncStatusAsync(string id, string qrzLogId);
    Task<int> GetPendingSyncCountAsync();

    /// <summary>
    /// Mark every NotSynced/Modified QSO as already synced to QRZ, without uploading anything.
    /// For operators who imported their existing QRZ log and don't want those QSOs re-uploaded
    /// as duplicates. Returns the number of QSOs marked.
    /// </summary>
    Task<int> MarkAllQrzSyncedAsync();

    /// <summary>
    /// Persist the Club Log / HRDLog / eQSL ledger for one QSO without
    /// touching any other field. Separate from UpdateAsync on purpose — see
    /// the implementation for why.
    /// </summary>
    Task<bool> UpdateQslSyncAsync(string id, QslSyncLedger ledger);

    /// <summary>QSOs whose ledger records a retryable failure for the given service.</summary>
    Task<IEnumerable<Qso>> GetQslFailuresAsync(string service);
    Task<long> DeleteAllAsync();

    /// <summary>
    /// Repair ONLY the QsoDate instant, preserving every sync flag and NOT bumping UpdatedAt.
    /// Deliberately bypasses UpdateAsync (which would flip QrzSyncStatus Synced→Modified and
    /// re-queue a QRZ upload) — a maintenance time-repair must never re-upload. Used by the
    /// "Verify QSO times" tool (docs/design/timezone-architecture.md §5a).
    /// </summary>
    Task<bool> RepairQsoDateAsync(string id, DateTime qsoDateUtc);
}
