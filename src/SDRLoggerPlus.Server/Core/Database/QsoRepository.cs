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
    Task<QsoStatistics> GetStatisticsAsync();
    Task<int> GetCountAsync();
    Task<bool> ExistsAsync(string callsign, DateTime qsoDate, string timeOn, string band, string mode);
    Task<IEnumerable<Qso>> GetByIdsAsync(IEnumerable<string> ids);

    /// <summary>All QSOs logged under a contest session, oldest first (engine replay order).</summary>
    Task<List<Qso>> GetByContestSessionAsync(string sessionId);

    /// <summary>Distinct callsigns across the whole log (for SCP / call-history seeding).</summary>
    Task<List<string>> GetDistinctCallsignsAsync();

    /// <summary>Most recent QSO with a callsign (for exchange prefill), or null.</summary>
    Task<Qso?> GetMostRecentByCallsignAsync(string callsign);
    Task<bool> UpdateQrzSyncStatusAsync(string id, string qrzLogId);
    Task<int> GetPendingSyncCountAsync();
    Task<long> DeleteAllAsync();
}
