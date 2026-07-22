using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

public interface IQsoService
{
    Task<QsoResponse?> GetByIdAsync(string id);
    Task<PaginatedQsoResponse> GetQsosAsync(QsoSearchRequest request);
    Task<QsoResponse> CreateAsync(CreateQsoRequest request);
    Task<QsoResponse?> UpdateAsync(string id, UpdateQsoRequest request);
    Task<bool> DeleteAsync(string id);
    Task<int> DeleteManyAsync(IEnumerable<string> ids);
    Task<QsoStatistics> GetStatisticsAsync();

    /// <summary>
    /// The QSO that makes (callsign, band, mode) a probable duplicate right
    /// now — same triple entered into the log within the dupe window — or
    /// null. Advisory only: logging is never blocked.
    /// </summary>
    Task<QsoResponse?> CheckRecentDupeAsync(string callsign, string band, string mode);
}
