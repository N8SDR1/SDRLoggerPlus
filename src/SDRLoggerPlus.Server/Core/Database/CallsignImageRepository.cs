using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Core.Database;

public interface ICallsignImageRepository
{
    Task UpsertAsync(CallsignMapImage image);
    Task<List<CallsignMapImage>> GetRecentAsync(int limit);
}
