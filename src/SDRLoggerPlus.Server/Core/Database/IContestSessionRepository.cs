using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Core.Database;

public interface IContestSessionRepository
{
    Task<List<ContestSession>> GetAllAsync();
    Task<ContestSession?> GetByIdAsync(string id);
    Task<ContestSession?> GetActiveAsync();
    Task UpsertAsync(ContestSession session);

    /// <summary>Clear the Active flag on every session (used before activating one).</summary>
    Task DeactivateAllAsync();

    Task<bool> DeleteAsync(string id);
}
