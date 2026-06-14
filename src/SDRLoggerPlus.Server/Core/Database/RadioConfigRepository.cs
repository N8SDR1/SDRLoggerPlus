using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Core.Database;

public interface IRadioConfigRepository
{
    Task<List<RadioConfigEntity>> GetAllAsync();
    Task<RadioConfigEntity?> GetByRadioIdAsync(string radioId);
    Task<List<RadioConfigEntity>> GetByTypeAsync(string radioType);
    Task UpsertByRadioIdAsync(RadioConfigEntity entity);
    Task<bool> DeleteByRadioIdAsync(string radioId);

    /// <summary>
    /// One-time migration: move old hamlib_config doc from settings collection to radio_configs.
    /// Returns true if migration was performed.
    /// </summary>
    Task<bool> MigrateOldHamlibConfigAsync();

    /// <summary>
    /// Self-healing: fix any radio_configs docs that have _id: null from a previous bug.
    /// Re-inserts them with a proper auto-generated _id.
    /// </summary>
    Task FixNullIdsAsync();
}
