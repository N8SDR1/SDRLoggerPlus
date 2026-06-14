using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Core.Database;

public interface ISettingsRepository
{
    Task<UserSettings?> GetAsync(string id = "default");
    Task<UserSettings> UpsertAsync(UserSettings settings);
}
