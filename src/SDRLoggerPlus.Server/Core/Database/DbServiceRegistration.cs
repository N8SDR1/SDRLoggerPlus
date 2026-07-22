using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Core.Security;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Core.Database;

public enum DatabaseProvider
{
    Local
}

public static class DbServiceRegistration
{
    public static IServiceCollection AddDatabase(
        this IServiceCollection services,
        UserConfig config)
    {
        // LiteDB is the sole supported provider. config.Provider is retained for
        // backward compatibility with older config.json files but always resolves
        // to the local LiteDB implementation.
        // Credential protection at the settings-storage boundary. Singleton so
        // the AES fallback's key file is read once rather than per request.
        services.AddSingleton<ISecretProtector>(sp =>
        {
            var configPath = sp.GetRequiredService<IUserConfigService>().GetConfigPath();
            return new SecretProtector(
                Path.GetDirectoryName(configPath)!,
                sp.GetRequiredService<ILogger<SecretProtector>>());
        });

        services.AddSingleton<LiteDbContext>();
        services.AddSingleton<IDbContext>(sp => sp.GetRequiredService<LiteDbContext>());
        services.AddScoped<IQsoRepository, LiteQsoRepository>();
        services.AddScoped<ISettingsRepository, LiteSettingsRepository>();
        services.AddScoped<ICallsignImageRepository, LiteCallsignImageRepository>();
        services.AddScoped<IRadioConfigRepository, LiteRadioConfigRepository>();
        services.AddScoped<IContestSessionRepository, LiteContestSessionRepository>();
        return services;
    }
}
