using SDRLoggerPlus.Server.Core.Database.LiteDb;
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
        services.AddSingleton<LiteDbContext>();
        services.AddSingleton<IDbContext>(sp => sp.GetRequiredService<LiteDbContext>());
        services.AddScoped<IQsoRepository, LiteQsoRepository>();
        services.AddScoped<ISettingsRepository, LiteSettingsRepository>();
        services.AddScoped<ICallsignImageRepository, LiteCallsignImageRepository>();
        services.AddScoped<IRadioConfigRepository, LiteRadioConfigRepository>();
        return services;
    }
}
