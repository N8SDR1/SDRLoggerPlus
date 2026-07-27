using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Core.Database.Remote;
using SDRLoggerPlus.Server.Core.Security;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Core.Database;

public enum DatabaseProvider
{
    /// <summary>This machine owns the log in a local LiteDB file (the default; also the log HOST).</summary>
    Local,

    /// <summary>
    /// This machine is a field CLIENT: its QSO log lives on a remote host, reached over HTTP+token
    /// (multi-op shared logging). Everything else (settings, layout) stays local. Requires HostUrl.
    /// </summary>
    RemoteHost
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
        // Singleton because the repository and the awards service are scoped:
        // the shared award snapshot has to outlive a request to be worth
        // anything. Invalidated by every QSO write (LiteQsoRepository.Commit).
        services.AddSingleton<QsoSnapshotCache>();

        // The QSO log is the one thing that goes remote in client mode; everything else (settings,
        // layout, radio config, contest sessions) stays local per machine. On the host — and on any
        // normal single-machine install — it's the local LiteDB.
        if (config.Provider == DatabaseProvider.RemoteHost && !string.IsNullOrWhiteSpace(config.HostUrl))
        {
            var baseUrl = config.HostUrl!.TrimEnd('/') + "/";
            services.AddHttpClient<IQsoRepository, RemoteApiQsoRepository>(client =>
            {
                client.BaseAddress = new Uri(baseUrl);
                if (!string.IsNullOrWhiteSpace(config.HostToken))
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", config.HostToken);
                client.Timeout = TimeSpan.FromSeconds(30);
            });
        }
        else
        {
            services.AddScoped<IQsoRepository, LiteQsoRepository>();
        }

        services.AddScoped<ISettingsRepository, LiteSettingsRepository>();
        services.AddScoped<ICallsignImageRepository, LiteCallsignImageRepository>();
        services.AddScoped<IRadioConfigRepository, LiteRadioConfigRepository>();
        services.AddScoped<IContestSessionRepository, LiteContestSessionRepository>();
        return services;
    }
}
