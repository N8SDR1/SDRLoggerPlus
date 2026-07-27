using System.Text.Json;
using System.Text.Json.Serialization;
using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services;

public interface IUserConfigService
{
    Task<UserConfig> GetConfigAsync();
    Task SaveConfigAsync(UserConfig config);
    bool IsConfigured();
    string GetConfigPath();
}

public class UserConfig
{
    [JsonConverter(typeof(TolerantDatabaseProviderConverter))]
    public DatabaseProvider Provider { get; set; } = DatabaseProvider.Local;
    public string? LocalDbPath { get; set; }
    public DateTime? ConfiguredAt { get; set; }

    // Multi-op field-CLIENT connection (Provider = RemoteHost): the shared-log host's base URL
    // (e.g. http://192.168.1.50:5050) and this device's access token. Ignored for a Local/host install.
    public string? HostUrl { get; set; }
    public string? HostToken { get; set; }

    // HOST this log for other stations on the network. When true (and at least one device token
    // exists), the launcher binds the backend to the LAN (0.0.0.0) instead of localhost. Default
    // false = a normal single-machine install stays localhost-only, exposing nothing.
    public bool ShareOnNetwork { get; set; }
}

/// <summary>
/// Deserializes the DB provider tolerantly: a config.json written by an older
/// build may name a provider this enum no longer has (e.g. the removed "MongoDb").
/// Any unknown/legacy value resolves to Local instead of throwing, so upgrading
/// never breaks config load and the rest of the config (e.g. ConfiguredAt) is
/// preserved.
/// </summary>
public class TolerantDatabaseProviderConverter : JsonConverter<DatabaseProvider>
{
    public override DatabaseProvider Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.String
            && Enum.TryParse<DatabaseProvider>(reader.GetString(), ignoreCase: true, out var provider)
            && Enum.IsDefined(provider))
        {
            return provider;
        }
        return DatabaseProvider.Local;
    }

    public override void Write(Utf8JsonWriter writer, DatabaseProvider value, JsonSerializerOptions options)
        => writer.WriteStringValue(value.ToString());
}

public class UserConfigService : IUserConfigService
{
    private readonly string _configPath;
    private readonly ILogger<UserConfigService> _logger;
    private UserConfig? _cachedConfig;

    public UserConfigService(ILogger<UserConfigService> logger, string? configPath = null)
    {
        _logger = logger;
        _configPath = configPath ?? GetPlatformConfigPath();
        _logger.LogInformation("User config path: {ConfigPath}", _configPath);

        // One-time migration of data from a previous Log4YM installation
        var configDir = Path.GetDirectoryName(_configPath);
        if (!string.IsNullOrEmpty(configDir))
            LegacyMigration.MigrateIfNeeded(configDir, msg => _logger.LogInformation("{Migration}", msg));
    }

    private static string GetPlatformConfigPath()
    {
        string configDir;

        if (OperatingSystem.IsMacOS())
        {
            configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                "Library", "Application Support", "SDRLoggerPlus");
        }
        else if (OperatingSystem.IsWindows())
        {
            configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "SDRLoggerPlus");
        }
        else // Linux
        {
            configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                ".config", "SDRLoggerPlus");
        }

        return Path.Combine(configDir, "config.json");
    }

    public string GetConfigPath() => _configPath;

    public bool IsConfigured()
    {
        // LiteDB is always available, so "configured" simply means a config has
        // been loaded/saved or a config.json already exists on disk.
        if (_cachedConfig != null)
            return true;

        if (!File.Exists(_configPath))
            return false;

        try
        {
            return JsonSerializer.Deserialize<UserConfig>(File.ReadAllText(_configPath)) != null;
        }
        catch
        {
            return false;
        }
    }

    public async Task<UserConfig> GetConfigAsync()
    {
        if (_cachedConfig != null)
            return _cachedConfig;

        if (!File.Exists(_configPath))
        {
            _cachedConfig = new UserConfig();
            return _cachedConfig;
        }

        try
        {
            var json = await File.ReadAllTextAsync(_configPath);
            _cachedConfig = JsonSerializer.Deserialize<UserConfig>(json) ?? new UserConfig();
            return _cachedConfig;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read config from {Path}", _configPath);
            _cachedConfig = new UserConfig();
            return _cachedConfig;
        }
    }

    public async Task SaveConfigAsync(UserConfig config)
    {
        var directory = Path.GetDirectoryName(_configPath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        config.ConfiguredAt = DateTime.UtcNow;

        var json = JsonSerializer.Serialize(config, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        await File.WriteAllTextAsync(_configPath, json);
        _cachedConfig = config;

        _logger.LogInformation("Configuration saved to {Path}", _configPath);
    }
}
