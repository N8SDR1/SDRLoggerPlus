using LiteDB;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Core.Database.Migrations;
using SDRLoggerPlus.Server.Core.Security;
using SDRLoggerPlus.Server.Services;
using Serilog;
using Serilog.Extensions.Logging;

namespace SDRLoggerPlus.Server.Core.Database.LiteDb;

public class LiteDbContext : IDbContext, IDisposable
{
    private LiteDatabase? _database;
    private readonly IUserConfigService _userConfigService;
    private readonly BsonMapper _mapper;
    private readonly object _initLock = new();
    private bool _isInitialized;
    private string? _dbPath;

    public LiteDbContext(IUserConfigService userConfigService)
    {
        _userConfigService = userConfigService;

        // Create a dedicated mapper and pre-warm entity registrations to avoid
        // concurrent first-access races during startup deserialization
        _mapper = new BsonMapper();

        // Canonical time frame = UTC, everywhere (see docs/design/timezone-architecture.md).
        //
        // LiteDB 5.0.21 stores every DateTime as the correct UTC instant on disk, but on read
        // it converts to the server's LOCAL time and returns Kind=Local — and the connection-string
        // `UtcDate=true` flag is inert in this version (proven in LiteDbDateTimeRoundTripProbe and
        // commit 1daa146). That local re-projection is the root of years of off-by-a-day bugs:
        // an evening QSO reads back on the previous calendar day, so the import dedupe key misses
        // it and it re-imports. Fix it once, at the boundary, by projecting every DateTime back to
        // Kind=Utc on read. .ToUniversalTime() recovers the exact stored instant regardless of the
        // Kind LiteDB hands us (no-op if already Utc, correct conversion if Local) — the on-disk
        // bytes are unchanged, so this is a read-projection, NOT a data migration, and it cannot
        // re-stamp or re-queue any QSO for upload (upload selection keys on sync flags, never on
        // QsoDate — see the timezone doc §5).
        _mapper.RegisterType<DateTime>(
            serialize: dt => dt.ToUniversalTime(),
            deserialize: bson => bson.AsDateTime.ToUniversalTime());
        _mapper.RegisterType<DateTime?>(
            serialize: dt => dt.HasValue ? dt.Value.ToUniversalTime() : BsonValue.Null,
            deserialize: bson => bson.IsNull ? (DateTime?)null : bson.AsDateTime.ToUniversalTime());

        // Register custom serializer for MongoDB.Bson.BsonDocument so that the
        // Qso.AdifExtra property (which stores unmapped ADIF fields) can be
        // persisted in LiteDB without type-cast errors between MongoDB BSON types.
        _mapper.RegisterType<MongoDB.Bson.BsonDocument>(
            serialize: mongoDoc =>
            {
                var liteDoc = new BsonDocument();
                foreach (var element in mongoDoc)
                {
                    liteDoc[element.Name] = new LiteDB.BsonValue(element.Value?.ToString() ?? string.Empty);
                }
                return liteDoc;
            },
            deserialize: bson =>
            {
                var mongoDoc = new MongoDB.Bson.BsonDocument();
                if (bson is BsonDocument liteDoc)
                {
                    foreach (var kvp in liteDoc)
                    {
                        if (kvp.Key != "_type")
                            mongoDoc[kvp.Key] = kvp.Value.AsString;
                    }
                }
                return mongoDoc;
            }
        );

        _mapper.Entity<UserSettings>();
        _mapper.Entity<Qso>();
        _mapper.Entity<CallsignMapImage>();
        _mapper.Entity<RadioConfigEntity>();
        _mapper.Entity<ContestSession>();

        TryInitialize();
    }

    public bool IsConnected => _isInitialized && _database != null;

    public string? DatabaseName => _dbPath != null ? Path.GetFileName(_dbPath) : null;

    /// <summary>Full path of the LiteDB file, or null before initialization.</summary>
    public string? DatabaseFilePath => _dbPath;

    public bool TryInitialize()
    {
        if (_isInitialized) return true;

        lock (_initLock)
        {
            if (_isInitialized) return true;

            try
            {
                _dbPath = GetDatabasePath();
                var directory = Path.GetDirectoryName(_dbPath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                Log.Information("LiteDB opening database at: {DbPath}", _dbPath);

                // Use Shared mode with WAL journal. Writes go to the WAL
                // (durable on disk), then Checkpoint() in each repository
                // merges them into the main file. This survives SIGKILL because
                // WAL recovery replays committed transactions on next startup.
                _database = new LiteDatabase($"Filename={_dbPath};Connection=shared", _mapper);
                _isInitialized = true;

                CreateIndexes();
                RunMigrations();

                Log.Information("LiteDB initialized successfully");
                return true;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to initialize LiteDB");
                _database?.Dispose();
                _database = null;
                return false;
            }
        }
    }

    public async Task<bool> ReinitializeAsync(string connectionString, string databaseName)
    {
        lock (_initLock)
        {
            _isInitialized = false;
            _database?.Dispose();
            _database = null;
            _dbPath = null;
        }

        await _userConfigService.SaveConfigAsync(new UserConfig
        {
            Provider = DatabaseProvider.Local,
        });

        return TryInitialize();
    }

    internal LiteDatabase Database
    {
        get
        {
            EnsureConnected();
            return _database!;
        }
    }

    internal ILiteCollection<Qso> Qsos
    {
        get { EnsureConnected(); return _database!.GetCollection<Qso>("qsos"); }
    }

    internal ILiteCollection<UserSettings> Settings
    {
        get { EnsureConnected(); return _database!.GetCollection<UserSettings>("settings"); }
    }


    internal ILiteCollection<CallsignMapImage> CallsignMapImages
    {
        get { EnsureConnected(); return _database!.GetCollection<CallsignMapImage>("callsign_images"); }
    }

    internal ILiteCollection<RadioConfigEntity> RadioConfigs
    {
        get { EnsureConnected(); return _database!.GetCollection<RadioConfigEntity>("radio_configs"); }
    }

    internal ILiteCollection<ContestSession> ContestSessions
    {
        get { EnsureConnected(); return _database!.GetCollection<ContestSession>("contest_sessions"); }
    }

    private void EnsureConnected()
    {
        if (!_isInitialized || _database == null)
        {
            throw new InvalidOperationException(
                "LiteDB is not initialized. Please complete the setup wizard.");
        }
    }

    private string GetDatabasePath()
    {
        var configPath = _userConfigService.GetConfigPath();
        var configDir = Path.GetDirectoryName(configPath)!;
        return Path.Combine(configDir, "sdrloggerplus.db");
    }

    /// <summary>
    /// Apply pending schema/data migrations. Runs after the indexes exist (a
    /// migration may scan a collection) and before any repository can read, so
    /// no caller ever observes half-repaired data.
    ///
    /// Migration failure must not stop the app from opening — see
    /// MigrationRunner — so this only guards against the runner itself
    /// throwing, which would mean a programming error such as duplicate
    /// version numbers.
    /// </summary>
    private void RunMigrations()
    {
        try
        {
            var loggerFactory = new SerilogLoggerFactory(Log.Logger);
            // Built here rather than injected: LiteDbContext is constructed
            // during DI setup, and a credential migration must use the same
            // protector the settings repository will later read back with.
            var protector = new SecretProtector(
                Path.GetDirectoryName(_dbPath)!,
                loggerFactory.CreateLogger<SecretProtector>());

            var runner = new MigrationRunner(
                MigrationCatalog.Build(protector), loggerFactory.CreateLogger("Migrations"));
            runner.Run(_database!, _dbPath);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Migration runner could not start; database left unmigrated");
        }
    }

    private void CreateIndexes()
    {
        // QSO indexes
        Qsos.EnsureIndex(q => q.Callsign);
        Qsos.EnsureIndex(q => q.QsoDate);
        Qsos.EnsureIndex(q => q.Band);
        Qsos.EnsureIndex(q => q.Mode);
        Qsos.EnsureIndex(q => q.QrzSyncStatus);

        // Composite index for duplicate detection during ADIF import
        // This significantly speeds up ExistsAsync queries
        Qsos.EnsureIndex("idx_duplicate_check",
            "$.Callsign + '|' + $.QsoDate + '|' + $.TimeOn + '|' + $.Band + '|' + $.Mode");

        // Callsign map image indexes
        CallsignMapImages.EnsureIndex(i => i.Callsign, true);
        CallsignMapImages.EnsureIndex(i => i.SavedAt);

        // Radio config indexes
        RadioConfigs.EnsureIndex(r => r.RadioId, true);
        RadioConfigs.EnsureIndex(r => r.RadioType);

        // Contest session indexes
        ContestSessions.EnsureIndex(s => s.Active);
        ContestSessions.EnsureIndex(s => s.DefinitionId);
    }

    public void Dispose()
    {
        _database?.Dispose();
        _database = null;
        _isInitialized = false;
    }
}
