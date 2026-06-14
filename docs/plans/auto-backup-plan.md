# Scheduled Auto-Backup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scheduled logbook backups (daily/weekly/on-exit) with keep-last-N retention, each run bundling a LiteDB file copy + ADIF export into a timestamped folder. Spec: `docs/design/auto-backup-design.md`.

**Architecture:** A pure `BackupSchedule` class (due-time math), a `BackupStateStore` (persists last-run state to `backup-state.json`), a `BackupRunner` (one backup pass: folder, DB copy, ADIF export, prune), and a thin `BackupService` `IHostedService` (60 s timer + on-exit hook). `BackupController` exposes status/run-now. Settings ride on `UserSettings.Backup`.

**Tech Stack:** .NET 10, xUnit + Moq + FluentAssertions (Category=Unit), React/zustand settings store, Vitest.

**Conventions observed in this repo:** file-scoped namespaces; services in `src/SDRLoggerPlus.Server/Services/`; DTOs in `src/SDRLoggerPlus.Contracts/Api/`; tests in `src/SDRLoggerPlus.Server.Tests/Tests/Services/` with `[Trait("Category", "Unit")]`; frontend settings interfaces in `src/SDRLoggerPlus.Web/src/store/settingsStore.ts`.

---

### Task 1: BackupSettings model

**Files:**
- Modify: `src/SDRLoggerPlus.Contracts/Models/Settings.cs` (add section property + class)

- [ ] **Step 1: Add `Backup` to `UserSettings`** — next to the other section properties:

```csharp
    [BsonElement("backup")]
    public BackupSettings Backup { get; set; } = new();
```

- [ ] **Step 2: Add the settings class** — at the end of the file:

```csharp
public class BackupSettings
{
    [BsonElement("enabled")]
    public bool Enabled { get; set; }

    /// <summary>"daily" | "weekly" | "on_exit"</summary>
    [BsonElement("interval")]
    public string Interval { get; set; } = "daily";

    [BsonElement("retention")]
    public int Retention { get; set; } = 10;

    /// <summary>Empty/null → default: &lt;config dir&gt;/backups</summary>
    [BsonElement("destinationPath")]
    public string? DestinationPath { get; set; }
}
```

- [ ] **Step 3: Build**

Run: `dotnet build src/SDRLoggerPlus.Server`
Expected: success.

- [ ] **Step 4: Commit** — `feat(backup): add BackupSettings to UserSettings`

---

### Task 2: BackupSchedule (pure due-time math)

**Files:**
- Create: `src/SDRLoggerPlus.Server/Services/Backup/BackupSchedule.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/BackupScheduleTests.cs`

- [ ] **Step 1: Write failing tests**

```csharp
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Backup;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BackupScheduleTests
{
    private static readonly DateTime Now = new(2026, 6, 10, 12, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("daily", 1)]
    [InlineData("weekly", 7)]
    public void NextDue_AnchorsToLastRun(string interval, int days)
    {
        var last = Now.AddHours(-2);
        BackupSchedule.NextDue(interval, last).Should().Be(last.AddDays(days));
    }

    [Fact]
    public void NextDue_OnExit_ReturnsNull()
        => BackupSchedule.NextDue("on_exit", Now).Should().BeNull();

    [Fact]
    public void NextDue_NoHistory_IsDueImmediately()
        => BackupSchedule.NextDue("daily", null).Should().BeNull();

    [Fact]
    public void IsDue_NoHistory_True()
        => BackupSchedule.IsDue("daily", null, Now).Should().BeTrue();

    [Fact]
    public void IsDue_RanRecently_False()
        => BackupSchedule.IsDue("daily", Now.AddHours(-23), Now).Should().BeFalse();

    [Fact]
    public void IsDue_IntervalElapsed_True()
        => BackupSchedule.IsDue("daily", Now.AddHours(-25), Now).Should().BeTrue();

    [Fact]
    public void IsDue_OnExit_NeverDueFromTimer()
        => BackupSchedule.IsDue("on_exit", null, Now).Should().BeFalse();

    [Fact]
    public void IsDue_UnknownInterval_TreatedAsDaily()
        => BackupSchedule.IsDue("bogus", Now.AddHours(-25), Now).Should().BeTrue();
}
```

- [ ] **Step 2: Run to verify failure**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~BackupScheduleTests"`
Expected: compile error (class missing).

- [ ] **Step 3: Implement**

```csharp
namespace SDRLoggerPlus.Server.Services.Backup;

/// <summary>
/// Due-time math for scheduled backups. Schedule anchors to the last
/// successful run (persisted across restarts), so a daily backup that
/// already fired today won't re-fire on app relaunch. No prior run →
/// due immediately. "on_exit" never fires from the timer.
/// </summary>
public static class BackupSchedule
{
    public static TimeSpan? IntervalOf(string interval) => interval switch
    {
        "on_exit" => null,
        "weekly" => TimeSpan.FromDays(7),
        _ => TimeSpan.FromDays(1),
    };

    public static DateTime? NextDue(string interval, DateTime? lastRunUtc)
    {
        var td = IntervalOf(interval);
        if (td is null || lastRunUtc is null) return null;
        return lastRunUtc.Value + td.Value;
    }

    public static bool IsDue(string interval, DateTime? lastRunUtc, DateTime nowUtc)
    {
        var td = IntervalOf(interval);
        if (td is null) return false;
        if (lastRunUtc is null) return true;
        return nowUtc - lastRunUtc.Value >= td.Value;
    }
}
```

- [ ] **Step 4: Run tests — expect PASS**

- [ ] **Step 5: Commit** — `feat(backup): add BackupSchedule due-time math`

---

### Task 3: BackupStateStore (persist last-run state)

**Files:**
- Create: `src/SDRLoggerPlus.Server/Services/Backup/BackupStateStore.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/BackupStateStoreTests.cs`

- [ ] **Step 1: Write failing tests**

```csharp
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Backup;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BackupStateStoreTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "sdrloggerplus-test-" + Guid.NewGuid());

    public BackupStateStoreTests() => Directory.CreateDirectory(_dir);
    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string StatePath => Path.Combine(_dir, "backup-state.json");

    [Fact]
    public void Load_NoFile_ReturnsEmptyState()
    {
        var state = new BackupStateStore(StatePath).Load();
        state.LastRunUtc.Should().BeNull();
        state.Ok.Should().BeNull();
    }

    [Fact]
    public void SaveThenLoad_RoundTrips()
    {
        var store = new BackupStateStore(StatePath);
        var state = new BackupState
        {
            LastRunUtc = new DateTime(2026, 6, 10, 3, 0, 0, DateTimeKind.Utc),
            Ok = true,
            Message = "Wrote 2 file(s)",
            Path = @"C:\backups\SDRLoggerPlus-2026-06-10_0300",
        };
        store.Save(state);

        var loaded = new BackupStateStore(StatePath).Load();
        loaded.LastRunUtc.Should().Be(state.LastRunUtc);
        loaded.Ok.Should().BeTrue();
        loaded.Message.Should().Be(state.Message);
        loaded.Path.Should().Be(state.Path);
    }

    [Fact]
    public void Load_CorruptFile_ReturnsEmptyState()
    {
        File.WriteAllText(StatePath, "{ not json");
        new BackupStateStore(StatePath).Load().LastRunUtc.Should().BeNull();
    }
}
```

- [ ] **Step 2: Run to verify failure** (compile error)

- [ ] **Step 3: Implement**

```csharp
using System.Text.Json;

namespace SDRLoggerPlus.Server.Services.Backup;

public class BackupState
{
    public DateTime? LastRunUtc { get; set; }
    public bool? Ok { get; set; }
    public string? Message { get; set; }
    public string? Path { get; set; }
}

/// <summary>
/// Persists backup state to a small JSON file next to the database so the
/// schedule anchor survives restarts without writing UserSettings every run.
/// </summary>
public class BackupStateStore
{
    private readonly string _path;

    public BackupStateStore(string path) => _path = path;

    public BackupState Load()
    {
        try
        {
            if (!File.Exists(_path)) return new BackupState();
            return JsonSerializer.Deserialize<BackupState>(File.ReadAllText(_path)) ?? new BackupState();
        }
        catch
        {
            return new BackupState();
        }
    }

    public void Save(BackupState state)
    {
        try
        {
            File.WriteAllText(_path, JsonSerializer.Serialize(state));
        }
        catch
        {
            // best effort — a failed state write must never break a backup run
        }
    }
}
```

- [ ] **Step 4: Run tests — expect PASS**

- [ ] **Step 5: Commit** — `feat(backup): add BackupStateStore`

---

### Task 4: Expose the LiteDB file path

**Files:**
- Modify: `src/SDRLoggerPlus.Server/Core/Database/LiteDb/LiteDbContext.cs`

- [ ] **Step 1: Add a public property** (the private `_dbPath` already exists; set in `TryInitialize`):

```csharp
    /// <summary>Full path of the LiteDB file, or null before initialization.</summary>
    public string? DatabaseFilePath => _dbPath;
```

- [ ] **Step 2: Build, commit** — `feat(backup): expose LiteDB file path`

---

### Task 5: BackupRunner (one backup pass)

**Files:**
- Create: `src/SDRLoggerPlus.Server/Services/Backup/BackupRunner.cs`
- Create: `src/SDRLoggerPlus.Contracts/Api/BackupDto.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/BackupRunnerTests.cs`

The runner takes everything as plain values/interfaces so tests need no DI: settings, source DB path (null when MongoDB), an ADIF-export delegate, destination resolver, and the state store.

- [ ] **Step 1: Create the DTO** (`src/SDRLoggerPlus.Contracts/Api/BackupDto.cs`):

```csharp
namespace SDRLoggerPlus.Contracts.Api;

public record BackupStatusDto(
    bool Enabled,
    string Interval,
    int Retention,
    string Destination,
    DateTime? LastRunUtc,
    bool? Ok,
    string? Message,
    string? Path,
    DateTime? NextDueUtc);

public record BackupRunResult(bool Ok, string Message, string? Path);
```

- [ ] **Step 2: Write failing tests**

```csharp
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Backup;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BackupRunnerTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "sdrloggerplus-bk-" + Guid.NewGuid());
    private readonly string _dest;
    private readonly string _dbFile;
    private readonly BackupStateStore _stateStore;

    public BackupRunnerTests()
    {
        Directory.CreateDirectory(_root);
        _dest = Path.Combine(_root, "backups");
        _dbFile = Path.Combine(_root, "sdrloggerplus.db");
        File.WriteAllText(_dbFile, "FAKE-LITEDB-CONTENT");
        _stateStore = new BackupStateStore(Path.Combine(_root, "backup-state.json"));
    }

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private BackupRunner CreateRunner(
        string? dbFile = "default",
        Func<Task<string>>? adifExport = null,
        int retention = 10)
    {
        return new BackupRunner(
            dbPath: dbFile == "default" ? _dbFile : dbFile,
            adifExport: adifExport ?? (() => Task.FromResult("<EOH>\n<CALL:4>W1AW <EOR>")),
            destinationRoot: _dest,
            retention: retention,
            stateStore: _stateStore,
            logger: NullLogger<BackupRunner>.Instance);
    }

    [Fact]
    public async Task Run_WritesDbCopyAndAdif_IntoTimestampedFolder()
    {
        var result = await CreateRunner().RunAsync("manual");

        result.Ok.Should().BeTrue();
        var folder = Directory.GetDirectories(_dest).Single();
        Path.GetFileName(folder).Should().StartWith("SDRLoggerPlus-");
        File.ReadAllText(Path.Combine(folder, "sdrloggerplus.db")).Should().Be("FAKE-LITEDB-CONTENT");
        File.ReadAllText(Path.Combine(folder, "sdrloggerplus.adi")).Should().Contain("W1AW");
    }

    [Fact]
    public async Task Run_MongoProvider_NoDbFile_WritesAdifOnly()
    {
        var result = await CreateRunner(dbFile: null).RunAsync("manual");

        result.Ok.Should().BeTrue();
        result.Message.Should().Contain("ADIF only");
        var folder = Directory.GetDirectories(_dest).Single();
        File.Exists(Path.Combine(folder, "sdrloggerplus.adi")).Should().BeTrue();
        File.Exists(Path.Combine(folder, "sdrloggerplus.db")).Should().BeFalse();
    }

    [Fact]
    public async Task Run_AdifExportFails_DbCopyStillSucceeds()
    {
        var result = await CreateRunner(
            adifExport: () => throw new InvalidOperationException("boom")).RunAsync("manual");

        result.Ok.Should().BeTrue(); // partial success is success
        result.Message.Should().Contain("boom");
        var folder = Directory.GetDirectories(_dest).Single();
        File.Exists(Path.Combine(folder, "sdrloggerplus.db")).Should().BeTrue();
    }

    [Fact]
    public async Task Run_NothingWritten_ReportsFailure_AndDoesNotPrune()
    {
        // Pre-existing old backup folders that must survive a failed run
        Directory.CreateDirectory(Path.Combine(_dest, "SDRLoggerPlus-2026-01-01_0000"));

        var result = await CreateRunner(
            dbFile: null,
            adifExport: () => throw new InvalidOperationException("boom"),
            retention: 1).RunAsync("scheduled");

        result.Ok.Should().BeFalse();
        Directory.Exists(Path.Combine(_dest, "SDRLoggerPlus-2026-01-01_0000")).Should().BeTrue();
    }

    [Fact]
    public async Task Run_PrunesOldestBeyondRetention_IgnoringForeignFolders()
    {
        var old1 = Path.Combine(_dest, "SDRLoggerPlus-2026-01-01_0000");
        var old2 = Path.Combine(_dest, "SDRLoggerPlus-2026-01-02_0000");
        var foreign = Path.Combine(_dest, "my-other-stuff");
        Directory.CreateDirectory(old1);
        Directory.CreateDirectory(old2);
        Directory.CreateDirectory(foreign);
        Directory.SetLastWriteTimeUtc(old1, DateTime.UtcNow.AddDays(-2));
        Directory.SetLastWriteTimeUtc(old2, DateTime.UtcNow.AddDays(-1));

        await CreateRunner(retention: 2).RunAsync("manual");

        Directory.Exists(old1).Should().BeFalse();   // oldest pruned
        Directory.Exists(old2).Should().BeTrue();    // kept (2nd newest)
        Directory.Exists(foreign).Should().BeTrue(); // never touched
        Directory.GetDirectories(_dest, "SDRLoggerPlus-*").Should().HaveCount(2);
    }

    [Fact]
    public async Task Run_UpdatesPersistedState()
    {
        await CreateRunner().RunAsync("manual");
        var state = _stateStore.Load();
        state.LastRunUtc.Should().NotBeNull();
        state.Ok.Should().BeTrue();
    }

    [Fact]
    public async Task Run_UncreatableDestination_ReportsFailure()
    {
        var badDest = Path.Combine(_dbFile, "impossible"); // parent is a file
        var runner = new BackupRunner(_dbFile, () => Task.FromResult(""), badDest, 10,
            _stateStore, NullLogger<BackupRunner>.Instance);

        var result = await runner.RunAsync("manual");
        result.Ok.Should().BeFalse();
    }
}
```

- [ ] **Step 3: Run to verify failure** (compile error)

- [ ] **Step 4: Implement**

```csharp
using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services.Backup;

/// <summary>
/// Executes one backup pass: timestamped folder + raw DB copy (LiteDB only)
/// + ADIF export, then retention prune. Prune runs ONLY after a successful
/// write, so a failed run never destroys prior backups (SDRLogger+ rule).
/// </summary>
public class BackupRunner
{
    public const string FolderPrefix = "SDRLoggerPlus-";

    private readonly string? _dbPath;            // null → MongoDB provider (ADIF only)
    private readonly Func<Task<string>> _adifExport;
    private readonly string _destinationRoot;
    private readonly int _retention;
    private readonly BackupStateStore _stateStore;
    private readonly ILogger<BackupRunner> _logger;

    public BackupRunner(string? dbPath, Func<Task<string>> adifExport, string destinationRoot,
        int retention, BackupStateStore stateStore, ILogger<BackupRunner> logger)
    {
        _dbPath = dbPath;
        _adifExport = adifExport;
        _destinationRoot = destinationRoot;
        _retention = retention;
        _stateStore = stateStore;
        _logger = logger;
    }

    public async Task<BackupRunResult> RunAsync(string trigger)
    {
        var now = DateTime.UtcNow;
        var folder = Path.Combine(_destinationRoot, $"{FolderPrefix}{now:yyyy-MM-dd_HHmm}");
        try
        {
            Directory.CreateDirectory(folder);
        }
        catch (Exception ex)
        {
            return Fail(now, folder, $"Cannot create backup folder: {ex.Message}", trigger);
        }

        var written = new List<string>();
        var failures = new List<string>();

        if (_dbPath != null)
        {
            try
            {
                if (File.Exists(_dbPath))
                {
                    File.Copy(_dbPath, Path.Combine(folder, Path.GetFileName(_dbPath)), overwrite: true);
                    written.Add(Path.GetFileName(_dbPath));
                }
                else
                {
                    failures.Add($"db: file not found ({_dbPath})");
                }
            }
            catch (Exception ex)
            {
                failures.Add($"db: {ex.Message}");
            }
        }

        try
        {
            var adif = await _adifExport();
            if (!string.IsNullOrEmpty(adif))
            {
                await File.WriteAllTextAsync(Path.Combine(folder, "sdrloggerplus.adi"), adif);
                written.Add("sdrloggerplus.adi");
            }
        }
        catch (Exception ex)
        {
            failures.Add($"adif: {ex.Message}");
        }

        if (written.Count == 0)
        {
            var msg = "No files written. " + string.Join(" ; ", failures);
            return Fail(now, folder, msg, trigger);
        }

        var pruned = Prune();

        var okMsg = $"Wrote {written.Count} file(s): {string.Join(", ", written)}";
        if (_dbPath == null) okMsg += " (ADIF only — MongoDB provider)";
        if (failures.Count > 0) okMsg += $" (partial — {string.Join(" ; ", failures)})";
        if (pruned > 0) okMsg += $" [pruned {pruned} old folder(s)]";

        _stateStore.Save(new BackupState { LastRunUtc = now, Ok = true, Message = okMsg, Path = folder });
        _logger.LogInformation("Auto-backup ({Trigger}) OK → {Folder}", trigger, folder);
        return new BackupRunResult(true, okMsg, folder);
    }

    private BackupRunResult Fail(DateTime now, string folder, string message, string trigger)
    {
        _stateStore.Save(new BackupState { LastRunUtc = now, Ok = false, Message = message, Path = folder });
        _logger.LogWarning("Auto-backup ({Trigger}) FAILED: {Message}", trigger, message);
        return new BackupRunResult(false, message, folder);
    }

    /// <summary>Keep newest N SDRLoggerPlus-* folders; never touch anything else.</summary>
    private int Prune()
    {
        var pruned = 0;
        try
        {
            if (_retention < 1 || !Directory.Exists(_destinationRoot)) return 0;
            var siblings = Directory.GetDirectories(_destinationRoot, $"{FolderPrefix}*")
                .Select(p => (Path: p, Mtime: Directory.GetLastWriteTimeUtc(p)))
                .OrderByDescending(t => t.Mtime)
                .ToList();
            foreach (var (path, _) in siblings.Skip(_retention))
            {
                try { Directory.Delete(path, recursive: true); pruned++; }
                catch { /* best effort per folder */ }
            }
        }
        catch { /* prune must never fail the run */ }
        return pruned;
    }
}
```

- [ ] **Step 5: Run tests — expect PASS** (note: new folder's mtime is "now", so it sorts newest — retention 2 keeps it + old2)

- [ ] **Step 6: Commit** — `feat(backup): add BackupRunner with safe-prune retention`

---

### Task 6: BackupService (hosted scheduler) + controller

**Files:**
- Create: `src/SDRLoggerPlus.Server/Services/Backup/BackupService.cs`
- Create: `src/SDRLoggerPlus.Server/Controllers/BackupController.cs`
- Modify: `src/SDRLoggerPlus.Server/Program.cs` (DI registration — add near the other singleton+hosted registrations)
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/BackupServiceTests.cs`

- [ ] **Step 1: Write failing tests** (status assembly + due-fire decision; the timer loop itself isn't unit-tested):

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Backup;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BackupServiceTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "sdrloggerplus-bks-" + Guid.NewGuid());
    private readonly Mock<ISettingsService> _settings = new();
    private readonly Mock<IAdifService> _adif = new();
    private readonly UserSettings _userSettings = new();

    public BackupServiceTests()
    {
        Directory.CreateDirectory(_root);
        _settings.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(_userSettings);
        _adif.Setup(a => a.ExportQsosAsync(null)).ReturnsAsync("<EOH>\n<EOR>");
    }

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private BackupService CreateService() => new(
        _settings.Object, _adif.Object,
        configDir: _root, liteDbPath: Path.Combine(_root, "sdrloggerplus.db"),
        NullLogger<BackupService>.Instance);

    [Fact]
    public async Task GetStatus_Defaults_DisabledDailyDefaultDest()
    {
        var status = await CreateService().GetStatusAsync();
        status.Enabled.Should().BeFalse();
        status.Interval.Should().Be("daily");
        status.Destination.Should().Be(Path.Combine(_root, "backups"));
        status.LastRunUtc.Should().BeNull();
    }

    [Fact]
    public async Task RunNow_WritesBackup_AndStatusReflectsIt()
    {
        File.WriteAllText(Path.Combine(_root, "sdrloggerplus.db"), "X");
        var svc = CreateService();

        var result = await svc.RunNowAsync();

        result.Ok.Should().BeTrue();
        var status = await svc.GetStatusAsync();
        status.LastRunUtc.Should().NotBeNull();
        status.Ok.Should().BeTrue();
        status.NextDueUtc.Should().NotBeNull(); // daily → lastRun+1d
    }

    [Fact]
    public async Task TickAsync_Disabled_DoesNotRun()
    {
        _userSettings.Backup.Enabled = false;
        var svc = CreateService();
        await svc.TickAsync();
        (await svc.GetStatusAsync()).LastRunUtc.Should().BeNull();
    }

    [Fact]
    public async Task TickAsync_EnabledNoHistory_FiresImmediately()
    {
        File.WriteAllText(Path.Combine(_root, "sdrloggerplus.db"), "X");
        _userSettings.Backup.Enabled = true;
        var svc = CreateService();
        await svc.TickAsync();
        (await svc.GetStatusAsync()).LastRunUtc.Should().NotBeNull();
    }

    [Fact]
    public async Task TickAsync_OnExitInterval_NeverFiresFromTimer()
    {
        _userSettings.Backup.Enabled = true;
        _userSettings.Backup.Interval = "on_exit";
        var svc = CreateService();
        await svc.TickAsync();
        (await svc.GetStatusAsync()).LastRunUtc.Should().BeNull();
    }

    [Fact]
    public async Task CustomDestination_IsUsed()
    {
        File.WriteAllText(Path.Combine(_root, "sdrloggerplus.db"), "X");
        var custom = Path.Combine(_root, "elsewhere");
        _userSettings.Backup.DestinationPath = custom;
        var result = await CreateService().RunNowAsync();
        result.Path.Should().StartWith(custom);
    }
}
```

- [ ] **Step 2: Run to verify failure** (compile error)

- [ ] **Step 3: Implement the service**

```csharp
using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services.Backup;

/// <summary>
/// Hosted scheduler for auto-backups. Wakes every 60 s; fires when
/// now - lastRun >= interval (anchored to last successful run, persisted in
/// backup-state.json). "on_exit" fires from ApplicationStopping instead,
/// with a 10 s budget so shutdown is never blocked indefinitely.
/// </summary>
public class BackupService : IHostedService, IDisposable
{
    private readonly ISettingsService _settingsService;
    private readonly IAdifService _adifService;
    private readonly string _configDir;
    private readonly string? _liteDbPath;
    private readonly ILogger<BackupService> _logger;
    private readonly BackupStateStore _stateStore;
    private readonly SemaphoreSlim _runLock = new(1, 1);
    private PeriodicTimer? _timer;
    private Task? _loop;
    private readonly CancellationTokenSource _cts = new();

    public BackupService(ISettingsService settingsService, IAdifService adifService,
        string configDir, string? liteDbPath, ILogger<BackupService> logger)
    {
        _settingsService = settingsService;
        _adifService = adifService;
        _configDir = configDir;
        _liteDbPath = liteDbPath;
        _logger = logger;
        _stateStore = new BackupStateStore(Path.Combine(configDir, "backup-state.json"));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _timer = new PeriodicTimer(TimeSpan.FromSeconds(60));
        _loop = Task.Run(async () =>
        {
            try
            {
                while (await _timer.WaitForNextTickAsync(_cts.Token))
                    await TickAsync();
            }
            catch (OperationCanceledException) { }
        });
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        try
        {
            var backup = (await _settingsService.GetSettingsAsync()).Backup;
            if (backup.Enabled && backup.Interval == "on_exit")
            {
                var run = RunNowAsync("on_exit");
                await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(10), cancellationToken));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "On-exit backup failed");
        }
    }

    /// <summary>One scheduler tick — public for tests.</summary>
    public async Task TickAsync()
    {
        try
        {
            var backup = (await _settingsService.GetSettingsAsync()).Backup;
            if (!backup.Enabled) return;
            var state = _stateStore.Load();
            if (BackupSchedule.IsDue(backup.Interval, state.LastRunUtc, DateTime.UtcNow))
                await RunNowAsync("scheduled");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Backup tick failed");
        }
    }

    public async Task<BackupRunResult> RunNowAsync(string trigger = "manual")
    {
        await _runLock.WaitAsync();
        try
        {
            var backup = (await _settingsService.GetSettingsAsync()).Backup;
            var runner = new BackupRunner(
                dbPath: _liteDbPath,
                adifExport: () => _adifService.ExportQsosAsync(null),
                destinationRoot: ResolveDestination(backup.DestinationPath),
                retention: Math.Max(1, backup.Retention),
                stateStore: _stateStore,
                logger: _loggerFactoryShim());
            return await runner.RunAsync(trigger);
        }
        finally
        {
            _runLock.Release();
        }
    }

    public async Task<BackupStatusDto> GetStatusAsync()
    {
        var backup = (await _settingsService.GetSettingsAsync()).Backup;
        var state = _stateStore.Load();
        return new BackupStatusDto(
            backup.Enabled, backup.Interval, backup.Retention,
            ResolveDestination(backup.DestinationPath),
            state.LastRunUtc, state.Ok, state.Message, state.Path,
            BackupSchedule.NextDue(backup.Interval, state.LastRunUtc));
    }

    private string ResolveDestination(string? configured) =>
        string.IsNullOrWhiteSpace(configured) ? Path.Combine(_configDir, "backups") : configured;

    private ILogger<BackupRunner> _loggerFactoryShim() =>
        Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupRunner>.Instance;

    public void Dispose()
    {
        _cts.Cancel();
        _timer?.Dispose();
        _cts.Dispose();
    }
}
```

Note: the runner's per-run log lines duplicate into state messages anyway; if wiring a real `ILogger<BackupRunner>` through `ILoggerFactory` is trivial at registration time, prefer that over the NullLogger shim — take `ILoggerFactory` in the constructor and call `loggerFactory.CreateLogger<BackupRunner>()`.

- [ ] **Step 4: Implement the controller**

```csharp
using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Server.Services.Backup;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
public class BackupController : ControllerBase
{
    private readonly BackupService _backupService;

    public BackupController(BackupService backupService) => _backupService = backupService;

    [HttpGet("status")]
    public async Task<ActionResult<BackupStatusDto>> GetStatus()
        => Ok(await _backupService.GetStatusAsync());

    [HttpPost("run")]
    public async Task<ActionResult<BackupRunResult>> RunNow()
        => Ok(await _backupService.RunNowAsync("manual"));
}
```

- [ ] **Step 5: Register in `Program.cs`** — with the other singleton+hosted pairs:

```csharp
// Register scheduled backup service
builder.Services.AddSingleton<BackupService>(sp =>
{
    var userConfig = sp.GetRequiredService<IUserConfigService>();
    var configDir = Path.GetDirectoryName(userConfig.GetConfigPath())!;
    var liteDb = sp.GetRequiredService<IDbContext>() as LiteDbContext;
    return new BackupService(
        sp.GetRequiredService<ISettingsService>(),
        sp.GetRequiredService<IAdifService>(),
        configDir,
        liteDb != null ? Path.Combine(configDir, "sdrloggerplus.db") : null,
        sp.GetRequiredService<ILogger<BackupService>>());
});
builder.Services.AddHostedService(sp => sp.GetRequiredService<BackupService>());
```

(Add `using SDRLoggerPlus.Server.Services.Backup;` and `using SDRLoggerPlus.Server.Core.Database.LiteDb;` to Program.cs. The DB path mirrors `LiteDbContext.GetDatabasePath()`; `DatabaseFilePath` from Task 4 is used instead if the context is already initialized at registration time — at runtime resolve lazily: prefer `liteDb.DatabaseFilePath ?? Path.Combine(configDir, "sdrloggerplus.db")` inside the factory.)

- [ ] **Step 6: Run all backup tests + build**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~Backup"`
Expected: all PASS.

- [ ] **Step 7: Commit** — `feat(backup): scheduled backup service, controller, DI wiring`

---

### Task 7: Frontend — settings section + status

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` (add `BackupSettings` interface + field on the settings type, default value where other sections are defaulted)
- Modify: `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx` (new "Backup" section)
- Modify: `src/SDRLoggerPlus.Web/src/api/client.ts` (status/run API calls)

- [ ] **Step 1: Add the interface to settingsStore.ts** (mirror neighboring section interfaces, camelCase as serialized by the API):

```typescript
export interface BackupSettings {
  enabled: boolean;
  interval: 'daily' | 'weekly' | 'on_exit';
  retention: number;
  destinationPath?: string | null;
}
```

Wire it into the `UserSettings`-equivalent type and defaults exactly the way `SpectrumSettings` is wired (search for `spectrum:` to find both spots).

- [ ] **Step 2: Add API calls to client.ts** (follow the existing fetch-wrapper style in that file):

```typescript
export interface BackupStatus {
  enabled: boolean;
  interval: string;
  retention: number;
  destination: string;
  lastRunUtc?: string | null;
  ok?: boolean | null;
  message?: string | null;
  path?: string | null;
  nextDueUtc?: string | null;
}

export interface BackupRunResult {
  ok: boolean;
  message: string;
  path?: string | null;
}

export const getBackupStatus = () => apiGet<BackupStatus>('/api/backup/status');
export const runBackupNow = () => apiPost<BackupRunResult>('/api/backup/run', {});
```

(If `client.ts` uses different helper names than `apiGet`/`apiPost`, match whatever the QRZ/LoTW calls in the same file use.)

- [ ] **Step 3: Add the Backup section to SettingsPanel.tsx**, following the visual pattern of an existing simple section (e.g. the LoTW one): enable toggle, interval `<select>` (Daily/Weekly/On exit), retention number input (min 1), destination text input with placeholder "Default: <config dir>\backups", a status line (last run time + message + next due), and a "Back Up Now" button calling `runBackupNow()` then refreshing status.

- [ ] **Step 4: Run frontend tests + typecheck**

Run: `cd src/SDRLoggerPlus.Web && npm run test && npx tsc --noEmit`
Expected: PASS / no type errors.

- [ ] **Step 5: Commit** — `feat(backup): settings UI for scheduled backups`

---

### Task 8: Full verification

- [ ] **Step 1: Backend suite**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Unit"`
Expected: all PASS (no regressions).

- [ ] **Step 2: Frontend suite**

Run: `cd src/SDRLoggerPlus.Web && npm run test`
Expected: all PASS.

- [ ] **Step 3: Update roadmap status** — in `docs/design/sdrloggerplus-port-roadmap.md`, set feature 1's status to "implemented".

- [ ] **Step 4: Commit** — `docs: mark auto-backup implemented in roadmap`
