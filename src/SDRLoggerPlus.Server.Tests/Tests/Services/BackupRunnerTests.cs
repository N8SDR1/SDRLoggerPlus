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
