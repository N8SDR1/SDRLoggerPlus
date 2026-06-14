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
