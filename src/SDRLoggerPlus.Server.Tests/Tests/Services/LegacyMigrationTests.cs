using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class LegacyMigrationTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "sdrloggerplus-mig-" + Guid.NewGuid());
    private string LegacyDir => Path.Combine(_root, "QSOThief");
    private string NewDir => Path.Combine(_root, "SDRLoggerPlus");

    public LegacyMigrationTests() => Directory.CreateDirectory(_root);
    public void Dispose() => Directory.Delete(_root, recursive: true);

    private void SeedLegacyInstall()
    {
        Directory.CreateDirectory(LegacyDir);
        File.WriteAllText(Path.Combine(LegacyDir, "config.json"), "{\"Provider\":0}");
        File.WriteAllText(Path.Combine(LegacyDir, "qsothief.db"), "DB-CONTENT");
        File.WriteAllText(Path.Combine(LegacyDir, "backup-state.json"), "{}");
        Directory.CreateDirectory(Path.Combine(LegacyDir, "backups", "QSOThief-2026-01-01_0000"));
        File.WriteAllText(Path.Combine(LegacyDir, "backups", "QSOThief-2026-01-01_0000", "qsothief.adi"), "<EOH>");
    }

    [Fact]
    public void FreshInstall_NoLegacyDir_NoOp()
    {
        var migrated = LegacyMigration.MigrateIfNeeded(NewDir);
        migrated.Should().BeEmpty();
        Directory.Exists(NewDir).Should().BeFalse();
    }

    [Fact]
    public void LegacyExists_NewMissing_CopiesEverything_RenamesDb()
    {
        SeedLegacyInstall();

        var migrated = LegacyMigration.MigrateIfNeeded(NewDir);

        migrated.Should().Contain(["config.json", "sdrloggerplus.db", "backup-state.json", "backups/"]);
        File.ReadAllText(Path.Combine(NewDir, "sdrloggerplus.db")).Should().Be("DB-CONTENT");
        File.Exists(Path.Combine(NewDir, "config.json")).Should().BeTrue();
        File.Exists(Path.Combine(NewDir, "backups", "QSOThief-2026-01-01_0000", "qsothief.adi")).Should().BeTrue();

        // Original untouched — safety net
        File.Exists(Path.Combine(LegacyDir, "qsothief.db")).Should().BeTrue();
        File.ReadAllText(Path.Combine(LegacyDir, "qsothief.db")).Should().Be("DB-CONTENT");
    }

    [Fact]
    public void BothExist_NoOp()
    {
        SeedLegacyInstall();
        Directory.CreateDirectory(NewDir);
        File.WriteAllText(Path.Combine(NewDir, "sdrloggerplus.db"), "EXISTING");

        var migrated = LegacyMigration.MigrateIfNeeded(NewDir);

        migrated.Should().BeEmpty();
        File.ReadAllText(Path.Combine(NewDir, "sdrloggerplus.db")).Should().Be("EXISTING");
    }

    [Fact]
    public void PartialLegacyDir_CopiesWhatExists()
    {
        Directory.CreateDirectory(LegacyDir);
        File.WriteAllText(Path.Combine(LegacyDir, "config.json"), "{}");
        // no db, no backups

        var migrated = LegacyMigration.MigrateIfNeeded(NewDir);

        migrated.Should().BeEquivalentTo("config.json");
        File.Exists(Path.Combine(NewDir, "sdrloggerplus.db")).Should().BeFalse();
    }
}