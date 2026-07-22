using FluentAssertions;
using LiteDB;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Server.Core.Database.Migrations;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.Migrations;

[Trait("Category", "Integration")]
public class MigrationRunnerTests : IDisposable
{
    private readonly string _dir;
    private readonly string _dbPath;
    private LiteDatabase _db;

    public MigrationRunnerTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"slp_runner_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _dbPath = Path.Combine(_dir, "test.db");
        _db = new LiteDatabase(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    /// <summary>A migration that records whether it ran and can be told to fail.</summary>
    private sealed class SpyMigration : IDbMigration
    {
        private readonly bool _throws;
        public SpyMigration(int version, bool throws = false)
        {
            Version = version;
            _throws = throws;
        }

        public int Version { get; }
        public string Name => $"spy-{Version}";
        public int Applications { get; private set; }

        public MigrationResult Apply(LiteDatabase database, ILogger logger)
        {
            Applications++;
            if (_throws) throw new InvalidOperationException($"spy-{Version} failed on purpose");
            // Touch the database so a failed run has something to have skipped.
            database.GetCollection<BsonDocument>("spy").Insert(
                new BsonDocument { ["_id"] = Version });
            return new MigrationResult(1, 1, new[] { $"applied {Version}" });
        }
    }

    private static MigrationRunner Runner(params IDbMigration[] migrations) =>
        new(migrations, NullLogger.Instance);

    private string PreMigrationFolder => Path.Combine(_dir, "pre-migration");

    [Fact]
    public void AppliesPendingMigrationsAndBumpsUserVersion()
    {
        var m1 = new SpyMigration(1);
        var m2 = new SpyMigration(2);

        var applied = Runner(m1, m2).Run(_db, _dbPath);

        applied.Should().Be(2);
        m1.Applications.Should().Be(1);
        m2.Applications.Should().Be(1);
        _db.UserVersion.Should().Be(2);
    }

    [Fact]
    public void SecondRunDoesNothing()
    {
        var m1 = new SpyMigration(1);
        Runner(m1).Run(_db, _dbPath);

        var applied = Runner(m1).Run(_db, _dbPath);

        applied.Should().Be(0);
        m1.Applications.Should().Be(1, "an already-applied migration must never run twice");
    }

    [Fact]
    public void SkipsMigrationsAtOrBelowTheCurrentVersion()
    {
        _db.UserVersion = 2;
        var m1 = new SpyMigration(1);
        var m2 = new SpyMigration(2);
        var m3 = new SpyMigration(3);

        Runner(m1, m2, m3).Run(_db, _dbPath);

        m1.Applications.Should().Be(0);
        m2.Applications.Should().Be(0);
        m3.Applications.Should().Be(1);
        _db.UserVersion.Should().Be(3);
    }

    [Fact]
    public void AppliesInAscendingVersionOrderRegardlessOfCatalogOrder()
    {
        var order = new List<int>();
        var m1 = new OrderRecordingMigration(1, order);
        var m2 = new OrderRecordingMigration(2, order);
        var m3 = new OrderRecordingMigration(3, order);

        Runner(m3, m1, m2).Run(_db, _dbPath);

        order.Should().Equal(1, 2, 3);
    }

    private sealed class OrderRecordingMigration : IDbMigration
    {
        private readonly List<int> _order;
        public OrderRecordingMigration(int version, List<int> order)
        {
            Version = version;
            _order = order;
        }
        public int Version { get; }
        public string Name => $"order-{Version}";
        public MigrationResult Apply(LiteDatabase database, ILogger logger)
        {
            _order.Add(Version);
            return MigrationResult.None();
        }
    }

    [Fact]
    public void FailureStopsTheRunAndLeavesVersionAtTheLastSuccess()
    {
        var m1 = new SpyMigration(1);
        var m2 = new SpyMigration(2, throws: true);
        var m3 = new SpyMigration(3);

        var applied = Runner(m1, m2, m3).Run(_db, _dbPath);

        applied.Should().Be(1);
        _db.UserVersion.Should().Be(1, "a failed migration must not be recorded as applied");
        m3.Applications.Should().Be(0, "migration 3 may assume migration 2 ran");
    }

    [Fact]
    public void AFailedMigrationIsRetriedOnTheNextRun()
    {
        var failing = new SpyMigration(1, throws: true);
        Runner(failing).Run(_db, _dbPath);
        _db.UserVersion.Should().Be(0);

        // Next launch, with the bug fixed — represented here by a migration
        // at the same version that succeeds.
        var fixedUp = new SpyMigration(1);
        Runner(fixedUp).Run(_db, _dbPath);

        fixedUp.Applications.Should().Be(1);
        _db.UserVersion.Should().Be(1);
    }

    [Fact]
    public void TakesExactlyOnePreMigrationBackupPerRun()
    {
        Runner(new SpyMigration(1), new SpyMigration(2)).Run(_db, _dbPath);

        Directory.GetFiles(PreMigrationFolder, "*.db")
            .Should().ContainSingle("the backup is per run, not per migration");
    }

    [Fact]
    public void TakesNoBackupWhenNothingIsPending()
    {
        _db.UserVersion = 5;

        Runner(new SpyMigration(1)).Run(_db, _dbPath);

        Directory.Exists(PreMigrationFolder).Should().BeFalse(
            "the common case is every launch after the first — it must cost nothing");
    }

    [Fact]
    public void BackupIsARestorableCopyOfThePreMigrationData()
    {
        _db.GetCollection<BsonDocument>("payload")
            .Insert(new BsonDocument { ["_id"] = 1, ["value"] = "before" });

        Runner(new SpyMigration(1)).Run(_db, _dbPath);

        var backup = Directory.GetFiles(PreMigrationFolder, "*.db").Single();
        using var restored = new LiteDatabase($"Filename={backup};ReadOnly=true");
        restored.GetCollection<BsonDocument>("payload").FindById(1)["value"]
            .AsString.Should().Be("before");
        restored.GetCollection<BsonDocument>("spy").Count()
            .Should().Be(0, "the backup must predate the migration's writes");
    }

    [Fact]
    public void SkipsEverythingWhenTheBackupCannotBeTaken()
    {
        // A file where the backup folder needs to be: CreateDirectory throws,
        // so no migration may touch user data.
        File.WriteAllText(PreMigrationFolder, "not a folder");
        var m1 = new SpyMigration(1);

        var applied = Runner(m1).Run(_db, _dbPath);

        applied.Should().Be(0);
        m1.Applications.Should().Be(0);
        _db.UserVersion.Should().Be(0);
    }

    [Fact]
    public void WritesAChangeLogWithTheDetailLines()
    {
        Runner(new SpyMigration(1)).Run(_db, _dbPath);

        var log = Directory.GetFiles(PreMigrationFolder, "changes-v1-*.log").Single();
        var text = File.ReadAllText(log);
        text.Should().Contain("spy-1").And.Contain("applied 1").And.Contain("Changed: 1");
    }

    [Fact]
    public void RunsWithoutADbPath()
    {
        // In-memory databases (tests) have no file to back up; migrations must
        // still apply rather than being silently skipped.
        var m1 = new SpyMigration(1);

        var applied = Runner(m1).Run(_db, dbPath: null);

        applied.Should().Be(1);
        _db.UserVersion.Should().Be(1);
    }

    [Fact]
    public void RejectsDuplicateVersionsAtConstruction()
    {
        var act = () => Runner(new SpyMigration(1), new SpyMigration(1));

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Duplicate migration version 1*");
    }
}

[Trait("Category", "Unit")]
public class MigrationCatalogTests
{
    [Fact]
    public void VersionsAreUniqueAndContiguousFromOne()
    {
        var versions = MigrationCatalog.All.Select(m => m.Version).ToList();

        versions.Should().OnlyHaveUniqueItems();
        versions.OrderBy(v => v).Should().Equal(Enumerable.Range(1, versions.Count),
            "a gap means a migration was removed after shipping, which strands " +
            "every database that already ran it");
    }

    [Fact]
    public void EveryMigrationHasAName()
    {
        MigrationCatalog.All.Should().OnlyContain(m => !string.IsNullOrWhiteSpace(m.Name));
    }

    [Fact]
    public void CatalogCanBeConstructedIntoARunner()
    {
        // Pins the duplicate-version guard against the real catalog, so adding
        // a migration with a copy-pasted version number fails here, not on a
        // user's machine at startup.
        var act = () => new MigrationRunner(MigrationCatalog.All, NullLogger.Instance);

        act.Should().NotThrow();
    }
}
