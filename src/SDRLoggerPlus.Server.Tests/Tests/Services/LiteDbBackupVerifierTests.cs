using FluentAssertions;
using LiteDB;
using SDRLoggerPlus.Server.Services.Backup;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class LiteDbBackupVerifierTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "sdrloggerplus-vfy-" + Guid.NewGuid());

    public LiteDbBackupVerifierTests() => Directory.CreateDirectory(_root);

    public void Dispose() => Directory.Delete(_root, recursive: true);

    [Fact]
    public void Verify_ValidLiteDb_ReturnsNull()
    {
        var path = Path.Combine(_root, "good.db");
        using (var db = new LiteDatabase($"Filename={path}"))
        {
            db.GetCollection<BsonDocument>("qsos").Insert(new BsonDocument { ["call"] = "W1AW" });
        }

        LiteDbBackupVerifier.Verify(path).Should().BeNull();
    }

    [Fact]
    public void Verify_CorruptFile_ReturnsError()
    {
        var path = Path.Combine(_root, "bad.db");
        File.WriteAllText(path, "NOT-A-LITEDB-FILE");

        LiteDbBackupVerifier.Verify(path).Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void Verify_MissingFile_ReturnsError()
    {
        LiteDbBackupVerifier.Verify(Path.Combine(_root, "absent.db")).Should().NotBeNullOrEmpty();
    }

    // Regression: a failed open must not leak the file handle. The caller
    // deletes a copy that fails verification, and that delete has to succeed.
    [Fact]
    public void Verify_CorruptFile_LeavesFileDeletable()
    {
        var path = Path.Combine(_root, "bad-then-deleted.db");
        File.WriteAllText(path, "NOT-A-LITEDB-FILE");

        LiteDbBackupVerifier.Verify(path).Should().NotBeNull();

        var deleting = () => File.Delete(path);
        deleting.Should().NotThrow();
        File.Exists(path).Should().BeFalse();
    }

    [Fact]
    public void Verify_ValidLiteDb_LeavesFileDeletable()
    {
        var path = Path.Combine(_root, "good-then-deleted.db");
        using (var db = new LiteDatabase($"Filename={path}"))
        {
            db.GetCollection<BsonDocument>("qsos").Insert(new BsonDocument { ["call"] = "W1AW" });
        }

        LiteDbBackupVerifier.Verify(path).Should().BeNull();

        var deleting = () => File.Delete(path);
        deleting.Should().NotThrow();
    }
}
