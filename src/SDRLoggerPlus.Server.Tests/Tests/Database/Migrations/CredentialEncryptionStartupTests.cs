using FluentAssertions;
using LiteDB;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Core.Security;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.Migrations;

/// <summary>
/// End-to-end through the real startup path: an existing database holding
/// plaintext credentials is opened by LiteDbContext (which runs migrations in
/// its constructor, exactly as it does at app start), then read back through
/// LiteSettingsRepository.
///
/// This covers the seam the unit tests cannot — that the protector the
/// migration encrypts with is the same one the repository decrypts with.
/// Getting that pairing wrong would encrypt every credential into something
/// the app can never read again, and no test of either half alone would
/// notice.
/// </summary>
[Trait("Category", "Integration")]
[Collection("LiteDbMapper")]
public class CredentialEncryptionStartupTests : IDisposable
{
    private readonly string _dir;
    private readonly string _dbPath;
    private readonly Mock<IUserConfigService> _userConfig = new();

    public CredentialEncryptionStartupTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"slp_startup_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _dbPath = Path.Combine(_dir, "sdrloggerplus.db");

        _userConfig.Setup(s => s.GetConfigPath()).Returns(Path.Combine(_dir, "config.json"));
        _userConfig.Setup(s => s.SaveConfigAsync(It.IsAny<UserConfig>())).Returns(Task.CompletedTask);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    /// <summary>Write a pre-encryption database, the way an existing install looks.</summary>
    private void SeedLegacyPlaintext()
    {
        using var db = new LiteDatabase(_dbPath);
        var settings = new UserSettings { Qrz = { Username = "N9BC", Password = "qrz-secret" }, Ai = { ApiKey = "sk-secret" } };
        settings.Cluster.Connections.Add(new ClusterConnection { Name = "VE7CC", Password = "cluster-secret" });
        db.GetCollection<UserSettings>("settings").Insert(settings);
    }

    private LiteSettingsRepository RepositoryOver(LiteDbContext context) =>
        new(context, new SecretProtector(_dir, NullLogger<SecretProtector>.Instance));

    [Fact]
    public async Task StartupEncryptsLegacyCredentialsAndTheAppStillReadsThem()
    {
        SeedLegacyPlaintext();

        // Opening the context runs the migrations, as it does at app start.
        using var context = new LiteDbContext(_userConfig.Object);

        // On disk: no plaintext.
        var raw = context.Settings.FindById("default");
        SecretEnvelope.IsProtected(raw.Qrz.Password).Should().BeTrue();
        SecretEnvelope.IsProtected(raw.Ai.ApiKey).Should().BeTrue();
        SecretEnvelope.IsProtected(raw.Cluster.Connections[0].Password).Should().BeTrue();

        // Through the app: the original values.
        var loaded = await RepositoryOver(context).GetAsync();
        loaded!.Qrz.Password.Should().Be("qrz-secret");
        loaded.Ai.ApiKey.Should().Be("sk-secret");
        loaded.Cluster.Connections[0].Password.Should().Be("cluster-secret");
        loaded.Qrz.Username.Should().Be("N9BC");
    }

    [Fact]
    public async Task ASaveAfterMigrationRoundTripsCleanly()
    {
        SeedLegacyPlaintext();
        using var context = new LiteDbContext(_userConfig.Object);
        var repo = RepositoryOver(context);

        var loaded = await repo.GetAsync();
        loaded!.Qrz.Password = "changed-password";
        await repo.UpsertAsync(loaded);

        SecretEnvelope.IsProtected(context.Settings.FindById("default").Qrz.Password).Should().BeTrue();
        (await repo.GetAsync())!.Qrz.Password.Should().Be("changed-password");
    }

    [Fact]
    public void MigrationsAreRecordedSoASecondStartDoesNotRepeatThem()
    {
        SeedLegacyPlaintext();

        using (var first = new LiteDbContext(_userConfig.Object))
        {
            first.Database.UserVersion.Should().Be(2, "both shipped migrations should have applied");
        }

        // Second launch: one backup, not two.
        using var second = new LiteDbContext(_userConfig.Object);
        Directory.GetFiles(Path.Combine(_dir, "pre-migration"), "*.db")
            .Should().ContainSingle();
    }

    [Fact]
    public async Task AFreshInstallNeedsNoMigrationAndStillEncrypts()
    {
        using var context = new LiteDbContext(_userConfig.Object);
        var repo = RepositoryOver(context);

        await repo.UpsertAsync(new UserSettings { Qrz = { Password = "brand-new" } });

        SecretEnvelope.IsProtected(context.Settings.FindById("default").Qrz.Password).Should().BeTrue();
        (await repo.GetAsync())!.Qrz.Password.Should().Be("brand-new");
    }
}
