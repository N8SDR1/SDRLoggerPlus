using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Core.Security;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

[Trait("Category", "Integration")]
[Collection("LiteDbMapper")]
public class LiteSettingsRepositoryEncryptionTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture = new();
    private readonly string _keyDir;
    private readonly SecretProtector _protector;
    private readonly LiteSettingsRepository _repo;

    public LiteSettingsRepositoryEncryptionTests()
    {
        _keyDir = Path.Combine(Path.GetTempPath(), $"slp_repo_sec_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_keyDir);
        _protector = new SecretProtector(_keyDir, NullLogger<SecretProtector>.Instance, useDpapi: false);
        _repo = new LiteSettingsRepository(_fixture.Context, _protector);
    }

    public void Dispose()
    {
        _fixture.Dispose();
        try { Directory.Delete(_keyDir, recursive: true); } catch { }
    }

    /// <summary>Reads the raw stored document, bypassing the repository's decryption.</summary>
    private UserSettings Raw() => _fixture.Context.Settings.FindById("default");

    [Fact]
    public async Task SaveEncryptsAtRestAndLoadDecrypts()
    {
        await _repo.UpsertAsync(new UserSettings { Qrz = { Password = "hunter2" } });

        SecretEnvelope.IsProtected(Raw().Qrz.Password).Should().BeTrue("the file must not hold plaintext");
        Raw().Qrz.Password.Should().NotContain("hunter2");

        var loaded = await _repo.GetAsync();
        loaded!.Qrz.Password.Should().Be("hunter2");
    }

    [Fact]
    public async Task CallersObjectIsLeftInPlaintextAfterSave()
    {
        // The API echoes the saved settings back to the SPA. Returning
        // ciphertext would put "slp$1$..." into the password box, and the next
        // save would store that as the new password.
        var settings = new UserSettings { Qrz = { Password = "hunter2" }, Ai = { ApiKey = "sk-secret" } };

        var returned = await _repo.UpsertAsync(settings);

        settings.Qrz.Password.Should().Be("hunter2");
        returned.Qrz.Password.Should().Be("hunter2");
        returned.Ai.ApiKey.Should().Be("sk-secret");
    }

    [Fact]
    public async Task ResavingLoadedSettingsDoesNotDoubleEncrypt()
    {
        await _repo.UpsertAsync(new UserSettings { Qrz = { Password = "hunter2" } });

        var loaded = await _repo.GetAsync();
        await _repo.UpsertAsync(loaded!);

        var reloaded = await _repo.GetAsync();
        reloaded!.Qrz.Password.Should().Be("hunter2");
    }

    [Fact]
    public async Task LegacyPlaintextInTheFileStillLoads()
    {
        // A database that has not been through migration 2 yet.
        _fixture.Context.Settings.Upsert(new UserSettings { Qrz = { Password = "legacy-plain" } });

        var loaded = await _repo.GetAsync();

        loaded!.Qrz.Password.Should().Be("legacy-plain");
    }

    [Fact]
    public async Task ClusterConnectionPasswordsAreEncrypted()
    {
        var settings = new UserSettings();
        settings.Cluster.Connections.Add(new ClusterConnection { Name = "VE7CC", Password = "clusterpw" });

        await _repo.UpsertAsync(settings);

        SecretEnvelope.IsProtected(Raw().Cluster.Connections[0].Password).Should().BeTrue();
        var loaded = await _repo.GetAsync();
        loaded!.Cluster.Connections[0].Password.Should().Be("clusterpw");
        loaded.Cluster.Connections[0].Name.Should().Be("VE7CC");
    }

    /// <summary>Protects normally until the Nth non-empty value, then throws.</summary>
    private sealed class FailingProtector : ISecretProtector
    {
        private readonly ISecretProtector _inner;
        private readonly int _failOn;
        private int _seen;

        public FailingProtector(ISecretProtector inner, int failOn)
        {
            _inner = inner;
            _failOn = failOn;
        }

        public string? Protect(string? plaintext)
        {
            if (string.IsNullOrEmpty(plaintext)) return plaintext;
            if (++_seen >= _failOn) throw new InvalidOperationException("protector failed on purpose");
            return _inner.Protect(plaintext);
        }

        public string? Unprotect(string? stored) => _inner.Unprotect(stored);
    }

    [Fact]
    public async Task AFailedSaveLeavesTheCallersObjectFullyPlaintext()
    {
        // Protect throws partway through the credential fields. The save must
        // fail loudly AND the caller's object must come back exactly as it
        // went in — no field left holding ciphertext from before the throw.
        var repo = new LiteSettingsRepository(_fixture.Context, new FailingProtector(_protector, failOn: 3));
        var settings = new UserSettings
        {
            Qrz = { Password = "one", ApiKey = "two" },
            Lotw = { Password = "three" },
            Ai = { ApiKey = "four" }
        };

        var act = () => repo.UpsertAsync(settings);
        await act.Should().ThrowAsync<InvalidOperationException>();

        settings.Qrz.Password.Should().Be("one");
        settings.Qrz.ApiKey.Should().Be("two");
        settings.Lotw.Password.Should().Be("three");
        settings.Ai.ApiKey.Should().Be("four");
    }

    [Fact]
    public async Task WithoutAProtectorBehaviourIsUnchanged()
    {
        var plain = new LiteSettingsRepository(_fixture.Context);

        await plain.UpsertAsync(new UserSettings { Qrz = { Password = "hunter2" } });

        var loaded = await plain.GetAsync();
        loaded!.Qrz.Password.Should().Be("hunter2");
    }

    [Fact]
    public async Task MissingSettingsReturnsNull()
    {
        (await _repo.GetAsync("nope")).Should().BeNull();
    }
}
