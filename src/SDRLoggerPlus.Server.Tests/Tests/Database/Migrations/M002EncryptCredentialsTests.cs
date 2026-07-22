using FluentAssertions;
using LiteDB;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.Migrations;
using SDRLoggerPlus.Server.Core.Security;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.Migrations;

[Trait("Category", "Integration")]
[Collection("LiteDbMapper")]
public class M002EncryptCredentialsTests : IDisposable
{
    private readonly string _dir;
    private readonly LiteDatabase _db;
    private readonly ILiteCollection<UserSettings> _settings;
    private readonly SecretProtector _protector;

    public M002EncryptCredentialsTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"slp_m002_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _db = new LiteDatabase(Path.Combine(_dir, "test.db"));
        _settings = _db.GetCollection<UserSettings>("settings");
        _protector = new SecretProtector(_dir, NullLogger<SecretProtector>.Instance, useDpapi: false);
    }

    public void Dispose()
    {
        _db.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private MigrationResult Apply() =>
        new M002EncryptCredentials(_protector).Apply(_db, NullLogger.Instance);

    private UserSettings Stored() => _settings.FindById("default");

    [Fact]
    public void EncryptsEveryPlaintextCredential()
    {
        var settings = new UserSettings();
        foreach (var field in SettingsSecrets.Fields) field.Set(settings, "hunter2");
        settings.Cluster.Connections.Add(new ClusterConnection { Password = "clusterpw" });
        _settings.Insert(settings);

        var result = Apply();

        result.Changed.Should().Be(SettingsSecrets.Fields.Count + 1);

        var after = Stored();
        foreach (var field in SettingsSecrets.Fields)
        {
            var value = field.Get(after);
            SecretEnvelope.IsProtected(value).Should().BeTrue($"{field.Name} must be encrypted");
            value.Should().NotContain("hunter2");
        }
        SecretEnvelope.IsProtected(after.Cluster.Connections[0].Password).Should().BeTrue();
    }

    [Fact]
    public void EncryptedValuesStillDecryptToTheOriginals()
    {
        var settings = new UserSettings();
        settings.Qrz.Password = "qrzpw";
        settings.Ai.ApiKey = "sk-secret";
        settings.Cluster.Connections.Add(new ClusterConnection { Password = "clusterpw" });
        _settings.Insert(settings);

        Apply();

        var after = Stored();
        _protector.Unprotect(after.Qrz.Password).Should().Be("qrzpw");
        _protector.Unprotect(after.Ai.ApiKey).Should().Be("sk-secret");
        _protector.Unprotect(after.Cluster.Connections[0].Password).Should().Be("clusterpw");
    }

    [Fact]
    public void IsIdempotent()
    {
        var settings = new UserSettings { Qrz = { Password = "hunter2" } };
        _settings.Insert(settings);

        Apply().Changed.Should().Be(1);
        var afterFirst = Stored().Qrz.Password;

        var second = Apply();

        second.Changed.Should().Be(0);
        Stored().Qrz.Password.Should().Be(afterFirst, "re-encrypting would change the ciphertext");
    }

    [Fact]
    public void LeavesEmptyCredentialsEmpty()
    {
        // An unconfigured service must not gain a value that makes it look
        // configured.
        _settings.Insert(new UserSettings { Qrz = { Password = "", ApiKey = null } });

        Apply().Changed.Should().Be(0);

        // Null-or-empty, not strictly empty: LiteDB's mapper has
        // EmptyStringToNull on by default, so "" comes back as null. What
        // matters is that neither became an encrypted blob.
        var after = Stored();
        after.Qrz.Password.Should().BeNullOrEmpty();
        after.Qrz.ApiKey.Should().BeNullOrEmpty();
        SecretEnvelope.IsProtected(after.Qrz.Password).Should().BeFalse();
    }

    [Fact]
    public void DoesNotTouchNonCredentialSettings()
    {
        _settings.Insert(new UserSettings
        {
            Qrz = { Username = "N9BC", Password = "hunter2" },
            Station = { Callsign = "N9BC" }
        });

        Apply();

        var after = Stored();
        after.Qrz.Username.Should().Be("N9BC");
        after.Station.Callsign.Should().Be("N9BC");
    }

    [Fact]
    public void ChangeLogNamesFieldsButNeverValues()
    {
        // The change log is a plain file next to the database. Writing the
        // credentials into it would recreate the exposure being closed.
        _settings.Insert(new UserSettings { Qrz = { Password = "hunter2" }, Ai = { ApiKey = "sk-secret" } });

        var result = Apply();

        var text = string.Join("\n", result.Details);
        text.Should().Contain("qrz.password").And.Contain("ai.apiKey");
        text.Should().NotContain("hunter2");
        text.Should().NotContain("sk-secret");
    }

    [Fact]
    public void HandlesADatabaseWithNoSettingsYet()
    {
        var result = Apply();

        result.Changed.Should().Be(0);
        result.Examined.Should().Be(0);
    }

    [Fact]
    public void EncryptsEveryClusterConnectionNotJustTheFirst()
    {
        var settings = new UserSettings();
        settings.Cluster.Connections.Add(new ClusterConnection { Password = "one" });
        settings.Cluster.Connections.Add(new ClusterConnection { Password = null });
        settings.Cluster.Connections.Add(new ClusterConnection { Password = "three" });
        _settings.Insert(settings);

        Apply().Changed.Should().Be(2);

        var after = Stored();
        _protector.Unprotect(after.Cluster.Connections[0].Password).Should().Be("one");
        after.Cluster.Connections[1].Password.Should().BeNull();
        _protector.Unprotect(after.Cluster.Connections[2].Password).Should().Be("three");
    }
}
