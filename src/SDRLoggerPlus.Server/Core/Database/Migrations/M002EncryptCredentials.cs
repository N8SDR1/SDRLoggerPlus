using LiteDB;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Security;

namespace SDRLoggerPlus.Server.Core.Database.Migrations;

/// <summary>
/// Encrypts credentials that are already sitting in the database as plaintext.
///
/// From this release LiteSettingsRepository encrypts on save, but that only
/// covers settings the operator saves again. Without this migration, someone
/// who configured QRZ two years ago and never reopened Settings would keep a
/// plaintext password in a file that gets backed up nightly — which is the
/// entire problem S-1 describes. The migration is what makes the fix reach
/// existing installs rather than only new writes.
///
/// Idempotent because Protect refuses to double-wrap an already-enveloped
/// value, so a re-run encrypts only what is genuinely still plaintext.
///
/// Deliberately reports only field NAMES in its change log, never values —
/// the change log is a plain text file next to the database, and writing the
/// credentials into it would recreate the exposure this migration exists to
/// close.
/// </summary>
public class M002EncryptCredentials : IDbMigration
{
    private readonly ISecretProtector _protector;

    public M002EncryptCredentials(ISecretProtector protector) => _protector = protector;

    public int Version => 2;
    public string Name => "encrypt-stored-credentials";

    public MigrationResult Apply(LiteDatabase database, ILogger logger)
    {
        // LiteDB maps by PROPERTY NAME here: the [BsonElement] attributes on
        // UserSettings are MongoDB's, which LiteDB does not read. Going
        // through the typed collection keeps this migration agreeing with the
        // repository instead of guessing at document keys.
        var collection = database.GetCollection<UserSettings>("settings");
        var documents = collection.FindAll().ToList();

        var examined = 0;
        var changed = 0;
        var details = new List<string>();
        var updated = new List<UserSettings>();

        foreach (var settings in documents)
        {
            var encryptedHere = new List<string>();

            foreach (var field in SettingsSecrets.Fields)
            {
                var value = field.Get(settings);
                examined++;
                if (string.IsNullOrEmpty(value) || SecretEnvelope.IsProtected(value)) continue;

                field.Set(settings, _protector.Protect(value));
                encryptedHere.Add(field.Name);
            }

            for (var i = 0; i < settings.Cluster.Connections.Count; i++)
            {
                var connection = settings.Cluster.Connections[i];
                var value = connection.Password;
                examined++;
                if (string.IsNullOrEmpty(value) || SecretEnvelope.IsProtected(value)) continue;

                connection.Password = _protector.Protect(value);
                encryptedHere.Add($"cluster.connections[{i}].password");
            }

            if (encryptedHere.Count == 0) continue;

            updated.Add(settings);
            changed += encryptedHere.Count;
            // Names only. Never the values.
            details.Add($"{settings.Id}: encrypted {encryptedHere.Count} credential(s) — " +
                        string.Join(", ", encryptedHere));
        }

        if (updated.Count > 0)
        {
            collection.Update(updated);
            logger.LogInformation(
                "Encrypted {Count} stored credential(s) that were previously plaintext", changed);
        }

        return new MigrationResult(examined, changed, details);
    }
}
