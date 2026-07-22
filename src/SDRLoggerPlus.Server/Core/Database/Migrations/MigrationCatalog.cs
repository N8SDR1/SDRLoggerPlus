using SDRLoggerPlus.Server.Core.Security;

namespace SDRLoggerPlus.Server.Core.Database.Migrations;

/// <summary>
/// The ordered list of every migration the app ships.
///
/// Deliberately a static list rather than assembly scanning: the set of
/// migrations that has run against a user's database is the most
/// consequential thing in this folder, and it should be readable in one
/// glance and greppable in a diff, not inferred from reflection at runtime.
///
/// To add one: create the class, append it here, and never renumber or
/// remove an entry that has shipped.
/// </summary>
public static class MigrationCatalog
{
    /// <summary>
    /// Build the catalog. Takes the protector rather than reaching for one,
    /// because a migration that rewrites credentials must use exactly the same
    /// protector the repository will later read them back with.
    /// </summary>
    public static IReadOnlyList<IDbMigration> Build(ISecretProtector protector) => new IDbMigration[]
    {
        new M001FrequencyKhzRepair(),
        new M002EncryptCredentials(protector),
    };

    /// <summary>Highest version in the catalog — the version a fresh database ends at.</summary>
    public static int LatestVersion => Build(NullSecretProtector.Instance).Max(m => m.Version);
}

/// <summary>
/// Pass-through protector, for the places that need the catalog's shape
/// (version numbers, names) rather than its behaviour.
/// </summary>
internal sealed class NullSecretProtector : ISecretProtector
{
    public static readonly NullSecretProtector Instance = new();
    public string? Protect(string? plaintext) => plaintext;
    public string? Unprotect(string? stored) => stored;
}
