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
    public static IReadOnlyList<IDbMigration> All { get; } = new IDbMigration[]
    {
        new M001FrequencyKhzRepair(),
    };

    /// <summary>Highest version in the catalog — the version a fresh database ends at.</summary>
    public static int LatestVersion => All.Max(m => m.Version);
}
