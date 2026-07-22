using LiteDB;

namespace SDRLoggerPlus.Server.Core.Database.Migrations;

/// <summary>
/// One versioned, forward-only change to the database, gated by LiteDB's
/// <see cref="LiteDatabase.UserVersion"/>.
///
/// A migration is a historical artifact: once it has shipped, some user's
/// database has already been through it, so its behaviour must never change.
/// That has one hard consequence — <b>a migration must not call into shared
/// helpers whose data can be edited later</b> (BandHelper, DXCC tables,
/// settings). Freeze whatever tables it needs inside the migration itself,
/// even when that means duplicating a few constants. Duplication here is the
/// cheap half of the trade; the expensive half is a shipped migration
/// silently doing something different next release.
/// </summary>
public interface IDbMigration
{
    /// <summary>
    /// Sequential version this migration brings the database TO. The runner
    /// applies every migration whose Version is greater than the database's
    /// current UserVersion, in ascending order. Never renumber a shipped one.
    /// </summary>
    int Version { get; }

    /// <summary>Short human name, used in logs and the change log.</summary>
    string Name { get; }

    /// <summary>
    /// Apply the change. Must be idempotent: running it twice over the same
    /// data changes nothing the second time. The runner verifies this in
    /// tests, and idempotence is what makes a half-finished run (power loss,
    /// crash) safe to simply re-run.
    ///
    /// Throwing aborts this migration; the runner leaves UserVersion
    /// unchanged so the next startup retries it.
    /// </summary>
    MigrationResult Apply(LiteDatabase database, ILogger logger);
}

/// <summary>
/// What a migration did, for the change log. <paramref name="Changed"/> is the
/// number of records actually modified; <paramref name="Examined"/> the number
/// considered. <paramref name="Details"/> carries per-record before/after lines
/// so a surprised operator can see exactly what moved.
/// </summary>
public record MigrationResult(int Examined, int Changed, IReadOnlyList<string> Details)
{
    public static MigrationResult None(int examined = 0) =>
        new(examined, 0, Array.Empty<string>());
}
