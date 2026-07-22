using System.Text;
using LiteDB;

namespace SDRLoggerPlus.Server.Core.Database.Migrations;

/// <summary>
/// Applies pending <see cref="IDbMigration"/>s at startup, gated by LiteDB's
/// UserVersion.
///
/// Ordering guarantees, in the order they matter:
///
/// 1. <b>Nothing happens when nothing is pending.</b> The common case (every
///    launch after the first) costs one integer read and takes no backup.
/// 2. <b>One pre-migration backup, before the first write.</b> Migrations
///    rewrite user data in place; the copy is the only way back. It is taken
///    once per run, not once per migration, and a failure to take it ABORTS
///    the run — an unbacked-up migration is not worth the risk on someone's
///    only log.
/// 3. <b>UserVersion is bumped per migration, after it succeeds.</b> So a run
///    that dies halfway keeps the completed prefix and retries only the rest.
///
/// A failing migration is logged and stops the run without bumping the
/// version, leaving the app to start normally. That is deliberate: this is a
/// desktop logging app, and refusing to launch because a data repair failed
/// would take the operator off the air over a problem they cannot fix mid-
/// contest. The repair retries next launch.
/// </summary>
public class MigrationRunner
{
    private readonly IReadOnlyList<IDbMigration> _migrations;
    private readonly ILogger _logger;

    public MigrationRunner(IEnumerable<IDbMigration> migrations, ILogger logger)
    {
        // Ascending, so a partial run always leaves a contiguous applied prefix.
        _migrations = migrations.OrderBy(m => m.Version).ToList();
        _logger = logger;

        var duplicate = _migrations.GroupBy(m => m.Version).FirstOrDefault(g => g.Count() > 1);
        if (duplicate != null)
        {
            throw new InvalidOperationException(
                $"Duplicate migration version {duplicate.Key}: " +
                string.Join(", ", duplicate.Select(m => m.Name)));
        }
    }

    /// <summary>
    /// Run every migration newer than the database's UserVersion.
    /// <paramref name="dbPath"/> may be null (in-memory databases in tests),
    /// in which case the pre-migration backup is skipped.
    /// </summary>
    /// <returns>The number of migrations successfully applied.</returns>
    public int Run(LiteDatabase database, string? dbPath)
    {
        var current = database.UserVersion;
        var pending = _migrations.Where(m => m.Version > current).ToList();
        if (pending.Count == 0)
        {
            _logger.LogDebug("Database schema up to date (version {Version})", current);
            return 0;
        }

        _logger.LogInformation(
            "Database at version {Current}; applying {Count} migration(s): {Names}",
            current, pending.Count, string.Join(", ", pending.Select(m => $"{m.Version}:{m.Name}")));

        if (dbPath != null && !TryBackup(database, dbPath, current))
        {
            // TryBackup logged the reason. Refuse to touch user data blind.
            return 0;
        }

        var applied = 0;
        foreach (var migration in pending)
        {
            try
            {
                var result = migration.Apply(database, _logger);
                database.UserVersion = migration.Version;
                database.Checkpoint();
                applied++;

                _logger.LogInformation(
                    "Migration {Version} ({Name}) applied: {Changed} changed of {Examined} examined",
                    migration.Version, migration.Name, result.Changed, result.Examined);

                WriteChangeLog(dbPath, migration, result);
            }
            catch (Exception ex)
            {
                // Stop rather than skip: migration N+1 may assume N ran.
                _logger.LogError(ex,
                    "Migration {Version} ({Name}) FAILED — database stays at version {Version0}. " +
                    "It will be retried on the next start; a pre-migration backup was taken.",
                    migration.Version, migration.Name, database.UserVersion);
                break;
            }
        }

        return applied;
    }

    /// <summary>
    /// Copy the database file aside before the first migration writes.
    /// Checkpoint first so the WAL is merged and the copy is a complete
    /// database rather than a main file missing its recent commits.
    /// </summary>
    private bool TryBackup(LiteDatabase database, string dbPath, int fromVersion)
    {
        try
        {
            database.Checkpoint();

            var folder = Path.Combine(Path.GetDirectoryName(dbPath)!, "pre-migration");
            Directory.CreateDirectory(folder);
            var target = UniquePath(folder,
                $"{Path.GetFileNameWithoutExtension(dbPath)}-v{fromVersion}-{DateTime.UtcNow:yyyyMMdd-HHmmss}",
                ".db");

            // overwrite: false — a pre-migration backup is the only copy of the
            // pre-repair data, so it must never be clobbered by a later run.
            File.Copy(dbPath, target, overwrite: false);
            _logger.LogInformation(
                "Pre-migration backup written to {Path}. It is an exact copy of the " +
                "pre-migration data — including anything a migration is about to encrypt " +
                "or repair — so treat it with the same care as the database itself, and " +
                "consider deleting it once the app is verified working.", target);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Pre-migration backup FAILED — skipping all migrations to leave the database untouched. " +
                "They will be retried on the next start.");
            return false;
        }
    }

    /// <summary>
    /// A path in <paramref name="folder"/> that does not exist yet, suffixing
    /// -2, -3, ... on collision.
    ///
    /// The timestamp resolves to the second, which is not fine enough on its
    /// own: a migration that fails and is retried on a quick restart lands in
    /// the same second, and without this the copy would throw, the run would
    /// be skipped, and the retry would never happen.
    /// </summary>
    private static string UniquePath(string folder, string baseName, string extension)
    {
        var candidate = Path.Combine(folder, baseName + extension);
        for (var n = 2; File.Exists(candidate); n++)
        {
            candidate = Path.Combine(folder, $"{baseName}-{n}{extension}");
        }
        return candidate;
    }

    /// <summary>
    /// Per-migration change log beside the database. Written best-effort: the
    /// migration already succeeded and its result is in the app log, so losing
    /// the detail file must not roll anything back.
    /// </summary>
    private void WriteChangeLog(string? dbPath, IDbMigration migration, MigrationResult result)
    {
        if (dbPath == null || result.Details.Count == 0) return;

        try
        {
            var folder = Path.Combine(Path.GetDirectoryName(dbPath)!, "pre-migration");
            Directory.CreateDirectory(folder);
            var path = UniquePath(folder,
                $"changes-v{migration.Version}-{DateTime.UtcNow:yyyyMMdd-HHmmss}", ".log");

            var text = new StringBuilder()
                .AppendLine($"Migration {migration.Version}: {migration.Name}")
                .AppendLine($"Applied (UTC): {DateTime.UtcNow:O}")
                .AppendLine($"Examined: {result.Examined}   Changed: {result.Changed}")
                .AppendLine(new string('-', 60));
            foreach (var line in result.Details) text.AppendLine(line);

            File.WriteAllText(path, text.ToString());
            _logger.LogInformation("Migration change log written to {Path}", path);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not write the migration change log (migration itself succeeded)");
        }
    }
}
