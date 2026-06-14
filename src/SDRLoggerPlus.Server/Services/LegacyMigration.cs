namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// One-time data migration from a previous QSOThief installation. SDRLoggerPlus
/// is a renamed continuation of QSOThief; on first run, an existing QSOThief
/// config dir is COPIED (never moved) into the SDRLoggerPlus location so the
/// user's log, settings, and backups carry over. The original QSOThief folder
/// is left untouched as a safety net. Runs only when SDRLoggerPlus has no
/// database of its own yet (keyed on the db file, not the directory — see below).
/// </summary>
public static class LegacyMigration
{
    public const string LegacyAppName = "QSOThief";
    public const string LegacyDbFileName = "qsothief.db";
    public const string NewDbFileName = "sdrloggerplus.db";

    /// <summary>
    /// newConfigDir is the SDRLoggerPlus config directory; the legacy dir is its
    /// sibling named QSOThief. Returns a list of migrated items (empty = no-op).
    /// </summary>
    public static List<string> MigrateIfNeeded(string newConfigDir, Action<string>? log = null)
    {
        var migrated = new List<string>();
        try
        {
            var parent = Path.GetDirectoryName(newConfigDir);
            if (string.IsNullOrEmpty(parent)) return migrated;
            var legacyDir = Path.Combine(parent, LegacyAppName);

            if (!Directory.Exists(legacyDir)) return migrated;     // no QSOThief install — nothing to migrate

            // Trigger on the absence of SDRLoggerPlus's OWN database, not the
            // config directory. The directory frequently already exists from an
            // older Log4YM install (which leaves hamlog.db behind), so the old
            // `Directory.Exists(newConfigDir)` guard made this migration silently
            // skip — abandoning the user's QSOThief log, settings and saved panel
            // layout and starting them on an empty database.
            var newDbPath = Path.Combine(newConfigDir, NewDbFileName);
            if (File.Exists(newDbPath)) return migrated;           // already has its own DB — never touch

            Directory.CreateDirectory(newConfigDir);

            CopyFile(Path.Combine(legacyDir, "config.json"), Path.Combine(newConfigDir, "config.json"), migrated);
            CopyFile(Path.Combine(legacyDir, LegacyDbFileName), newDbPath, migrated);
            CopyFile(Path.Combine(legacyDir, "backup-state.json"), Path.Combine(newConfigDir, "backup-state.json"), migrated);

            var legacyBackups = Path.Combine(legacyDir, "backups");
            if (Directory.Exists(legacyBackups))
            {
                CopyDirectory(legacyBackups, Path.Combine(newConfigDir, "backups"));
                migrated.Add("backups/");
            }

            log?.Invoke($"Migrated from QSOThief: {string.Join(", ", migrated)} (original left in place at {legacyDir})");
        }
        catch (Exception ex)
        {
            // Non-fatal: fall back to first-run behavior; the legacy data is untouched
            log?.Invoke($"QSOThief migration failed (continuing as fresh install): {ex.Message}");
        }
        return migrated;
    }

    private static void CopyFile(string source, string dest, List<string> migrated)
    {
        // Skip when the destination already exists: the SDRLoggerPlus dir may
        // pre-exist (e.g. a Log4YM config.json), and we must never clobber it.
        if (!File.Exists(source) || File.Exists(dest)) return;
        File.Copy(source, dest);
        migrated.Add(Path.GetFileName(dest));
    }

    private static void CopyDirectory(string source, string dest)
    {
        Directory.CreateDirectory(dest);
        foreach (var file in Directory.GetFiles(source))
        {
            var target = Path.Combine(dest, Path.GetFileName(file));
            if (!File.Exists(target)) File.Copy(file, target);
        }
        foreach (var dir in Directory.GetDirectories(source))
            CopyDirectory(dir, Path.Combine(dest, Path.GetFileName(dir)));
    }
}