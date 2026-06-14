namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// One-time data migration from a previous QSOThief installation. SDRLoggerPlus
/// is a renamed continuation of QSOThief; on first run, an existing QSOThief
/// config dir is COPIED (never moved) into the SDRLoggerPlus location so the
/// user's log, settings, and backups carry over. The original QSOThief folder
/// is left untouched as a safety net. Runs only when the SDRLoggerPlus dir
/// doesn't exist.
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

            if (Directory.Exists(newConfigDir)) return migrated;   // already set up — never touch
            if (!Directory.Exists(legacyDir)) return migrated;     // fresh install — nothing to migrate

            Directory.CreateDirectory(newConfigDir);

            CopyFile(Path.Combine(legacyDir, "config.json"), Path.Combine(newConfigDir, "config.json"), migrated);
            CopyFile(Path.Combine(legacyDir, LegacyDbFileName), Path.Combine(newConfigDir, NewDbFileName), migrated);
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
        if (!File.Exists(source)) return;
        File.Copy(source, dest, overwrite: false);
        migrated.Add(Path.GetFileName(dest));
    }

    private static void CopyDirectory(string source, string dest)
    {
        Directory.CreateDirectory(dest);
        foreach (var file in Directory.GetFiles(source))
            File.Copy(file, Path.Combine(dest, Path.GetFileName(file)), overwrite: false);
        foreach (var dir in Directory.GetDirectories(source))
            CopyDirectory(dir, Path.Combine(dest, Path.GetFileName(dir)));
    }
}