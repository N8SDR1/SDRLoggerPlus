using LiteDB;

namespace SDRLoggerPlus.Server.Services.Backup;

/// <summary>
/// Verifies that a freshly written LiteDB backup copy is a structurally valid,
/// openable database — not a torn, truncated, or corrupt file that only looks
/// complete on disk. A bad backup is worthless precisely when it is needed, so
/// we prove it opens at creation time instead of during a restore emergency.
/// Returns null on success, or a short error description on failure.
/// </summary>
public static class LiteDbBackupVerifier
{
    public static string? Verify(string path)
    {
        try
        {
            // ReadOnly opens the file without writing a WAL/-log sidecar into the
            // backup folder; reading the collection catalog forces the engine to
            // parse the header pages, which throws on a corrupt/invalid file.
            using var db = new LiteDatabase($"Filename={path};ReadOnly=true");
            _ = db.GetCollectionNames().ToList();
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }
}
