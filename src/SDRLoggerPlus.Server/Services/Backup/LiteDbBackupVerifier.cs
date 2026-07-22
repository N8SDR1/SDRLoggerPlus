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
            // Two deliberate choices here, both learned the hard way:
            //
            // 1. We open the FileStream ourselves rather than passing a
            //    filename. When LiteDatabase's constructor throws on a corrupt
            //    file, a `using` on the database never binds and the underlying
            //    handle leaks — which would then block the caller from deleting
            //    the bad copy. Owning the stream releases it either way.
            //
            // 2. The stream is READ-ONLY. Given a writable stream it does not
            //    recognise, LiteDB formats it as a brand-new empty database
            //    rather than failing — which would report a corrupt backup as
            //    healthy *and* destroy it. Read-only makes an unreadable file
            //    throw, which is the whole point of verifying.
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var db = new LiteDatabase(fs);
            // Reading the collection catalog forces the engine to parse the
            // header pages, which throws on a corrupt or non-LiteDB file.
            _ = db.GetCollectionNames().ToList();
            return null;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }
}
