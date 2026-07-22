using MongoDB.Bson.Serialization.Attributes;

namespace SDRLoggerPlus.Contracts.Models;

/// <summary>
/// Why a QSL upload did not succeed. The distinction is made where the HTTP
/// status is actually known — inside each service — rather than by sniffing
/// error strings afterwards, because the difference decides whether a QSO may
/// ever be retried.
/// </summary>
public enum QslFailureKind
{
    /// <summary>Upload succeeded.</summary>
    None = 0,

    /// <summary>
    /// The service is disabled or missing credentials, so nothing was sent.
    /// This is NOT a failure and must never be recorded against a QSO —
    /// otherwise every contact would carry red marks for services the
    /// operator has deliberately not configured.
    /// </summary>
    NotConfigured = 1,

    /// <summary>
    /// Credentials were rejected. Never retry automatically: Club Log
    /// firewalls IP addresses that repeat failed POSTs, which is what its
    /// one-strike rule exists to prevent.
    /// </summary>
    Auth = 2,

    /// <summary>
    /// The service understood the request and refused this QSO (malformed
    /// field, duplicate rejection). Retrying the identical payload cannot
    /// help; the operator has to fix the QSO.
    /// </summary>
    Rejected = 3,

    /// <summary>
    /// Server error, timeout, or network failure. The QSO is fine and the
    /// upload is worth retrying later — this is the only kind a future
    /// outbox should pick up.
    /// </summary>
    Temporary = 4,
}

/// <summary>
/// Outcome of one upload attempt. A readonly record struct so it stays
/// allocation-free on the logging hot path.
/// </summary>
public readonly record struct QslUploadResult(bool Ok, string? Error, QslFailureKind Kind)
{
    public static QslUploadResult Success() => new(true, null, QslFailureKind.None);

    public static QslUploadResult Fail(QslFailureKind kind, string error) => new(false, error, kind);

    /// <summary>Nothing was sent because the service is off or unconfigured.</summary>
    public static QslUploadResult NotConfigured(string reason) =>
        new(false, reason, QslFailureKind.NotConfigured);

    /// <summary>True when this attempt should be written to the QSO's ledger.</summary>
    public bool IsRecordable => Kind != QslFailureKind.NotConfigured;
}

/// <summary>
/// Per-service upload state for one QSO.
///
/// Mirrors the shape the QRZ and LoTW fields already established on
/// <see cref="Qso"/>, but nested so that adding a service costs one property
/// rather than four more columns on the QSO root.
/// </summary>
public class QslServiceSync
{
    [BsonElement("status")]
    [BsonRepresentation(MongoDB.Bson.BsonType.String)]
    public SyncStatus Status { get; set; } = SyncStatus.NotSynced;

    /// <summary>Set only on success, and never cleared by a later failure —
    /// "it did reach Club Log on this date" stays true even if a subsequent
    /// re-send fails.</summary>
    [BsonElement("syncedAt")]
    public DateTime? SyncedAt { get; set; }

    [BsonElement("lastAttemptAt")]
    public DateTime? LastAttemptAt { get; set; }

    /// <summary>Why the last attempt failed; null once an attempt succeeds.</summary>
    [BsonElement("lastError")]
    public string? LastError { get; set; }

    [BsonElement("failureKind")]
    [BsonRepresentation(MongoDB.Bson.BsonType.String)]
    public QslFailureKind FailureKind { get; set; } = QslFailureKind.None;

    /// <summary>Consecutive failed attempts; reset to zero on success.</summary>
    [BsonElement("attempts")]
    public int Attempts { get; set; }

    /// <summary>
    /// Whether a retry could plausibly succeed. Only transient failures
    /// qualify — auth failures and per-QSO rejections would just repeat, and
    /// in Club Log's case repeating is what gets an IP banned.
    /// </summary>
    public bool IsRetryable => Status == SyncStatus.NotSynced && FailureKind == QslFailureKind.Temporary;
}

/// <summary>
/// The QSL sync ledger for one QSO.
///
/// A null ledger — or a null entry within it — means "logged before this
/// tracking existed", which is deliberately distinct from NotSynced. Roughly
/// 24,000 QSOs predate it, and their true upload state is unknowable. Treating
/// them as NotSynced would make a future "Sync All" re-post the entire log to
/// realtime.php and earn an immediate IP ban, so unknown must stay unknown and
/// must never be eligible for bulk sync.
///
/// QRZ and LoTW keep their existing top-level fields rather than moving here:
/// migrating live sync state to gain tidiness would risk re-uploading QSOs
/// that are already confirmed.
/// </summary>
public class QslSyncLedger
{
    [BsonElement("clubLog")]
    public QslServiceSync? ClubLog { get; set; }

    [BsonElement("hrdLog")]
    public QslServiceSync? HrdLog { get; set; }

    [BsonElement("eqsl")]
    public QslServiceSync? Eqsl { get; set; }

    /// <summary>Canonical service keys, used by the API and the recorder.</summary>
    public const string ClubLogKey = "clublog";
    public const string HrdLogKey = "hrdlog";
    public const string EqslKey = "eqsl";

    public QslServiceSync? For(string service) => service switch
    {
        ClubLogKey => ClubLog,
        HrdLogKey => HrdLog,
        EqslKey => Eqsl,
        _ => null,
    };

    public void Set(string service, QslServiceSync entry)
    {
        switch (service)
        {
            case ClubLogKey: ClubLog = entry; break;
            case HrdLogKey: HrdLog = entry; break;
            case EqslKey: Eqsl = entry; break;
            default: throw new ArgumentOutOfRangeException(nameof(service), service, "Unknown QSL service");
        }
    }
}
