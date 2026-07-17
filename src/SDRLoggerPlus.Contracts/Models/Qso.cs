using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace SDRLoggerPlus.Contracts.Models;

/// <summary>
/// Sync status for external services (QRZ, LoTW, etc.)
/// Follows QLog's Y/N/M pattern for efficient incremental sync
/// </summary>
public enum SyncStatus
{
    /// <summary>Not yet synced to the service</summary>
    NotSynced = 0,
    /// <summary>Successfully synced, no changes since</summary>
    Synced = 1,
    /// <summary>Was synced but has been modified since - needs re-sync</summary>
    Modified = 2
}

public class Qso
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string Id { get; set; } = null!;

    [BsonElement("call")]
    public string Callsign { get; set; } = null!;

    [BsonElement("qso_datetime")]
    public DateTime QsoDate { get; set; }

    [BsonElement("time_on")]
    public string TimeOn { get; set; } = null!;

    [BsonElement("time_off")]
    public string? TimeOff { get; set; }

    [BsonElement("band")]
    public string Band { get; set; } = null!;

    [BsonElement("mode")]
    public string Mode { get; set; } = null!;

    [BsonElement("freq")]
    public double? Frequency { get; set; }

    [BsonElement("rst_sent")]
    public string? RstSent { get; set; }

    [BsonElement("rst_rcvd")]
    public string? RstRcvd { get; set; }

    [BsonElement("name")]
    public string? Name { get; set; }

    [BsonElement("country")]
    public string? Country { get; set; }

    [BsonElement("gridsquare")]
    public string? Grid { get; set; }

    [BsonElement("dxcc")]
    public int? Dxcc { get; set; }

    [BsonElement("cont")]
    public string? Continent { get; set; }

    [BsonElement("station")]
    public StationInfo? Station { get; set; }

    [BsonElement("qsl")]
    public QslStatus? Qsl { get; set; }

    [BsonElement("contest")]
    public ContestInfo? Contest { get; set; }

    [BsonElement("comment")]
    public string? Comment { get; set; }

    [BsonElement("notes")]
    public string? Notes { get; set; }

    [BsonExtraElements]
    public BsonDocument? AdifExtra { get; set; }

    [BsonElement("imported_at")]
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    [BsonElement("updatedAt")]
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;

    // QRZ sync tracking
    [BsonElement("qrzLogId")]
    public string? QrzLogId { get; set; }

    [BsonElement("qrzSyncedAt")]
    public DateTime? QrzSyncedAt { get; set; }

    [BsonElement("qrzSyncStatus")]
    [BsonRepresentation(BsonType.String)]
    public SyncStatus QrzSyncStatus { get; set; } = SyncStatus.NotSynced;

    // LOTW sync tracking
    [BsonElement("lotwSyncedAt")]
    public DateTime? LotwSyncedAt { get; set; }

    [BsonElement("lotwSyncStatus")]
    [BsonRepresentation(BsonType.String)]
    public SyncStatus LotwSyncStatus { get; set; } = SyncStatus.NotSynced;
}

public class StationInfo
{
    [BsonElement("name")]
    public string? Name { get; set; }

    [BsonElement("qth")]
    public string? Qth { get; set; }

    [BsonElement("grid")]
    public string? Grid { get; set; }

    [BsonElement("country")]
    public string? Country { get; set; }

    [BsonElement("dxcc")]
    public int? Dxcc { get; set; }

    [BsonElement("cqZone")]
    public int? CqZone { get; set; }

    [BsonElement("ituZone")]
    public int? ItuZone { get; set; }

    [BsonElement("state")]
    public string? State { get; set; }

    [BsonElement("county")]
    public string? County { get; set; }

    [BsonElement("continent")]
    public string? Continent { get; set; }

    [BsonElement("latitude")]
    public double? Latitude { get; set; }

    [BsonElement("longitude")]
    public double? Longitude { get; set; }
}

public class QslStatus
{
    [BsonElement("sent")]
    public string? Sent { get; set; }

    [BsonElement("sentDate")]
    public DateTime? SentDate { get; set; }

    [BsonElement("rcvd")]
    public string? Rcvd { get; set; }

    [BsonElement("rcvdDate")]
    public DateTime? RcvdDate { get; set; }

    [BsonElement("lotw")]
    public LotwStatus? Lotw { get; set; }

    [BsonElement("eqsl")]
    public EqslStatus? Eqsl { get; set; }
}

public class LotwStatus
{
    [BsonElement("sent")]
    public string? Sent { get; set; }

    [BsonElement("sentDate")]
    public DateTime? SentDate { get; set; }

    [BsonElement("rcvd")]
    public string? Rcvd { get; set; }

    [BsonElement("rcvdDate")]
    public DateTime? RcvdDate { get; set; }
}

public class EqslStatus
{
    [BsonElement("sent")]
    public string? Sent { get; set; }

    [BsonElement("rcvd")]
    public string? Rcvd { get; set; }
}

public class ContestInfo
{
    // The ContestDefinition id (maps to/from ADIF CONTEST_ID).
    [BsonElement("id")]
    public string? ContestId { get; set; }

    // Which ContestSession this QSO belongs to.
    [BsonElement("sessionId")]
    public string? SessionId { get; set; }

    [BsonElement("serialSent")]
    public string? SerialSent { get; set; }

    [BsonElement("serialRcvd")]
    public string? SerialRcvd { get; set; }

    // Raw received exchange text (whatever the operator typed).
    [BsonElement("exchange")]
    public string? Exchange { get; set; }

    // All received-exchange field values keyed by the definition's field keys
    // (e.g. {"age":"42","check":"73","prec":"A"}). Preserves fidelity for exotic
    // exchanges the typed fields below don't cover, so Cabrillo can emit them.
    [BsonElement("rcvdFields")]
    public Dictionary<string, string>? RcvdFields { get; set; }

    // Structured received-exchange components used for scoring / Cabrillo.
    [BsonElement("rcvdZone")]
    public string? RcvdZone { get; set; }

    [BsonElement("rcvdState")]
    public string? RcvdState { get; set; }

    [BsonElement("rcvdSection")]
    public string? RcvdSection { get; set; }

    [BsonElement("rcvdName")]
    public string? RcvdName { get; set; }

    [BsonElement("rcvdPower")]
    public string? RcvdPower { get; set; }

    [BsonElement("rcvdGrid")]
    public string? RcvdGrid { get; set; }

    // Snapshot of the engine's evaluation at log time (keeps exports reproducible).
    [BsonElement("qsoPoints")]
    public int? QsoPoints { get; set; }

    [BsonElement("isDupe")]
    public bool IsDupe { get; set; }

    // Multiplier keys this QSO claimed (source-qualified, e.g. "CqZone:14@20M").
    [BsonElement("mults")]
    public List<string>? Mults { get; set; }
}
