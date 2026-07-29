namespace SDRLoggerPlus.Contracts.Api;

public record CreateQsoRequest(
    string Callsign,
    DateTime QsoDate,
    string TimeOn,
    string Band,
    string Mode,
    double? Frequency = null,
    string? RstSent = null,
    string? RstRcvd = null,
    string? Name = null,
    string? Grid = null,
    string? Country = null,
    string? Comment = null,
    // v1.x General-mode fields ported into v2 (contest name, general
    // notes distinct from remarks, and worked-station QTH string).
    string? Contest = null,
    string? Notes = null,
    string? Qth = null,
    // Worked-station US state + county (bare name, no "ST," prefix), usually
    // auto-filled from the callbook lookup. County feeds USA-CA tracking.
    string? State = null,
    string? County = null,
    // v1.x POTA-mode fields — MyPotaRef is the park the operator is
    // activating (e.g. "K-1234"), PotaRef is the worked station's park
    // when it's a park-to-park contact. Both stored on the QSO via
    // AdifExtra so the existing PotaStatistics service picks them up
    // and QSOs round-trip cleanly through ADIF export.
    string? MyPotaRef = null,
    string? PotaRef = null,
    // v1.x SAT-mode fields — Satellite is the bird name (e.g. "SO-50"),
    // UplinkFreq/DownlinkFreq are MHz, UpMode/DownMode are per-leg modes
    // (SSB/FM/CW/etc). Stored via AdifExtra so ADIF export preserves
    // LoTW satellite-credit fields: sat_name / prop_mode=SAT / freq_rx
    // (downlink) / down_mode. Primary Frequency/Mode = uplink leg.
    string? Satellite = null,
    double? UplinkFreq = null,
    double? DownlinkFreq = null,
    string? UpMode = null,
    string? DownMode = null
);

public record UpdateQsoRequest(
    string? Callsign = null,
    DateTime? QsoDate = null,
    string? TimeOn = null,
    string? Band = null,
    string? Mode = null,
    double? Frequency = null,
    string? RstSent = null,
    string? RstRcvd = null,
    string? Name = null,
    string? Grid = null,
    string? Country = null,
    string? State = null,
    string? County = null,
    string? Comment = null
);

public record QsoResponse(
    string Id,
    string Callsign,
    DateTime QsoDate,
    string TimeOn,
    string? TimeOff,
    string Band,
    string Mode,
    double? Frequency,
    string? RstSent,
    string? RstRcvd,
    StationInfoDto? Station,
    string? Comment,
    DateTime CreatedAt,
    // The contest this QSO was logged under (ContestDefinition id / ADIF CONTEST_ID),
    // null for casual QSOs. Surfaced as the Log History "Contest" column.
    string? ContestId = null,
    // The bird, for satellite QSOs (ADIF SAT_NAME, held in AdifExtra). Without
    // this the name goes in through Log Entry and out through ADIF export but
    // never comes back from the log API, so no UI can show or filter on it.
    string? Satellite = null,
    // Per-QSO confirmation status for the Log History "QSL" column.
    bool ConfirmedLotw = false,
    bool ConfirmedEqsl = false,
    bool ConfirmedQrz = false,
    bool ConfirmedCard = false,
    // Upload state per QSL service for the Log History "Sync" column. Null
    // means the QSO predates upload tracking — deliberately distinct from
    // "not sent", because for those QSOs we genuinely do not know.
    QslSyncDto? QslSync = null,
    // The QSO's QTH (City, State) as logged — used for call-history prefill.
    string? Qth = null,
    // The operating callsign this QSO was made under (contest sessions); null = personal/global.
    // Non-null + != station call → Log History badges it and it's excluded from personal upload/awards.
    string? StationCallsign = null
);

/// <summary>Upload state for one QSL service, as shown in Log History.</summary>
public record QslServiceSyncDto(
    string Status,
    DateTime? SyncedAt,
    DateTime? LastAttemptAt,
    string? LastError,
    string FailureKind,
    int Attempts,
    bool Retryable
);

public record QslSyncDto(
    QslServiceSyncDto? ClubLog,
    QslServiceSyncDto? HrdLog,
    QslServiceSyncDto? Eqsl
);

/// <summary>Ledger totals for the whole log, per service.</summary>
public record QslSyncSummaryDto(
    int Total,
    int Synced,
    int Failed,
    int Retryable,
    int Untracked
);

/// <summary>Ids to remove in one call — the Log History multi-select delete.</summary>
public record BulkDeleteQsosRequest(List<string> Ids);

/// <summary>
/// Deleted counts what actually existed; Requested is what the caller asked
/// for. They differ when a QSO was already gone (deleted in another window),
/// which the UI reports rather than treating as failure.
/// </summary>
public record BulkDeleteQsosResponse(int Deleted, int Requested);

public record StationInfoDto(
    string? Name,
    string? Grid,
    string? Country,
    int? Dxcc,
    string? State,
    string? County,
    string? Continent,
    double? Latitude,
    double? Longitude
);

public record QsoSearchRequest(
    string? Callsign = null,
    string? Name = null,
    string? Band = null,
    string? Mode = null,
    DateTime? FromDate = null,
    DateTime? ToDate = null,
    int? Dxcc = null,
    // Page size; null means UNBOUNDED — every match. The null form exists for
    // whole-log operations (ADIF export, LoTW upload selection) where a cap
    // would silently truncate someone's backup. Grids should keep paging.
    int? Limit = 50,
    int Skip = 0
);

public record PaginatedQsoResponse(
    IEnumerable<QsoResponse> Items,
    int TotalCount,
    int Page,
    int PageSize,
    int TotalPages
);

public record QsoStatistics(
    int TotalQsos,
    int UniqueCallsigns,
    int UniqueCountries,
    int UniqueGrids,
    int QsosToday,
    Dictionary<string, int> QsosByBand,
    Dictionary<string, int> QsosByMode
);
