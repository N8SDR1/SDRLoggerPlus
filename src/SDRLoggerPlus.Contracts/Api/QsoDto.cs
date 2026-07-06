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
    // v1.x POTA-mode fields — MyPotaRef is the park the operator is
    // activating (e.g. "K-1234"), PotaRef is the worked station's park
    // when it's a park-to-park contact. Both stored on the QSO via
    // AdifExtra so the existing PotaStatistics service picks them up
    // and QSOs round-trip cleanly through ADIF export.
    string? MyPotaRef = null,
    string? PotaRef = null
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
    DateTime CreatedAt
);

public record StationInfoDto(
    string? Name,
    string? Grid,
    string? Country,
    int? Dxcc,
    string? State,
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
    int Limit = 50,
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
