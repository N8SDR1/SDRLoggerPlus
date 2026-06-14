namespace SDRLoggerPlus.Contracts.Api;

public record AdifImportResponse(
    int TotalRecords,
    int ImportedCount,
    int SkippedDuplicates,
    int ErrorCount,
    IEnumerable<string> Errors
);

public record AdifExportRequest(
    string? Callsign = null,
    string? Band = null,
    string? Mode = null,
    DateTime? FromDate = null,
    DateTime? ToDate = null,
    IEnumerable<string>? QsoIds = null
);
