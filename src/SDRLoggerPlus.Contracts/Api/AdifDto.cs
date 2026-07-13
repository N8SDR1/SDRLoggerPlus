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

/// <summary>Which confirmation channel an imported report should stamp.</summary>
public enum ConfirmationSource
{
    Lotw,
    Eqsl,
    Qrz,
    Card
}

/// <summary>
/// Result of merging a confirmation report (LoTW / eQSL / card ADIF) against the
/// existing log — matched records are marked confirmed, never duplicated.
/// </summary>
public record ConfirmationMergeResponse(
    int TotalRecords,
    int Matched,
    int Updated,
    int AlreadyConfirmed,
    int Unmatched
);
