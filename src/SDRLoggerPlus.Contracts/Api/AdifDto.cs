namespace SDRLoggerPlus.Contracts.Api;

public record AdifImportResponse(
    int TotalRecords,
    int ImportedCount,
    int SkippedDuplicates,
    int ErrorCount,
    IEnumerable<string> Errors,
    // Band/mode values that had to be corrected, or that this app does not recognise and
    // stored unchanged. Previously the import reported only the counts above, so a file full
    // of malformed modes was indistinguishable from a clean one.
    IEnumerable<AdifImportIssueDto>? Issues = null
);

/// <summary>
/// One distinct import problem and how many records it affected. Grouped, not per-record:
/// "'FT2' is not an ADIF mode — 114 records" is actionable; 114 identical lines are not.
/// </summary>
/// <param name="Action">Corrected (rewritten, no meaning lost) or Flagged (kept as-is).</param>
public record AdifImportIssueDto(
    string Field,
    string OriginalValue,
    string StoredValue,
    string Action,
    int Count,
    string Note
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
