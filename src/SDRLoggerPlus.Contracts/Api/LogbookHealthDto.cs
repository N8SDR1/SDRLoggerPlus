namespace SDRLoggerPlus.Contracts.Api;

/// <summary>
/// Which bucket a QSO's time falls into during a "Verify QSO times" scan.
/// See docs/design/timezone-architecture.md §5a.
/// </summary>
public enum QsoTimeBucket
{
    /// <summary>QsoDate's time-of-day agrees with the TimeOn string — nothing to do.</summary>
    Consistent = 0,
    /// <summary>QsoDate time is 00:00 but TimeOn holds a real time (the old edit-modal flatten):
    /// the lost time can be reconstructed from TimeOn. The ONLY bucket the tool repairs.</summary>
    FixableLostTime = 1,
    /// <summary>QsoDate and TimeOn disagree in some other way, or TimeOn is missing — no safe
    /// signal to correct it (a wrong date can't be recovered from the row). Reported, never changed.</summary>
    Ambiguous = 2,
}

/// <summary>One QSO flagged by the time-audit scan (sample rows for the review UI).</summary>
public record QsoTimeIssue(
    string Id,
    string Callsign,
    DateTime QsoDate,
    string? TimeOn,
    QsoTimeBucket Bucket,
    /// <summary>For FixableLostTime: the corrected UTC instant we would write (date + TimeOn).</summary>
    DateTime? ProposedQsoDate);

/// <summary>Outcome of repairing the fixable-lost-time rows.</summary>
public record QsoTimeRepairResult(int Requested, int Repaired, int Skipped);

/// <summary>Request body: which fixable QSO ids to repair (from the scan's FixableSamples).</summary>
public record QsoTimeRepairRequest(IReadOnlyList<string> Ids);

/// <summary>Read-only result of a "Verify QSO times" scan over the whole logbook.</summary>
public record QsoTimeAuditResult(
    int Total,
    int Consistent,
    int FixableLostTime,
    int Ambiguous,
    IReadOnlyList<QsoTimeIssue> FixableSamples,
    IReadOnlyList<QsoTimeIssue> AmbiguousSamples);
