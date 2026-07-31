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

// ── Find duplicates ─────────────────────────────────────────────────────────────

/// <summary>One QSO in a duplicate group.</summary>
public record QsoDuplicateMember(
    string Id,
    string Callsign,
    DateTime QsoDate,
    string Band,
    string Mode,
    bool Synced,
    /// <summary>True for the row we keep; false for the redundant copies proposed for deletion.</summary>
    bool Keep,
    /// <summary>Why this row was chosen as the keeper (shown in the UI).</summary>
    string? KeepReason);

/// <summary>A set of QSOs sharing one canonical identity (call + UTC date + minute + band).</summary>
public record QsoDuplicateGroup(
    string Key,
    IReadOnlyList<QsoDuplicateMember> Members,
    /// <summary>True when the members disagree on MODE — likely one has a wrong mode rather than being a
    /// plain double-entry, so the tool defaults to keeping all and lets the operator choose.</summary>
    bool ModeMismatch);

/// <summary>Read-only result of a whole-log duplicate scan.</summary>
public record QsoDuplicateScanResult(
    int Total,
    int GroupCount,
    int RedundantCount,
    IReadOnlyList<QsoDuplicateGroup> Groups);

/// <summary>Request body: the redundant QSO ids to delete (the non-keeper rows).</summary>
public record QsoDuplicateRemoveRequest(IReadOnlyList<string> Ids);

/// <summary>Outcome of removing duplicates.</summary>
public record QsoDuplicateRemoveResult(int Requested, int Deleted, int Skipped);

// ── Normalize country names ─────────────────────────────────────────────────────

/// <summary>One country-name spelling used within a DXCC entity, and how many QSOs use it.</summary>
public record CountryNameVariant(string Country, int Count);

/// <summary>
/// A DXCC entity whose QSOs are stored under more than one country-name spelling (e.g. "USA",
/// "UNITED STATES OF AMERICA", "United States") — all the SAME entity by DXCC number, so they can
/// be safely unified. Only QSOs carrying a DXCC number are considered.
/// </summary>
public record CountryNameGroup(
    int Dxcc,
    /// <summary>The name every QSO in this entity would be set to (cty.dat's canonical name).</summary>
    string Canonical,
    IReadOnlyList<CountryNameVariant> Variants,
    /// <summary>How many QSOs would actually change (their Country differs from Canonical).</summary>
    int ChangeCount);

/// <summary>Read-only result of a country-name consistency scan.</summary>
public record CountryNameAuditResult(
    int TotalQsos,
    /// <summary>QSOs with no DXCC number — skipped, since they can't be grouped by entity safely.</summary>
    int WithoutDxcc,
    int GroupCount,
    int ChangeCount,
    IReadOnlyList<CountryNameGroup> Groups);

/// <summary>Request body: which DXCC entities to normalize (from the scan). Empty = all proposed.</summary>
public record CountryNameNormalizeRequest(IReadOnlyList<int> Dxccs);

/// <summary>Outcome of a country-name normalization.</summary>
public record CountryNameNormalizeResult(int Requested, int Changed, int Skipped);
