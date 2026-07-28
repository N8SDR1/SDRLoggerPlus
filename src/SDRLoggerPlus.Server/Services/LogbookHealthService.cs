using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services.Adif;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// "Logbook health" maintenance — read-only scans + guarded repairs the operator opts into.
///
/// First tool: Verify QSO times (docs/design/timezone-architecture.md §5a). It finds rows whose
/// stored QsoDate lost its time-of-day to the old edit-modal flatten bug (QsoDate=00:00 while the
/// real time survives in the TimeOn string) and can reconstruct them. It never guesses at a wrong
/// DATE (unrecoverable from the row) and only ever repairs on explicit confirmation.
/// </summary>
public class LogbookHealthService
{
    private readonly IQsoRepository _repository;

    public LogbookHealthService(IQsoRepository repository) => _repository = repository;

    private const int SampleCap = 200;

    /// <summary>Read-only: bucket every QSO by whether its QsoDate time agrees with TimeOn.</summary>
    public async Task<QsoTimeAuditResult> AuditQsoTimesAsync()
    {
        var all = await _repository.GetAllAsync();

        int consistent = 0, fixable = 0, ambiguous = 0;
        var fixableSamples = new List<QsoTimeIssue>();
        var ambiguousSamples = new List<QsoTimeIssue>();

        foreach (var q in all)
        {
            var (bucket, proposed) = Classify(q);
            switch (bucket)
            {
                case QsoTimeBucket.Consistent:
                    consistent++;
                    break;
                case QsoTimeBucket.FixableLostTime:
                    fixable++;
                    if (fixableSamples.Count < SampleCap)
                        fixableSamples.Add(new QsoTimeIssue(q.Id, q.Callsign, q.QsoDate, q.TimeOn, bucket, proposed));
                    break;
                case QsoTimeBucket.Ambiguous:
                    ambiguous++;
                    if (ambiguousSamples.Count < SampleCap)
                        ambiguousSamples.Add(new QsoTimeIssue(q.Id, q.Callsign, q.QsoDate, q.TimeOn, bucket, null));
                    break;
            }
        }

        return new QsoTimeAuditResult(
            all.Count(), consistent, fixable, ambiguous, fixableSamples, ambiguousSamples);
    }

    /// <summary>
    /// Repair the chosen fixable-lost-time rows: reconstruct QsoDate from TimeOn. Each id is
    /// RE-VALIDATED as still FixableLostTime before any write (so a stale request can't corrupt a
    /// row that changed meanwhile), and written via the repository's flag-preserving path — no sync
    /// flag touched, no UpdatedAt bump, never through UpdateAsync, so nothing is re-uploaded.
    ///
    /// <paramref name="snapshotBefore"/> is awaited ONCE before the first write; if it throws, the
    /// repair aborts having changed nothing (no snapshot ⇒ no repair). The caller wires it to a real
    /// backup. When there is nothing to repair, no snapshot is taken.
    /// </summary>
    public async Task<QsoTimeRepairResult> RepairFixableTimesAsync(
        IReadOnlyCollection<string> ids, Func<Task>? snapshotBefore = null)
    {
        int repaired = 0, skipped = 0;
        var snapshotted = false;

        foreach (var id in ids.Distinct())
        {
            var qso = await _repository.GetByIdAsync(id);
            if (qso is null) { skipped++; continue; }

            var (bucket, proposed) = Classify(qso);
            if (bucket != QsoTimeBucket.FixableLostTime || proposed is null) { skipped++; continue; }

            if (!snapshotted)
            {
                if (snapshotBefore is not null) await snapshotBefore();
                snapshotted = true;
            }

            if (await _repository.RepairQsoDateAsync(id, proposed.Value)) repaired++;
            else skipped++;
        }

        return new QsoTimeRepairResult(ids.Count, repaired, skipped);
    }

    // ── Find duplicates (whole-log) ──────────────────────────────────────────────────────────────
    private const int GroupCap = 500;

    /// <summary>
    /// Read-only: group QSOs by canonical identity (call + UTC date + minute + band; mode excluded,
    /// matching the importer) and, for each group of 2+, decide which single row to KEEP. Keep policy:
    /// prefer the copy already synced to QRZ/LoTW (keeps local ↔ online consistent), then the most
    /// complete record, then the earliest CreatedAt. The others are proposed for deletion.
    /// </summary>
    public async Task<QsoDuplicateScanResult> FindDuplicatesAsync()
    {
        var all = (await _repository.GetAllAsync()).ToList();

        var groups = all
            .GroupBy(DuplicateKey)
            .Where(g => g.Count() > 1)
            .ToList();

        int redundant = 0;
        var outGroups = new List<QsoDuplicateGroup>();

        foreach (var g in groups)
        {
            // Keeper first: synced, then completeness, then oldest.
            var ordered = g
                .OrderByDescending(IsSynced)
                .ThenByDescending(Completeness)
                .ThenBy(q => q.CreatedAt)
                .ToList();

            var keeper = ordered[0];
            redundant += ordered.Count - 1;

            if (outGroups.Count < GroupCap)
            {
                var members = ordered.Select(q => new QsoDuplicateMember(
                    q.Id, q.Callsign, q.QsoDate, q.Band, q.Mode, IsSynced(q),
                    Keep: ReferenceEquals(q, keeper),
                    KeepReason: ReferenceEquals(q, keeper) ? KeeperReason(q, ordered) : null)).ToList();
                outGroups.Add(new QsoDuplicateGroup(g.Key, members));
            }
        }

        return new QsoDuplicateScanResult(all.Count, groups.Count, redundant, outGroups);
    }

    /// <summary>
    /// Delete the chosen redundant rows. Recomputes the keep/delete decision server-side and only
    /// deletes ids that are genuinely a NON-keeper in a duplicate group — so a stale or malicious
    /// request can never delete a keeper or a unique QSO. Deletes are local-only (no QRZ/LoTW cascade)
    /// and a snapshot is taken before the first delete.
    /// </summary>
    public async Task<QsoDuplicateRemoveResult> RemoveDuplicatesAsync(
        IReadOnlyCollection<string> ids, Func<Task>? snapshotBefore = null)
    {
        var scan = await FindDuplicatesAsync();
        var deletable = scan.Groups
            .SelectMany(g => g.Members)
            .Where(m => !m.Keep)
            .Select(m => m.Id)
            .ToHashSet();

        var toDelete = ids.Distinct().Where(deletable.Contains).ToList();
        var skipped = ids.Distinct().Count() - toDelete.Count;

        if (toDelete.Count == 0)
            return new QsoDuplicateRemoveResult(ids.Count, 0, skipped);

        if (snapshotBefore is not null) await snapshotBefore();
        var deleted = await _repository.DeleteManyAsync(toDelete);

        return new QsoDuplicateRemoveResult(ids.Count, deleted, skipped);
    }

    /// <summary>Canonical duplicate identity: call + UTC date + minute-of-day + band. Mode excluded
    /// (the least reliable ADIF field), matching the import dedupe rule.</summary>
    private static string DuplicateKey(Qso q)
    {
        var utc = q.QsoDate.ToUniversalTime();
        return $"{q.Callsign.ToUpperInvariant()}|{utc:yyyyMMddHHmm}|{AdifFieldNormalizer.CanonicalBandKey(q.Band)}";
    }

    private static bool IsSynced(Qso q) =>
        q.QrzSyncStatus == SyncStatus.Synced ||
        string.Equals(q.Qsl?.Lotw?.Sent, "Y", StringComparison.OrdinalIgnoreCase);

    /// <summary>How much a row carries — used to keep the richest copy.</summary>
    private static int Completeness(Qso q)
    {
        int n = 0;
        if (!string.IsNullOrWhiteSpace(q.Name) || !string.IsNullOrWhiteSpace(q.Station?.Name)) n++;
        if (!string.IsNullOrWhiteSpace(q.Grid) || !string.IsNullOrWhiteSpace(q.Station?.Grid)) n++;
        if (!string.IsNullOrWhiteSpace(q.Country) || !string.IsNullOrWhiteSpace(q.Station?.Country)) n++;
        if (!string.IsNullOrWhiteSpace(q.RstSent)) n++;
        if (!string.IsNullOrWhiteSpace(q.RstRcvd)) n++;
        if (!string.IsNullOrWhiteSpace(q.Comment)) n++;
        if (!string.IsNullOrWhiteSpace(q.Notes)) n++;
        if (q.Frequency is > 0) n++;
        if (q.AdifExtra is not null && q.AdifExtra.ElementCount > 0) n++;
        return n;
    }

    private static string KeeperReason(Qso keeper, IReadOnlyList<Qso> group)
    {
        if (IsSynced(keeper) && group.Count(IsSynced) == 1) return "already synced to QRZ/LoTW";
        var maxComplete = group.Max(Completeness);
        if (Completeness(keeper) == maxComplete && group.Count(q => Completeness(q) == maxComplete) == 1)
            return "most complete record";
        return "earliest logged";
    }

    /// <summary>
    /// Classify one QSO. Compares the UTC time-of-day of QsoDate against the TimeOn string at
    /// minute granularity. Returns the bucket and, for FixableLostTime, the corrected UTC instant.
    /// </summary>
    internal static (QsoTimeBucket Bucket, DateTime? Proposed) Classify(Qso q)
    {
        var utc = q.QsoDate.ToUniversalTime();
        var qMin = utc.Hour * 60 + utc.Minute;
        var tMin = TimeOnMinutes(q.TimeOn);

        // No usable TimeOn — nothing to compare against, leave it for manual review.
        if (tMin is null)
            return qMin == 0 ? (QsoTimeBucket.Ambiguous, null) : (QsoTimeBucket.Consistent, null);

        if (qMin == tMin.Value)
            return (QsoTimeBucket.Consistent, null);

        // QsoDate flattened to midnight while TimeOn kept the real time → reconstruct it.
        if (qMin == 0 && tMin.Value > 0)
        {
            var proposed = DateTime.SpecifyKind(
                utc.Date.AddMinutes(tMin.Value), DateTimeKind.Utc);
            return (QsoTimeBucket.FixableLostTime, proposed);
        }

        // Any other disagreement (incl. a possibly-wrong date) — no safe signal to correct.
        return (QsoTimeBucket.Ambiguous, null);
    }

    /// <summary>ADIF TIME_ON ("HHmm"/"HHmmss", possibly with colons) → minutes past UTC midnight, or null.</summary>
    internal static int? TimeOnMinutes(string? timeOn)
    {
        if (string.IsNullOrWhiteSpace(timeOn)) return null;
        var digits = new string(timeOn.Where(char.IsDigit).ToArray());
        if (digits.Length < 4) digits = digits.PadLeft(4, '0');
        if (digits.Length < 4) return null;
        if (!int.TryParse(digits.AsSpan(0, 2), out var hh)) return null;
        if (!int.TryParse(digits.AsSpan(2, 2), out var mm)) return null;
        if (hh > 23 || mm > 59) return null;
        return hh * 60 + mm;
    }
}
