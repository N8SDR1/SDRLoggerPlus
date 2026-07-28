using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;

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
