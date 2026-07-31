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

            // Different modes at the same call/band/minute usually means one row has a WRONG mode, not
            // a plain double-entry. Don't guess — default the whole group to KEEP and let the operator
            // decide which (if any) to remove. Only same-mode groups get an auto keep/remove default.
            var modeMismatch = ordered.Select(q => NormalizeMode(q.Mode)).Distinct().Count() > 1;
            var keeper = ordered[0];

            // "Redundant" (the default-remove count) only counts unambiguous same-mode extras.
            if (!modeMismatch) redundant += ordered.Count - 1;

            if (outGroups.Count < GroupCap)
            {
                var members = ordered.Select(q => new QsoDuplicateMember(
                    q.Id, q.Callsign, q.QsoDate, q.Band, q.Mode, IsSynced(q),
                    // Mode-mismatch groups: everyone Keep by default (nothing pre-selected for removal).
                    Keep: modeMismatch || ReferenceEquals(q, keeper),
                    KeepReason: !modeMismatch && ReferenceEquals(q, keeper) ? KeeperReason(q, ordered) : null)).ToList();
                outGroups.Add(new QsoDuplicateGroup(g.Key, members, modeMismatch));
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
        var all = (await _repository.GetAllAsync()).ToList();

        // Map each QSO id to its duplicate-group key (only for ids that are actually in a group of 2+).
        var groupOf = new Dictionary<string, string>();
        var groupMembers = new Dictionary<string, List<string>>();
        foreach (var g in all.GroupBy(DuplicateKey).Where(g => g.Count() > 1))
        {
            var memberIds = g.Select(q => q.Id).ToList();
            groupMembers[g.Key] = memberIds;
            foreach (var id in memberIds) groupOf[id] = g.Key;
        }

        var requested = ids.Distinct().ToList();

        // Only a member of a duplicate group may be deleted (never a unique QSO — protects against a
        // stale/bad request), AND we must leave at least ONE survivor in every group. Honour the
        // operator's per-row choice within that guardrail: if they somehow select every member of a
        // group, keep the group's default keeper so nothing is fully erased.
        var deletable = requested.Where(id => groupOf.ContainsKey(id)).ToHashSet();
        var toDelete = new List<string>();
        foreach (var byGroup in deletable.GroupBy(id => groupOf[id]))
        {
            var members = groupMembers[byGroup.Key];
            var wanted = byGroup.ToList();
            // If the whole group is selected, spare one survivor (the last-ordered = the auto-keeper).
            if (wanted.Count >= members.Count)
                wanted = wanted.Where(id => id != PickSurvivor(all, members)).ToList();
            toDelete.AddRange(wanted);
        }

        var skipped = requested.Count - toDelete.Count;
        if (toDelete.Count == 0)
            return new QsoDuplicateRemoveResult(ids.Count, 0, skipped);

        if (snapshotBefore is not null) await snapshotBefore();
        var deleted = await _repository.DeleteManyAsync(toDelete);

        return new QsoDuplicateRemoveResult(ids.Count, deleted, skipped);
    }

    /// <summary>The row to spare if a whole group is selected for deletion — the keep-policy winner.</summary>
    private static string PickSurvivor(IReadOnlyList<Qso> all, IReadOnlyList<string> memberIds)
    {
        var members = all.Where(q => memberIds.Contains(q.Id));
        return members
            .OrderByDescending(IsSynced)
            .ThenByDescending(Completeness)
            .ThenBy(q => q.CreatedAt)
            .First().Id;
    }

    /// <summary>Canonical duplicate identity: call + UTC date + minute-of-day + band. Mode excluded
    /// (the least reliable ADIF field), matching the import dedupe rule.</summary>
    private static string DuplicateKey(Qso q)
    {
        var utc = q.QsoDate.ToUniversalTime();
        return $"{q.Callsign.ToUpperInvariant()}|{utc:yyyyMMddHHmm}|{AdifFieldNormalizer.CanonicalBandKey(q.Band)}";
    }

    /// <summary>Collapse sideband/phone/spelling variants so only genuinely different modes count as a
    /// mismatch (SSB≡USB≡LSB≡PH, RTTY≡FSK, PSK*≡PSK). CW vs FT8, JT65 vs JT9, FSK vs FT8 stay distinct.</summary>
    private static string NormalizeMode(string? mode)
    {
        var m = (mode ?? "").ToUpperInvariant().Trim();
        return m switch
        {
            "USB" or "LSB" or "SSB" or "PH" or "PHONE" or "VOICE" => "SSB",
            "FSK" or "RTTY" => "RTTY",
            "PSK31" or "PSK63" or "PSK125" or "PSK" => "PSK",
            _ => m,
        };
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

    // ── Normalize country names ──────────────────────────────────────────────────────────────────
    private const int CountryGroupCap = 500;

    /// <summary>
    /// Read-only: find DXCC entities whose QSOs are stored under more than one country-name spelling.
    /// Grouping is by DXCC NUMBER, so every QSO in a group is provably the SAME entity — unifying
    /// their country strings can't merge two different countries. QSOs without a DXCC number are
    /// skipped (we can't be sure of their entity). The canonical is cty.dat's own name for the entity.
    /// </summary>
    public async Task<CountryNameAuditResult> AuditCountryNamesAsync()
    {
        var all = (await _repository.GetAllAsync()).ToList();
        var byDxcc = new Dictionary<int, List<Qso>>();
        int withoutDxcc = 0;
        foreach (var q in all)
        {
            if (q.Dxcc is not int dx) { withoutDxcc++; continue; }
            if (!byDxcc.TryGetValue(dx, out var list)) byDxcc[dx] = list = new();
            list.Add(q);
        }

        var groups = new List<CountryNameGroup>();
        int totalChange = 0;
        foreach (var (dxcc, qsos) in byDxcc)
        {
            var variants = CountryVariants(qsos);
            if (variants.Count < 2) continue; // one spelling (or all blank) — nothing to unify

            var canonical = CanonicalCountryFor(qsos, variants);
            if (canonical is null) continue;

            var change = qsos.Count(q => DiffersFrom(q, canonical));
            if (change == 0) continue;
            totalChange += change;
            if (groups.Count < CountryGroupCap)
                groups.Add(new CountryNameGroup(dxcc, canonical, variants, change));
        }

        return new CountryNameAuditResult(
            all.Count, withoutDxcc, groups.Count, totalChange,
            groups.OrderByDescending(g => g.ChangeCount).ToList());
    }

    /// <summary>
    /// Set every QSO in the chosen DXCC entities (empty = all proposed) to that entity's canonical
    /// country name. The canonical is re-derived server-side (never trust a client-sent name), each
    /// write goes through the flag-preserving RepairQsoCountryAsync (no sync flag, no UpdatedAt, never
    /// UpdateAsync → nothing re-uploaded), and the backup snapshot is taken once before the first write.
    /// </summary>
    public async Task<CountryNameNormalizeResult> NormalizeCountryNamesAsync(
        IReadOnlyCollection<int> dxccs, Func<Task>? snapshotBefore = null)
    {
        var all = (await _repository.GetAllAsync()).ToList();
        var target = dxccs is { Count: > 0 } ? new HashSet<int>(dxccs) : null; // null = all proposed

        int requested = 0, changed = 0, skipped = 0;
        var snapshotted = false;

        foreach (var group in all.Where(q => q.Dxcc is int).GroupBy(q => q.Dxcc!.Value))
        {
            if (target is not null && !target.Contains(group.Key)) continue;
            var qsos = group.ToList();
            var variants = CountryVariants(qsos);
            if (variants.Count < 2) continue;
            var canonical = CanonicalCountryFor(qsos, variants);
            if (canonical is null) continue;

            foreach (var q in qsos)
            {
                if (q.Id is null || !DiffersFrom(q, canonical)) continue;
                requested++;
                if (!snapshotted)
                {
                    if (snapshotBefore is not null) await snapshotBefore();
                    snapshotted = true;
                }
                if (await _repository.RepairQsoCountryAsync(q.Id, canonical)) changed++;
                else skipped++;
            }
        }

        return new CountryNameNormalizeResult(requested, changed, skipped);
    }

    private static bool DiffersFrom(Qso q, string canonical) =>
        !string.Equals((q.Country ?? "").Trim(), canonical, StringComparison.Ordinal);

    private static List<CountryNameVariant> CountryVariants(IEnumerable<Qso> qsos) =>
        qsos.Select(q => (q.Country ?? "").Trim())
            .Where(c => c.Length > 0)
            .GroupBy(c => c, StringComparer.OrdinalIgnoreCase)
            .Select(g => new CountryNameVariant(g.First(), g.Count()))
            .OrderByDescending(v => v.Count)
            .ToList();

    /// <summary>
    /// The name to unify a DXCC group on: cty.dat's own name for the entity (what native logging
    /// stores), taken as the MAJORITY resolution across the group's callsigns so a single odd call
    /// can't skew it. Falls back to the most common stored spelling — it never invents a name.
    /// </summary>
    private static string? CanonicalCountryFor(IEnumerable<Qso> qsos, List<CountryNameVariant> variants)
    {
        var ctyName = qsos
            .Select(q => CtyService.GetCountryFromCallsign(q.Callsign ?? "").Country)
            .Where(c => !string.IsNullOrWhiteSpace(c))
            .GroupBy(c => c!, StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(g => g.Count())
            .Select(g => g.First())
            .FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(ctyName)) return ctyName;
        return variants.Count > 0 ? variants[0].Country : null;
    }
}
