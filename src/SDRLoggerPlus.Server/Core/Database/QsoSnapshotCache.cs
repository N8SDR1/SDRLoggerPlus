using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Core.Database;

/// <summary>
/// Whole-log snapshot shared by the award statistics, which are pure
/// read-only aggregations over every QSO.
///
/// Measured on a 24.5k-QSO log: every award endpoint cost ~0.7s, and the
/// simplest award (WAC — six continents) cost the same as the heaviest
/// (DXCC — per-entity per-band). The time is the LiteDB fetch and
/// deserialization, not the aggregation, so one shared snapshot fixes all
/// of them and caching computed award results would not.
///
/// Invalidation is explicit rather than time-based: a TTL would leave the
/// awards panel claiming "not worked" right after the operator logs a new
/// country, which is the one moment the panel matters. Every write path
/// lives in LiteQsoRepository, so the chokepoint is a single file.
///
/// Registered as a singleton because IQsoRepository and IAwardsService are
/// scoped — a per-instance cache would never survive a request.
///
/// The snapshot is SHARED and must be treated as read-only. Callers that
/// mutate QSOs (e.g. the ADIF confirmation merge, which writes QSL fields
/// onto the objects it fetched) must keep using IQsoRepository.GetAllAsync
/// directly so they get their own instances.
/// </summary>
public class QsoSnapshotCache
{
    private readonly object _gate = new();
    private IReadOnlyList<Qso>? _snapshot;
    // Bumped by every Invalidate. A load that starts before a write and
    // finishes after it must not be published: the data it read predates the
    // write, so caching it would strand the stale list until the next write.
    private long _generation;

    /// <summary>
    /// The cached snapshot, loading it via <paramref name="load"/> on a miss.
    /// Treat the result as read-only — see the type remarks.
    /// </summary>
    public async Task<IReadOnlyList<Qso>> GetAsync(Func<Task<IEnumerable<Qso>>> load)
    {
        long startedAt;
        lock (_gate)
        {
            if (_snapshot != null) return _snapshot;
            startedAt = _generation;
        }

        // Loaded outside the lock so a slow read can't block invalidation.
        // A concurrent miss may load twice; both results are equivalent, and
        // that is cheaper than holding the lock across the whole fetch.
        var loaded = (await load()).ToList();

        lock (_gate)
        {
            // Publish only if no write landed while we were loading. When one
            // did, this caller still gets its (pre-write) list — it is no more
            // stale than any read that raced the write — but the cache stays
            // empty so the next read sees the write.
            if (_generation != startedAt) return loaded;
            return _snapshot ??= loaded;
        }
    }

    /// <summary>Drop the snapshot; the next read reloads from the database.</summary>
    public void Invalidate()
    {
        lock (_gate)
        {
            _snapshot = null;
            _generation++;
        }
    }
}
