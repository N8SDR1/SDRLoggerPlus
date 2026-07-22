using LiteDB;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Core.Database.LiteDb;

public class LiteQsoRepository : IQsoRepository
{
    private readonly LiteDbContext _context;
    private readonly QsoSnapshotCache? _snapshots;

    public LiteQsoRepository(LiteDbContext context, QsoSnapshotCache? snapshots = null)
    {
        _context = context;
        _snapshots = snapshots;
    }

    /// <summary>
    /// Flush the write and drop the shared award snapshot. Every mutation goes
    /// through here so a new write path cannot persist without invalidating —
    /// a stale snapshot would show "not worked" for a country just logged.
    /// </summary>
    private void Commit()
    {
        _context.Database.Checkpoint();
        _snapshots?.Invalidate();
    }

    public Task<Qso?> GetByIdAsync(string id)
    {
        var qso = _context.Qsos.FindById(new BsonValue(id));
        return Task.FromResult<Qso?>(qso);
    }

    public Task<IEnumerable<Qso>> GetRecentAsync(int limit = 100)
    {
        var results = _context.Qsos.Query()
            .OrderByDescending(q => q.QsoDate)
            .Limit(limit)
            .ToList();

        return Task.FromResult<IEnumerable<Qso>>(results);
    }

    public Task<IEnumerable<Qso>> GetAllAsync()
    {
        var results = _context.Qsos.Query()
            .OrderByDescending(q => q.QsoDate)
            .ToList();

        return Task.FromResult<IEnumerable<Qso>>(results);
    }

    public Task<(IEnumerable<Qso> Items, int TotalCount)> SearchAsync(QsoSearchRequest criteria)
    {
        var query = _context.Qsos.Query();

        // Build filters using Where clauses
        if (!string.IsNullOrEmpty(criteria.Callsign))
        {
            var pattern = criteria.Callsign;
            query = query.Where(q => q.Callsign.Contains(pattern) ||
                q.Callsign.ToUpper().Contains(pattern.ToUpper()));
        }

        if (!string.IsNullOrEmpty(criteria.Name))
        {
            var namePattern = criteria.Name;
            query = query.Where(q => q.Name != null &&
                (q.Name.Contains(namePattern) ||
                 q.Name.ToUpper().Contains(namePattern.ToUpper())));
        }

        if (!string.IsNullOrEmpty(criteria.Band))
            query = query.Where(q => q.Band == criteria.Band);

        if (!string.IsNullOrEmpty(criteria.Mode))
            query = query.Where(q => q.Mode == criteria.Mode);

        if (criteria.FromDate.HasValue)
        {
            var fromDate = criteria.FromDate.Value;
            query = query.Where(q => q.QsoDate >= fromDate);
        }

        if (criteria.ToDate.HasValue)
        {
            var toDate = criteria.ToDate.Value.AddDays(1);
            query = query.Where(q => q.QsoDate <= toDate);
        }

        if (criteria.Dxcc.HasValue)
        {
            var dxcc = criteria.Dxcc.Value;
            query = query.Where(q => q.Dxcc == dxcc);
        }

        // Get total count by executing the query
        var allMatches = query.ToList();
        var totalCount = allMatches.Count;

        // Apply sorting, skip, and limit in-memory
        var items = allMatches
            .OrderByDescending(q => q.QsoDate)
            .ThenByDescending(q => q.TimeOn)
            .Skip(criteria.Skip)
            .Take(criteria.Limit)
            .ToList();

        return Task.FromResult<(IEnumerable<Qso> Items, int TotalCount)>((items, totalCount));
    }

    public Task<Qso> CreateAsync(Qso qso)
    {
        qso.CreatedAt = DateTime.UtcNow;
        qso.UpdatedAt = DateTime.UtcNow;

        // Generate an ID if not set
        if (string.IsNullOrEmpty(qso.Id))
        {
            qso.Id = ObjectId.NewObjectId().ToString();
        }

        _context.Qsos.Insert(qso);
        Commit();
        return Task.FromResult(qso);
    }

    public Task<IEnumerable<Qso>> CreateBulkAsync(IEnumerable<Qso> qsos)
    {
        var qsoList = qsos.ToList();
        var now = DateTime.UtcNow;

        foreach (var qso in qsoList)
        {
            qso.CreatedAt = now;
            qso.UpdatedAt = now;

            // Generate an ID if not set
            if (string.IsNullOrEmpty(qso.Id))
            {
                qso.Id = ObjectId.NewObjectId().ToString();
            }
        }

        // Bulk insert all QSOs
        _context.Qsos.InsertBulk(qsoList);

        // Single checkpoint after all inserts - this is the key optimization
        Commit();

        return Task.FromResult<IEnumerable<Qso>>(qsoList);
    }

    public Task<bool> UpdateAsync(string id, Qso qso)
    {
        qso.UpdatedAt = DateTime.UtcNow;

        // If QSO was previously synced, mark as Modified (like QLog's trigger)
        if (qso.QrzSyncStatus == SyncStatus.Synced)
        {
            qso.QrzSyncStatus = SyncStatus.Modified;
        }

        qso.Id = id;
        var success = _context.Qsos.Update(qso);
        Commit();
        return Task.FromResult(success);
    }

    /// <summary>
    /// Writes only the QSL ledger.
    ///
    /// Deliberately NOT routed through UpdateAsync: that method bumps
    /// UpdatedAt and flips a Synced QSO to Modified, which would queue a QRZ
    /// re-upload every time an unrelated service reported back. Recording that
    /// eQSL accepted a QSO must not tell QRZ the QSO changed — it didn't.
    /// </summary>
    public Task<bool> UpdateQslSyncAsync(string id, QslSyncLedger ledger)
    {
        var qso = _context.Qsos.FindById(new BsonValue(id));
        if (qso == null) return Task.FromResult(false);

        qso.QslSync = ledger;
        var success = _context.Qsos.Update(qso);
        _context.Database.Checkpoint();
        return Task.FromResult(success);
    }

    public Task<IEnumerable<Qso>> GetQslFailuresAsync(string service)
    {
        // Filtered in memory: the ledger is a nested document and LiteDB's
        // expression support for nested optional paths is fragile enough that
        // a wrong predicate would silently return nothing — the worst failure
        // mode for a "what still needs sending?" query. This runs on demand,
        // not on a hot path.
        var results = _context.Qsos.FindAll()
            .Where(q => q.QslSync?.For(service)?.IsRetryable == true)
            .OrderByDescending(q => q.QsoDate)
            .ThenByDescending(q => q.TimeOn)
            .ToList();

        return Task.FromResult<IEnumerable<Qso>>(results);
    }

    public Task<bool> DeleteAsync(string id)
    {
        var success = _context.Qsos.Delete(new BsonValue(id));
        Commit();
        return Task.FromResult(success);
    }

    public Task<int> DeleteManyAsync(IEnumerable<string> ids)
    {
        var deleted = 0;
        foreach (var id in ids.Distinct(StringComparer.Ordinal))
        {
            if (_context.Qsos.Delete(new BsonValue(id))) deleted++;
        }

        // One checkpoint for the whole batch. Checkpointing per row would turn
        // a 50-QSO delete into 50 flushes of the write-ahead log.
        if (deleted > 0) _context.Database.Checkpoint();
        return Task.FromResult(deleted);
    }

    public Task<QsoStatistics> GetStatisticsAsync()
    {
        var all = _context.Qsos.FindAll().ToList();

        // "Today" = the operator's LOCAL calendar day. In-app QSOs (manual, FT8
        // auto-log, SAT) store a local QsoDate, so the old DateTime.UtcNow.Date
        // comparison under-counted evening QSOs once UTC had already rolled past
        // midnight — a 21:33 local QSO on the 17th read as "before" 00:00 UTC on
        // the 18th and dropped to 0. Normalise every QsoDate to local time (a
        // no-op for the local rows, a proper conversion for UTC-imported rows)
        // and match today's local date.
        var today = DateTime.Now.Date;
        var qsosToday = all.Count(q => q.QsoDate.ToLocalTime().Date == today);

        // Qso stores DXCC / Country / Grid on the nested StationInfo (v2
        // schema) and ALSO carries legacy top-level columns for older rows —
        // prefer the nested value with a fallback to the legacy field.
        //
        // "Countries" is counted by distinct country NAME rather than DXCC
        // entity id: many ADIF exports (including old SDRLogger+ v1) omit
        // the DXCC field, but populate COUNTRY (or we back-fill it via
        // CtyService callsign lookup on import). Counting by name gives the
        // operator the number they actually think of as "countries worked"
        // — which is what the label reads — and stays non-zero on imports
        // that only carry the country name.
        string? CountryOf(Qso q) => !string.IsNullOrEmpty(q.Station?.Country) ? q.Station!.Country : q.Country;
        string? GridOf(Qso q) => !string.IsNullOrEmpty(q.Station?.Grid) ? q.Station!.Grid : q.Grid;

        var stats = new QsoStatistics(
            TotalQsos: all.Count,
            UniqueCallsigns: all.Select(q => q.Callsign).Distinct().Count(),
            UniqueCountries: all.Select(CountryOf)
                .Where(c => !string.IsNullOrEmpty(c))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Count(),
            UniqueGrids: all.Select(GridOf).Where(g => !string.IsNullOrEmpty(g)).Distinct().Count(),
            QsosToday: qsosToday,
            // Group by case-normalised band / mode. ADIF exports differ in
            // case (v1 SDRLogger+ wrote "40M", QRZ writes "40m") and a
            // case-sensitive GroupBy splits the same band across two
            // buckets. Bands are normalised to lowercase because the app-
            // wide convention ("40m", "70cm") is lowercase; modes to
            // uppercase because that matches the app's mode-picker
            // ("USB", "FT8"), so the returned dictionary keys line up
            // with the UI without extra plumbing.
            QsosByBand: all.GroupBy(q => (q.Band ?? "Unknown").Trim().ToLowerInvariant())
                .ToDictionary(g => g.Key, g => g.Count()),
            QsosByMode: all.GroupBy(q => (q.Mode ?? "Unknown").Trim().ToUpperInvariant())
                .ToDictionary(g => g.Key, g => g.Count())
        );

        return Task.FromResult(stats);
    }

    public Task<int> GetCountAsync()
    {
        var count = _context.Qsos.Count();
        return Task.FromResult(count);
    }

    public Task<bool> ExistsAsync(string callsign, DateTime qsoDate, string timeOn, string band, string mode)
    {
        var upperCallsign = callsign.ToUpperInvariant();
        var date = qsoDate.Date;

        var exists = _context.Qsos.Query()
            .Where(q => q.Callsign == upperCallsign
                && q.QsoDate == date
                && q.TimeOn == timeOn
                && q.Band == band
                && q.Mode == mode)
            .Exists();

        return Task.FromResult(exists);
    }

    public Task<IEnumerable<Qso>> GetByIdsAsync(IEnumerable<string> ids)
    {
        var idList = ids.ToList();
        var results = _context.Qsos.Find(q => idList.Contains(q.Id)).ToList();
        return Task.FromResult<IEnumerable<Qso>>(results);
    }

    public Task<List<Qso>> GetByContestSessionAsync(string sessionId)
    {
        // Oldest-first so the scoring engine replays the log in operating order.
        var results = _context.Qsos
            .Find(q => q.Contest != null && q.Contest.SessionId == sessionId)
            .OrderBy(q => q.QsoDate).ThenBy(q => q.TimeOn).ThenBy(q => q.CreatedAt)
            .ToList();
        return Task.FromResult(results);
    }

    public Task<List<string>> GetDistinctCallsignsAsync()
    {
        var calls = _context.Qsos.Query()
            .Select(q => q.Callsign)
            .ToList()
            .Where(c => !string.IsNullOrWhiteSpace(c))
            .Select(c => c.ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(c => c, StringComparer.Ordinal)
            .ToList();
        return Task.FromResult(calls);
    }

    public Task<Qso?> GetMostRecentByCallsignAsync(string callsign)
    {
        var call = callsign.Trim().ToUpperInvariant();
        var qso = _context.Qsos
            .Find(q => q.Callsign == call)
            .OrderByDescending(q => q.QsoDate).ThenByDescending(q => q.TimeOn)
            .FirstOrDefault();
        return Task.FromResult<Qso?>(qso);
    }

    public Task<Qso?> FindRecentDuplicateAsync(string callsign, string band, string mode, DateTime createdSinceUtc)
    {
        var call = callsign.Trim().ToUpperInvariant();
        // Callsign + window go to LiteDB (CreatedAt is stored UTC, and LiteDB
        // compares the UTC BSON values, so the read-side local-Kind conversion
        // doesn't skew the window). Band/mode are compared in memory so the
        // match is case-insensitive — the candidate set is tiny.
        var qso = _context.Qsos
            .Find(q => q.Callsign == call && q.CreatedAt >= createdSinceUtc)
            .Where(q => string.Equals(q.Band, band, StringComparison.OrdinalIgnoreCase)
                     && string.Equals(q.Mode, mode, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(q => q.CreatedAt)
            .FirstOrDefault();
        return Task.FromResult<Qso?>(qso);
    }

    public Task<IEnumerable<Qso>> GetUnsyncedToQrzAsync()
    {
        var results = _context.Qsos.Find(q =>
            q.QrzSyncStatus == SyncStatus.NotSynced ||
            q.QrzSyncStatus == SyncStatus.Modified)
            .OrderByDescending(q => q.QsoDate)
            .ThenByDescending(q => q.TimeOn)
            .ToList();

        return Task.FromResult<IEnumerable<Qso>>(results);
    }

    public Task<int> GetPendingSyncCountAsync()
    {
        var count = _context.Qsos.Count(q =>
            q.QrzSyncStatus == SyncStatus.NotSynced ||
            q.QrzSyncStatus == SyncStatus.Modified);

        return Task.FromResult(count);
    }

    public Task<bool> UpdateQrzSyncStatusAsync(string id, string qrzLogId)
    {
        var qso = _context.Qsos.FindById(new BsonValue(id));
        if (qso == null) return Task.FromResult(false);

        qso.QrzLogId = qrzLogId;
        qso.QrzSyncedAt = DateTime.UtcNow;
        qso.QrzSyncStatus = SyncStatus.Synced;
        qso.UpdatedAt = DateTime.UtcNow;

        var success = _context.Qsos.Update(qso);
        Commit();
        return Task.FromResult(success);
    }

    public Task<long> DeleteAllAsync()
    {
        var count = _context.Qsos.DeleteAll();
        Commit();
        return Task.FromResult((long)count);
    }
}
