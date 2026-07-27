using System.Net.Http.Json;
using System.Text.Json;
using LiteDB;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;

namespace SDRLoggerPlus.Server.Core.Database.Remote;

/// <summary>
/// <see cref="IQsoRepository"/> that lives on a FIELD station and reads/writes the shared log on a
/// remote HOST over HTTP+token (the mirror of the host's <c>DataSyncController</c>). Three laptops —
/// each running its own full app + own radio — all log into, and dupe-check against, one shared log.
///
/// Resilience (S3): a durable <see cref="RemoteWriteOutbox"/> so a host/Wi-Fi blip never loses a QSO,
/// and a local LiteDB CACHE (write-through on create, warmed on recent-log reads) that the read paths
/// fall back to when the host is unreachable — so dupe-check and the recent log keep working offline.
///
/// Role split: the QRZ/LoTW/QSL SYNC surface is HOST-ONLY (the host owns uploads for the shared log —
/// three clients each uploading would triple-post), so those methods are no-ops here.
/// </summary>
public sealed class RemoteApiQsoRepository : IQsoRepository
{
    private readonly HttpClient _http; // pre-configured: BaseAddress = host, Authorization: Bearer <token>
    private readonly RemoteWriteOutbox _outbox;
    private readonly LiteQsoRepository _cache; // local mirror for offline reads (client's own LiteDB)
    private readonly ILogger<RemoteApiQsoRepository> _log;

    private static readonly JsonSerializerOptions _json = RemoteJson.Options;

    public RemoteApiQsoRepository(HttpClient http, RemoteWriteOutbox outbox,
        LiteDbContext localDb, ILogger<RemoteApiQsoRepository> log)
    {
        _http = http;
        _outbox = outbox;
        _cache = new LiteQsoRepository(localDb); // no snapshot cache — awards aren't computed on a client
        _log = log;
    }

    private sealed record QsoPage(List<Qso> Items, int TotalCount);

    private static bool IsTransport(Exception ex) => ex is HttpRequestException or TaskCanceledException;

    /// <summary>Try the host; on an unreachable host serve the local cache instead.</summary>
    private async Task<T> WithFallback<T>(Func<Task<T>> remote, Func<Task<T>> cache)
    {
        try { return await remote(); }
        catch (Exception ex) when (IsTransport(ex))
        {
            _log.LogDebug("Host unreachable — serving read from local cache ({Msg}).", ex.Message);
            return await cache();
        }
    }

    // -- writes: to the host (or outbox), write-through to the cache ------------------------------

    public async Task<Qso> CreateAsync(Qso qso)
    {
        // Mint the origin id here if the caller didn't — the idempotency/dedup key the outbox and the
        // multi-USB merge depend on; the host's repo (and the cache) respect a provided id (S0/S2).
        if (string.IsNullOrEmpty(qso.Id)) qso.Id = ObjectId.NewObjectId().ToString();

        HttpResponseMessage res;
        try
        {
            res = await _http.PostAsJsonAsync("api/data/qsos", qso, _json);
        }
        catch (Exception ex) when (IsTransport(ex))
        {
            // Host unreachable: queue for retry AND write-through to the cache so it's immediately
            // dupe-checkable offline. Returned as logged so the operator keeps working.
            _log.LogWarning("Host unreachable — QSO {Id} queued to the outbox for retry.", qso.Id);
            _outbox.Enqueue(qso);
            await _cache.CreateAsync(qso);
            return qso;
        }

        res.EnsureSuccessStatusCode();
        var created = (await res.Content.ReadFromJsonAsync<Qso>(_json))!;
        await _cache.CreateAsync(created); // write-through (idempotent on Id)
        return created;
    }

    public async Task<IEnumerable<Qso>> CreateBulkAsync(IEnumerable<Qso> qsos)
    {
        var list = qsos.ToList();
        foreach (var q in list)
            if (string.IsNullOrEmpty(q.Id)) q.Id = ObjectId.NewObjectId().ToString();
        var res = await _http.PostAsJsonAsync("api/data/qsos/bulk", list, _json);
        res.EnsureSuccessStatusCode();
        var created = (await res.Content.ReadFromJsonAsync<List<Qso>>(_json)) ?? new();
        await _cache.CreateBulkAsync(created); // write-through
        return created;
    }

    // -- reads: host first, local cache when the host is unreachable ----------------------------

    public Task<Qso?> GetByIdAsync(string id) => WithFallback(
        async () =>
        {
            var res = await _http.GetAsync($"api/data/qsos/{Uri.EscapeDataString(id)}");
            if (res.StatusCode == System.Net.HttpStatusCode.NotFound) return null;
            res.EnsureSuccessStatusCode();
            return await res.Content.ReadFromJsonAsync<Qso>(_json);
        },
        () => _cache.GetByIdAsync(id));

    public Task<IEnumerable<Qso>> GetRecentAsync(int limit = 100) => WithFallback(
        async () =>
        {
            var items = (await _http.GetFromJsonAsync<List<Qso>>($"api/data/qsos/recent?limit={limit}", _json)) ?? new();
            await _cache.CreateBulkAsync(items); // warm the cache with the recent shared log (idempotent)
            return (IEnumerable<Qso>)items;
        },
        () => _cache.GetRecentAsync(limit));

    public Task<IEnumerable<Qso>> GetAllAsync() => WithFallback(
        async () =>
        {
            var items = (await _http.GetFromJsonAsync<List<Qso>>("api/data/qsos/all", _json)) ?? new();
            await _cache.CreateBulkAsync(items);
            return (IEnumerable<Qso>)items;
        },
        () => _cache.GetAllAsync());

    public async Task<bool> UpdateAsync(string id, Qso qso)
    {
        var res = await _http.PutAsJsonAsync($"api/data/qsos/{Uri.EscapeDataString(id)}", qso, _json);
        res.EnsureSuccessStatusCode();
        var ok = await res.Content.ReadFromJsonAsync<bool>();
        if (ok) await _cache.UpdateAsync(id, qso);
        return ok;
    }

    public async Task<bool> DeleteAsync(string id)
    {
        var res = await _http.DeleteAsync($"api/data/qsos/{Uri.EscapeDataString(id)}");
        res.EnsureSuccessStatusCode();
        var ok = await res.Content.ReadFromJsonAsync<bool>();
        if (ok) await _cache.DeleteAsync(id);
        return ok;
    }

    public async Task<int> DeleteManyAsync(IEnumerable<string> ids)
    {
        var idList = ids.ToList();
        var res = await _http.PostAsJsonAsync("api/data/qsos/delete-many", idList);
        res.EnsureSuccessStatusCode();
        var n = await res.Content.ReadFromJsonAsync<int>();
        await _cache.DeleteManyAsync(idList);
        return n;
    }

    public Task<(IEnumerable<Qso> Items, int TotalCount)> SearchAsync(QsoSearchRequest criteria) => WithFallback(
        async () =>
        {
            var res = await _http.PostAsJsonAsync("api/data/qsos/search", criteria, _json);
            res.EnsureSuccessStatusCode();
            var page = await res.Content.ReadFromJsonAsync<QsoPage>(_json) ?? new(new(), 0);
            return ((IEnumerable<Qso>)page.Items, page.TotalCount);
        },
        () => _cache.SearchAsync(criteria));

    public Task<QsoStatistics> GetStatisticsAsync(string? myCall = null) => WithFallback(
        () => _http.GetFromJsonAsync<QsoStatistics>(
            $"api/data/qsos/statistics{(string.IsNullOrEmpty(myCall) ? "" : $"?myCall={Uri.EscapeDataString(myCall)}")}")!,
        () => _cache.GetStatisticsAsync(myCall));

    public Task<int> GetCountAsync() => WithFallback(
        () => _http.GetFromJsonAsync<int>("api/data/qsos/count"),
        () => _cache.GetCountAsync());

    public async Task<bool> ExistsAsync(string callsign, DateTime qsoDate, string timeOn, string band, string mode) =>
        await WithFallback(
            async () =>
            {
                var res = await _http.PostAsJsonAsync("api/data/qsos/exists",
                    new { Callsign = callsign, QsoDate = qsoDate, TimeOn = timeOn, Band = band, Mode = mode });
                res.EnsureSuccessStatusCode();
                return await res.Content.ReadFromJsonAsync<bool>();
            },
            () => _cache.ExistsAsync(callsign, qsoDate, timeOn, band, mode));

    public Task<IEnumerable<Qso>> GetByIdsAsync(IEnumerable<string> ids)
    {
        var idList = ids.ToList();
        return WithFallback(
            async () =>
            {
                var res = await _http.PostAsJsonAsync("api/data/qsos/by-ids", idList);
                res.EnsureSuccessStatusCode();
                return (IEnumerable<Qso>)((await res.Content.ReadFromJsonAsync<List<Qso>>(_json)) ?? new());
            },
            () => _cache.GetByIdsAsync(idList));
    }

    public Task<List<Qso>> GetByContestSessionAsync(string sessionId) => WithFallback(
        async () => (await _http.GetFromJsonAsync<List<Qso>>($"api/data/qsos/contest-session/{Uri.EscapeDataString(sessionId)}", _json)) ?? new(),
        () => _cache.GetByContestSessionAsync(sessionId));

    public Task<List<string>> GetDistinctCallsignsAsync() => WithFallback(
        async () => (await _http.GetFromJsonAsync<List<string>>("api/data/qsos/distinct-callsigns")) ?? new(),
        () => _cache.GetDistinctCallsignsAsync());

    public Task<Qso?> GetMostRecentByCallsignAsync(string callsign) => WithFallback(
        async () =>
        {
            var res = await _http.GetAsync($"api/data/qsos/recent-by-callsign?callsign={Uri.EscapeDataString(callsign)}");
            if (res.StatusCode == System.Net.HttpStatusCode.NoContent) return null;
            res.EnsureSuccessStatusCode();
            return await res.Content.ReadFromJsonAsync<Qso>(_json);
        },
        () => _cache.GetMostRecentByCallsignAsync(callsign));

    public Task<Qso?> FindRecentDuplicateAsync(string callsign, string band, string mode, DateTime createdSinceUtc) => WithFallback(
        async () =>
        {
            var url = $"api/data/qsos/find-duplicate?callsign={Uri.EscapeDataString(callsign)}"
                    + $"&band={Uri.EscapeDataString(band)}&mode={Uri.EscapeDataString(mode)}"
                    + $"&createdSinceUtc={Uri.EscapeDataString(createdSinceUtc.ToString("o"))}";
            var res = await _http.GetAsync(url);
            if (res.StatusCode == System.Net.HttpStatusCode.NoContent) return null;
            res.EnsureSuccessStatusCode();
            return await res.Content.ReadFromJsonAsync<Qso>(_json);
        },
        () => _cache.FindRecentDuplicateAsync(callsign, band, mode, createdSinceUtc));

    // -- host-only sync surface: a client never uploads the shared log -------------------------------

    public Task<IEnumerable<Qso>> GetUnsyncedToQrzAsync() => Task.FromResult(Enumerable.Empty<Qso>());
    public Task<bool> UpdateQrzSyncStatusAsync(string id, string qrzLogId) => Task.FromResult(false);
    public Task<int> GetPendingSyncCountAsync() => Task.FromResult(0);
    public Task<int> MarkAllQrzSyncedAsync() => Task.FromResult(0);
    public Task<bool> UpdateQslSyncAsync(string id, QslSyncLedger ledger) => Task.FromResult(false);
    public Task<IEnumerable<Qso>> GetQslFailuresAsync(string service) => Task.FromResult(Enumerable.Empty<Qso>());

    // Destructive whole-log wipe is intentionally NOT proxied — it must be done on the host directly.
    public Task<long> DeleteAllAsync() =>
        throw new NotSupportedException("Deleting the entire shared log must be done on the host, not a connected client.");
}
