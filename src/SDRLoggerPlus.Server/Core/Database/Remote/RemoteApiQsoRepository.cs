using System.Net.Http.Json;
using LiteDB;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Core.Database.Remote;

/// <summary>
/// <see cref="IQsoRepository"/> that lives on a FIELD station and reads/writes the shared log on a
/// remote HOST over HTTP+token (the mirror of the host's <c>DataSyncController</c>). This is what makes
/// three laptops — each running its own full app + own radio — all log into, and dupe-check against, one
/// shared log (S1 of the multi-op plan).
///
/// Role split (deliberate): the OPERATIONAL surface (create / read / dupe / search / stats) proxies to the
/// host. The QRZ/LoTW/QSL SYNC surface is HOST-ONLY — the host owns uploads for the shared log, so a client
/// never runs them (three clients each uploading would triple-post). Those methods return empty/no-op here.
///
/// Resilience (offline outbox / local cache) is S3; for S1 a transport failure surfaces as an exception.
/// </summary>
public sealed class RemoteApiQsoRepository : IQsoRepository
{
    private readonly HttpClient _http; // pre-configured: BaseAddress = host, Authorization: Bearer <token>
    private readonly ILogger<RemoteApiQsoRepository> _log;

    public RemoteApiQsoRepository(HttpClient http, ILogger<RemoteApiQsoRepository> log)
    {
        _http = http;
        _log = log;
    }

    private sealed record QsoPage(List<Qso> Items, int TotalCount);

    // -- operational surface: proxied to the host -----------------------------------------------

    public async Task<Qso> CreateAsync(Qso qso)
    {
        // Mint the origin id here if the caller didn't — this is the idempotency/dedup key the outbox
        // (S3) and multi-USB merge depend on, and the host's repo respects a provided id (S0.5).
        if (string.IsNullOrEmpty(qso.Id)) qso.Id = ObjectId.NewObjectId().ToString();
        var res = await _http.PostAsJsonAsync("api/data/qsos", qso);
        res.EnsureSuccessStatusCode();
        return (await res.Content.ReadFromJsonAsync<Qso>())!;
    }

    public async Task<IEnumerable<Qso>> CreateBulkAsync(IEnumerable<Qso> qsos)
    {
        var list = qsos.ToList();
        foreach (var q in list)
            if (string.IsNullOrEmpty(q.Id)) q.Id = ObjectId.NewObjectId().ToString();
        var res = await _http.PostAsJsonAsync("api/data/qsos/bulk", list);
        res.EnsureSuccessStatusCode();
        return (await res.Content.ReadFromJsonAsync<List<Qso>>()) ?? new();
    }

    public async Task<Qso?> GetByIdAsync(string id)
    {
        var res = await _http.GetAsync($"api/data/qsos/{Uri.EscapeDataString(id)}");
        if (res.StatusCode == System.Net.HttpStatusCode.NotFound) return null;
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<Qso>();
    }

    public async Task<IEnumerable<Qso>> GetRecentAsync(int limit = 100) =>
        (await _http.GetFromJsonAsync<List<Qso>>($"api/data/qsos/recent?limit={limit}")) ?? new();

    public async Task<IEnumerable<Qso>> GetAllAsync() =>
        (await _http.GetFromJsonAsync<List<Qso>>("api/data/qsos/all")) ?? new();

    public async Task<bool> UpdateAsync(string id, Qso qso)
    {
        var res = await _http.PutAsJsonAsync($"api/data/qsos/{Uri.EscapeDataString(id)}", qso);
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<bool>();
    }

    public async Task<bool> DeleteAsync(string id)
    {
        var res = await _http.DeleteAsync($"api/data/qsos/{Uri.EscapeDataString(id)}");
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<bool>();
    }

    public async Task<int> DeleteManyAsync(IEnumerable<string> ids)
    {
        var res = await _http.PostAsJsonAsync("api/data/qsos/delete-many", ids.ToList());
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<int>();
    }

    public async Task<(IEnumerable<Qso> Items, int TotalCount)> SearchAsync(QsoSearchRequest criteria)
    {
        var res = await _http.PostAsJsonAsync("api/data/qsos/search", criteria);
        res.EnsureSuccessStatusCode();
        var page = await res.Content.ReadFromJsonAsync<QsoPage>() ?? new(new(), 0);
        return (page.Items, page.TotalCount);
    }

    public Task<QsoStatistics> GetStatisticsAsync(string? myCall = null) =>
        _http.GetFromJsonAsync<QsoStatistics>(
            $"api/data/qsos/statistics{(string.IsNullOrEmpty(myCall) ? "" : $"?myCall={Uri.EscapeDataString(myCall)}")}")!;

    public Task<int> GetCountAsync() => _http.GetFromJsonAsync<int>("api/data/qsos/count");

    public async Task<bool> ExistsAsync(string callsign, DateTime qsoDate, string timeOn, string band, string mode)
    {
        var res = await _http.PostAsJsonAsync("api/data/qsos/exists",
            new { Callsign = callsign, QsoDate = qsoDate, TimeOn = timeOn, Band = band, Mode = mode });
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<bool>();
    }

    public async Task<IEnumerable<Qso>> GetByIdsAsync(IEnumerable<string> ids)
    {
        var res = await _http.PostAsJsonAsync("api/data/qsos/by-ids", ids.ToList());
        res.EnsureSuccessStatusCode();
        return (await res.Content.ReadFromJsonAsync<List<Qso>>()) ?? new();
    }

    public async Task<List<Qso>> GetByContestSessionAsync(string sessionId) =>
        (await _http.GetFromJsonAsync<List<Qso>>($"api/data/qsos/contest-session/{Uri.EscapeDataString(sessionId)}")) ?? new();

    public async Task<List<string>> GetDistinctCallsignsAsync() =>
        (await _http.GetFromJsonAsync<List<string>>("api/data/qsos/distinct-callsigns")) ?? new();

    public async Task<Qso?> GetMostRecentByCallsignAsync(string callsign)
    {
        var res = await _http.GetAsync($"api/data/qsos/recent-by-callsign?callsign={Uri.EscapeDataString(callsign)}");
        if (res.StatusCode == System.Net.HttpStatusCode.NoContent) return null;
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<Qso>();
    }

    public async Task<Qso?> FindRecentDuplicateAsync(string callsign, string band, string mode, DateTime createdSinceUtc)
    {
        var url = $"api/data/qsos/find-duplicate?callsign={Uri.EscapeDataString(callsign)}"
                + $"&band={Uri.EscapeDataString(band)}&mode={Uri.EscapeDataString(mode)}"
                + $"&createdSinceUtc={Uri.EscapeDataString(createdSinceUtc.ToString("o"))}";
        var res = await _http.GetAsync(url);
        if (res.StatusCode == System.Net.HttpStatusCode.NoContent) return null;
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<Qso>();
    }

    // -- host-only sync surface: a client never uploads the shared log -------------------------------
    // The HOST owns QRZ/LoTW/QSL sync for the shared log. A field client running these would triple-post.

    public Task<IEnumerable<Qso>> GetUnsyncedToQrzAsync() => Task.FromResult(Enumerable.Empty<Qso>());
    public Task<bool> UpdateQrzSyncStatusAsync(string id, string qrzLogId) => Task.FromResult(false);
    public Task<int> GetPendingSyncCountAsync() => Task.FromResult(0);
    public Task<int> MarkAllQrzSyncedAsync() => Task.FromResult(0);
    public Task<bool> UpdateQslSyncAsync(string id, QslSyncLedger ledger) => Task.FromResult(false);
    public Task<IEnumerable<Qso>> GetQslFailuresAsync(string service) => Task.FromResult(Enumerable.Empty<Qso>());

    // Destructive whole-log wipe is intentionally NOT proxied — it must be done on the host directly,
    // never triggered remotely from a field client.
    public Task<long> DeleteAllAsync() =>
        throw new NotSupportedException("Deleting the entire shared log must be done on the host, not a connected client.");
}
