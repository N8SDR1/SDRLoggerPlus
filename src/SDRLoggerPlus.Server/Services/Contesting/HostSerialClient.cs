using System.Net.Http.Json;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// How a client draws contest serials. In a normal (single-machine or host) install the serial comes
/// from the LOCAL session counter. In multi-op RemoteHost mode it must come from the HOST's atomic
/// allocator (SerialController) so a fast running op and a slow S&amp;P op never issue the same number.
/// One impl is always registered by <c>DbServiceRegistration</c> so <see cref="IsRemote"/> tells the
/// caller which world it's in.
///
/// NOTE (S5b): this delivers the client→host routing + a preview via Peek. The HARD-LOCKED reservation
/// (reserve-on-show / commit-on-log / release-on-clear, so a number on your screen can't be stolen) is
/// still outstanding — see GitHub #52 and docs/design/networked-shared-database-execution-plan.md.
/// </summary>
public interface IHostSerialClient
{
    bool IsRemote { get; }
    Task<int> NextAsync(string contestId, string? band, CancellationToken ct = default);
    Task<int> PeekAsync(string contestId, string? band, CancellationToken ct = default);
}

/// <summary>Local install / host: serials come from the local session counter, never from here.</summary>
public sealed class LocalHostSerialClient : IHostSerialClient
{
    public bool IsRemote => false;
    public Task<int> NextAsync(string contestId, string? band, CancellationToken ct = default) => Task.FromResult(0);
    public Task<int> PeekAsync(string contestId, string? band, CancellationToken ct = default) => Task.FromResult(0);
}

/// <summary>Multi-op client: asks the host's SerialController for the shared serial (token-gated HTTP).</summary>
public sealed class RemoteHostSerialClient : IHostSerialClient
{
    private readonly HttpClient _http; // BaseAddress = host, Authorization: Bearer <token>
    public RemoteHostSerialClient(HttpClient http) => _http = http;

    public bool IsRemote => true;

    private sealed record SerialResponse(int Serial, string? Formatted);

    public Task<int> NextAsync(string contestId, string? band, CancellationToken ct = default)
        => PostAsync("api/data/serial/next", contestId, band, ct);

    public Task<int> PeekAsync(string contestId, string? band, CancellationToken ct = default)
        => PostAsync("api/data/serial/peek", contestId, band, ct);

    private async Task<int> PostAsync(string path, string contestId, string? band, CancellationToken ct)
    {
        var resp = await _http.PostAsJsonAsync(path, new { contestId, band }, ct);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadFromJsonAsync<SerialResponse>(ct);
        return body?.Serial ?? 0;
    }
}
