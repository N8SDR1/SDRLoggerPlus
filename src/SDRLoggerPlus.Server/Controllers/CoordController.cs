using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Multi-op coordination on the shared event bus (S-COORD): radio-presence reporting (for the "who's on
/// what" board + the RF-collision/desense warning) and operator-to-operator chat.
///
/// Presence/messages must originate at the HOST so it can broadcast to the whole group. The frontend
/// only ever talks to its own backend, so on a field CLIENT (RemoteHost) this controller FORWARDS to the
/// host; on the host it tracks + broadcasts locally. Either way the change fans out to every station's
/// UI (each client sees it via its HostBridge relay). Under <c>/api/data</c>, token-gated.
/// </summary>
[ApiController]
[Route("api/data/coord")]
public class CoordController : ControllerBase
{
    private readonly StationPresenceTracker _tracker;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;
    private readonly IUserConfigService _config;
    private readonly IHttpClientFactory _httpFactory;

    public CoordController(StationPresenceTracker tracker, IHubContext<LogHub, ILogHubClient> hub,
        IUserConfigService config, IHttpClientFactory httpFactory)
    {
        _tracker = tracker;
        _hub = hub;
        _config = config;
        _httpFactory = httpFactory;
    }

    public record PresenceRequest(string StationId, string? Operator, string? Band, string? Mode);
    public record MessageRequest(string StationId, string? Operator, string Text);

    [HttpPost("presence")]
    public async Task<ActionResult> ReportPresence([FromBody] PresenceRequest req)
    {
        var (isClient, client) = await HostClientAsync();
        if (isClient)
            return Ok(await ForwardJsonAsync<List<StationPresenceEvent>>(client!, "presence", req) ?? new());

        var evt = new StationPresenceEvent(req.StationId, req.Operator, req.Band, req.Mode, DateTime.UtcNow);
        _tracker.Update(evt);
        await _hub.BroadcastPresence(evt);
        return Ok(_tracker.Current());
    }

    [HttpGet("presence")]
    public async Task<ActionResult> Board()
    {
        var (isClient, client) = await HostClientAsync();
        if (isClient)
        {
            var board = await client!.GetFromJsonAsync<List<StationPresenceEvent>>("api/data/coord/presence");
            return Ok(board ?? new());
        }
        return Ok(_tracker.Current());
    }

    [HttpPost("message")]
    public async Task<ActionResult> SendMessage([FromBody] MessageRequest req)
    {
        if (string.IsNullOrWhiteSpace(req.Text)) return BadRequest(new { error = "Message is empty." });

        var (isClient, client) = await HostClientAsync();
        if (isClient)
        {
            await client!.PostAsJsonAsync("api/data/coord/message", req);
            return Ok();
        }

        var evt = new OperatorMessageEvent(req.StationId, req.Operator, req.Text.Trim(), DateTime.UtcNow);
        await _hub.BroadcastOperatorMessage(evt);
        return Ok();
    }

    // -- client → host forwarding -----------------------------------------------------------------

    /// <summary>(true, configured client) on a field client; (false, null) on the host / standalone.</summary>
    private async Task<(bool IsClient, HttpClient? Client)> HostClientAsync()
    {
        var cfg = await _config.GetConfigAsync();
        if (cfg.Provider != DatabaseProvider.RemoteHost || string.IsNullOrWhiteSpace(cfg.HostUrl))
            return (false, null);

        var client = _httpFactory.CreateClient();
        client.BaseAddress = new Uri(cfg.HostUrl.TrimEnd('/') + "/");
        client.Timeout = TimeSpan.FromSeconds(10);
        if (!string.IsNullOrWhiteSpace(cfg.HostToken))
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", cfg.HostToken);
        return (true, client);
    }

    private static async Task<T?> ForwardJsonAsync<T>(HttpClient client, string path, object body)
    {
        var res = await client.PostAsJsonAsync($"api/data/coord/{path}", body);
        res.EnsureSuccessStatusCode();
        return await res.Content.ReadFromJsonAsync<T>();
    }
}
