using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Multi-op coordination on the shared event bus (S-COORD): radio-presence reporting (for the "who's on
/// what" board + the RF-collision/desense warning) and operator-to-operator chat. A station POSTs its
/// band/mode or a message to the HOST, which broadcasts to every station (its own UI + each client via
/// its HostBridge). Under <c>/api/data</c>, token-gated on a shared host.
/// </summary>
[ApiController]
[Route("api/data/coord")]
public class CoordController : ControllerBase
{
    private readonly StationPresenceTracker _tracker;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;

    public CoordController(StationPresenceTracker tracker, IHubContext<LogHub, ILogHubClient> hub)
    {
        _tracker = tracker;
        _hub = hub;
    }

    public record PresenceRequest(string StationId, string? Operator, string? Band, string? Mode);
    public record MessageRequest(string StationId, string? Operator, string Text);

    /// <summary>Report this station's band/mode; returns the current board and fans the change out.</summary>
    [HttpPost("presence")]
    public async Task<ActionResult> ReportPresence([FromBody] PresenceRequest req)
    {
        var evt = new StationPresenceEvent(req.StationId, req.Operator, req.Band, req.Mode, DateTime.UtcNow);
        _tracker.Update(evt);
        await _hub.BroadcastPresence(evt);
        return Ok(_tracker.Current());
    }

    /// <summary>The current "who's on what" board (for a station that just joined).</summary>
    [HttpGet("presence")]
    public ActionResult Board() => Ok(_tracker.Current());

    [HttpPost("message")]
    public async Task<ActionResult> SendMessage([FromBody] MessageRequest req)
    {
        if (string.IsNullOrWhiteSpace(req.Text)) return BadRequest(new { error = "Message is empty." });
        var evt = new OperatorMessageEvent(req.StationId, req.Operator, req.Text.Trim(), DateTime.UtcNow);
        await _hub.BroadcastOperatorMessage(evt);
        return Ok();
    }
}
