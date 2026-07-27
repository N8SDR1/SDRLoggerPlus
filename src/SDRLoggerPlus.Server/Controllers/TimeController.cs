using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Core.Time;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Time authority for multi-op (S4). <c>GET /api/time</c> returns this host's UTC — a field client
/// polls it (via <c>HostTimeSyncService</c>) to measure its own clock offset so every station agrees
/// even with no internet. <c>GET /api/time/offset</c> reports THIS machine's measured offset for the
/// UI's sync indicator (on a host it reports "I am the authority").
/// </summary>
[ApiController]
[Route("api/time")]
public class TimeController : ControllerBase
{
    private readonly HostTimeOffset _offset;
    public TimeController(HostTimeOffset offset) => _offset = offset;

    [HttpGet]
    public ActionResult Now() => Ok(new { utc = DateTime.UtcNow });

    [HttpGet("offset")]
    public ActionResult OffsetState() => Ok(new
    {
        isHost = _offset.IsHost,
        offsetMs = _offset.Offset.TotalMilliseconds,
        lastSyncUtc = _offset.LastSyncUtc,
    });
}
