using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Core.Time;
using SDRLoggerPlus.Server.Services;

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
    private readonly NtpService _ntp;
    public TimeController(HostTimeOffset offset, NtpService ntp)
    {
        _offset = offset;
        _ntp = ntp;
    }

    [HttpGet]
    public ActionResult Now() => Ok(new { utc = DateTime.UtcNow });

    [HttpGet("offset")]
    public ActionResult OffsetState() => Ok(new
    {
        isHost = _offset.IsHost,
        offsetMs = _offset.Offset.TotalMilliseconds,
        lastSyncUtc = _offset.LastSyncUtc,
    });

    /// <summary>Query public NTP and report how far THIS PC's clock is off (read-only).</summary>
    [HttpGet("ntp")]
    public async Task<ActionResult> Ntp([FromQuery] string? server, CancellationToken ct)
        => Ok(await _ntp.QueryAsync(server, ct));

    /// <summary>
    /// Sync the PC clock to NTP. Queries NTP, then (Windows) sets the system clock via an elevated
    /// helper — a UAC prompt the operator must approve. Returns the pre-sync offset for the UI.
    /// </summary>
    [HttpPost("ntp/resync")]
    public async Task<ActionResult> NtpResync([FromQuery] string? server, CancellationToken ct)
    {
        var q = await _ntp.QueryAsync(server, ct);
        if (!q.Reachable)
            return Ok(new { ok = false, detail = $"Couldn't reach NTP: {q.Error}", offsetMs = 0.0, server = q.Server });

        var r = _ntp.SetSystemClock(q.ServerUtc);
        return Ok(new { ok = r.Ok, detail = r.Detail, offsetMs = q.OffsetMs, server = q.Server });
    }
}
