using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Host endpoint for multi-op shared serial numbers (S5). A field client asks the host for its next
/// contest serial so the whole station draws from one atomic sequence (no collisions between a fast
/// running op and a slow S&amp;P op). The host applies the contest's own <see cref="SerialMode"/> —
/// authoritative, so a client can't accidentally draw from the wrong (per-band vs all-band) counter.
/// Under <c>/api/data</c>, so it's token-gated on a shared host.
/// </summary>
[ApiController]
[Route("api/data/serial")]
public class SerialController : ControllerBase
{
    private readonly SharedSerialAllocator _allocator;
    private readonly ContestDefinitionService _defs;

    public SerialController(SharedSerialAllocator allocator, ContestDefinitionService defs)
    {
        _allocator = allocator;
        _defs = defs;
    }

    public record SerialRequest(string ContestId, string? Band);

    [HttpPost("next")]
    public ActionResult Next([FromBody] SerialRequest req)
    {
        var mode = _defs.Get(req.ContestId)?.Serial ?? SerialMode.None;
        var serial = _allocator.Next(req.ContestId, mode, req.Band ?? "");
        return Ok(new { serial, formatted = ContestSerials.Format(serial) });
    }

    [HttpPost("peek")]
    public ActionResult Peek([FromBody] SerialRequest req)
    {
        var mode = _defs.Get(req.ContestId)?.Serial ?? SerialMode.None;
        var serial = _allocator.Peek(req.ContestId, mode, req.Band ?? "");
        return Ok(new { serial, formatted = ContestSerials.Format(serial) });
    }
}
