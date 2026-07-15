using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Services.Wsjtx;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
public class WsjtxController : ControllerBase
{
    private readonly WsjtxService _wsjtxService;

    public WsjtxController(WsjtxService wsjtxService) => _wsjtxService = wsjtxService;

    [HttpGet("status")]
    public ActionResult<IReadOnlyList<WsjtxStatus>> GetStatus() => Ok(_wsjtxService.GetStatuses());

    /// <summary>Recent decodes for the Decodes panel to backfill on open.</summary>
    [HttpGet("decodes")]
    public ActionResult<IReadOnlyList<WsjtxDecodeEvent>> GetDecodes() => Ok(_wsjtxService.GetRecentDecodes());
}
