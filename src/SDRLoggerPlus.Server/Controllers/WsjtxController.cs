using Microsoft.AspNetCore.Mvc;
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
}
