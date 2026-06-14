using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/clublog")]
[Produces("application/json")]
public class ClubLogController : ControllerBase
{
    private readonly ClubLogService _clubLog;

    public ClubLogController(ClubLogService clubLog)
    {
        _clubLog = clubLog;
    }

    /// <summary>
    /// Verify Club Log credentials (read-only check; a success also re-enables
    /// uploads after an authentication block).
    /// </summary>
    [HttpPost("test")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public async Task<ActionResult> Test(CancellationToken ct)
    {
        var (ok, message) = await _clubLog.TestAsync(ct);
        return Ok(new { success = ok, message, blocked = _clubLog.IsBlocked });
    }
}
