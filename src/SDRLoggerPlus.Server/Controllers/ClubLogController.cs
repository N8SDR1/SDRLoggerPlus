using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

public record ClubLogTestRequest(string? Email, string? Password, string? Callsign, string? ApiKey);

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
    public async Task<ActionResult> Test([FromBody] ClubLogTestRequest? draft, CancellationToken ct)
    {
        // The form values, when the client sends them. A body-less call keeps
        // testing whatever was last saved.
        ClubLogSettings? probe = draft is null ? null : new ClubLogSettings
        {
            Email = draft.Email,
            Password = draft.Password,
            Callsign = draft.Callsign,
            ApiKey = draft.ApiKey,
        };
        var (ok, message) = await _clubLog.TestAsync(probe, ct);
        return Ok(new { success = ok, message, blocked = _clubLog.IsBlocked });
    }
}
