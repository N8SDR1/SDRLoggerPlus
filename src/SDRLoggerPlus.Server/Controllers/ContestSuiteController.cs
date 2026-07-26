using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Contest suite endpoints: definitions (seed + user CRUD), sessions
/// (start/activate/stop), live state, dupe check, and contest QSO logging.
/// Route is /api/contest — distinct from /api/contests (the WA7BNM calendar).
/// </summary>
[ApiController]
[Route("api/contest")]
public class ContestSuiteController : ControllerBase
{
    private readonly ContestDefinitionService _definitions;
    private readonly ContestSessionService _sessions;
    private readonly ContestService _contest;

    public ContestSuiteController(
        ContestDefinitionService definitions,
        ContestSessionService sessions,
        ContestService contest)
    {
        _definitions = definitions;
        _sessions = sessions;
        _contest = contest;
    }

    // -- definitions ---------------------------------------------------------

    [HttpGet("definitions")]
    public ActionResult<IReadOnlyList<ContestDefinition>> GetDefinitions() => Ok(_definitions.GetAll());

    [HttpGet("definitions/{id}")]
    public ActionResult<ContestDefinition> GetDefinition(string id)
    {
        var def = _definitions.Get(id);
        return def == null ? NotFound() : Ok(def);
    }

    [HttpPost("definitions")]
    public ActionResult<ContestDefinition> SaveDefinition([FromBody] ContestDefinition def)
    {
        try
        {
            return Ok(_definitions.Save(def));
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("definitions/{id}/clone")]
    public ActionResult<ContestDefinition> CloneDefinition(string id, [FromQuery] string newName)
    {
        try
        {
            return Ok(_definitions.CloneAsDraft(id, newName));
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpDelete("definitions/{id}")]
    public IActionResult DeleteDefinition(string id)
    {
        try
        {
            _definitions.Delete(id);
            return NoContent();
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    // -- sessions ------------------------------------------------------------

    [HttpGet("sessions")]
    public async Task<ActionResult<List<ContestSession>>> GetSessions() => Ok(await _sessions.GetAllAsync());

    [HttpGet("sessions/active")]
    public async Task<ActionResult<ContestSession?>> GetActiveSession()
    {
        var session = await _sessions.GetActiveAsync();
        return session == null ? NoContent() : Ok(session);
    }

    [HttpPost("sessions")]
    public async Task<ActionResult<ContestSession>> StartSession([FromBody] StartContestSessionRequest request)
    {
        try
        {
            var session = await _sessions.StartAsync(request.DefinitionId, request.MyExchange, request.Label, request.OperatingCallsign);
            await _contest.BroadcastStateAsync();
            return Ok(session);
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("sessions/{id}/activate")]
    public async Task<ActionResult<ContestSession>> ActivateSession(string id)
    {
        try
        {
            var session = await _sessions.ActivateAsync(id);
            await _contest.BroadcastStateAsync();
            return Ok(session);
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpGet("sessions/{id}/cabrillo")]
    public async Task<IActionResult> GetCabrillo(string id)
    {
        try
        {
            var (fileName, content) = await _contest.GenerateCabrilloAsync(id);
            return File(System.Text.Encoding.UTF8.GetBytes(content), "text/plain", fileName);
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("sessions/{id}/stop")]
    public async Task<IActionResult> StopSession(string id)
    {
        await _sessions.StopAsync(id);
        return NoContent();
    }

    /// <summary>
    /// Update the operator's own exchange on a running session (e.g. fix the county
    /// or power class mid-contest). Re-derives the in/out-of-area role, replays the
    /// session so power/role changes are reflected in the score, and broadcasts.
    /// </summary>
    [HttpPut("sessions/{id}/exchange")]
    public async Task<ActionResult<ContestStateDto?>> UpdateExchange(string id, [FromBody] MyExchange exchange)
    {
        try
        {
            await _sessions.UpdateExchangeAsync(id, exchange);
            return Ok(await _contest.RefreshActiveSessionAsync());
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpDelete("sessions/{id}")]
    public async Task<IActionResult> DeleteSession(string id)
    {
        var ok = await _sessions.DeleteAsync(id);
        return ok ? NoContent() : NotFound();
    }

    // -- live operating ------------------------------------------------------

    [HttpGet("state")]
    public async Task<ActionResult<ContestStateDto?>> GetState()
    {
        var state = await _contest.GetStateAsync();
        return state == null ? NoContent() : Ok(state);
    }

    [HttpGet("scp")]
    public async Task<ActionResult<List<string>>> GetScpCalls() => Ok(await _contest.GetScpCallsAsync());

    [HttpGet("check")]
    public async Task<ActionResult<ContestCheckResponse>> Check(
        [FromQuery] string callsign, [FromQuery] string band, [FromQuery] string mode)
    {
        try
        {
            return Ok(await _contest.CheckAsync(callsign, band, mode));
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("check-batch")]
    public async Task<ActionResult<List<BatchCheckEntry>>> CheckBatch([FromBody] ContestBatchCheckRequest request)
        => Ok(await _contest.CheckBatchAsync(request.Items ?? new List<BatchCheckItem>()));

    [HttpPost("qso")]
    public async Task<ActionResult<ContestLogResult>> LogQso([FromBody] LogContestQsoRequest request)
    {
        try
        {
            return Ok(await _contest.LogQsoAsync(request));
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpGet("qsos")]
    public async Task<ActionResult<List<ContestQsoDto>>> GetSessionQsos([FromQuery] int limit = 8)
        => Ok(await _contest.GetSessionQsosAsync(limit));

    [HttpPut("qso/{id}")]
    public async Task<ActionResult<ContestStateDto>> UpdateQso(string id, [FromBody] UpdateContestQsoRequest request)
    {
        try
        {
            return Ok(await _contest.UpdateQsoAsync(id, request));
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpDelete("qso/{id}")]
    public async Task<ActionResult<ContestStateDto>> DeleteQso(string id)
    {
        try
        {
            return Ok(await _contest.DeleteQsoAsync(id));
        }
        catch (ContestDefinitionException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }
}
