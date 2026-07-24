using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services.Sat;

namespace SDRLoggerPlus.Server.Controllers;

public record SatActiveRequest(bool Active);
public record SatUpdateTleRequest(string SourceUrl);

[ApiController]
[Route("api/sat")]
public class SatController : ControllerBase
{
    private readonly SatControllerService _satService;

    public SatController(SatControllerService satService) => _satService = satService;

    [HttpGet("status")]
    public ActionResult<SatState> GetStatus() => Ok(_satService.GetState());

    [HttpPost("active")]
    public async Task<IActionResult> SetActive([FromBody] SatActiveRequest request)
    {
        await _satService.SetActiveAsync(request.Active);
        return Ok(new { ok = true, active = _satService.IsActive });
    }

    /// <summary>The satellites/transponders configured on the CSN controller (from /f.txt).</summary>
    [HttpGet("configured")]
    public async Task<ActionResult<IReadOnlyList<SatConfiguredTransponder>>> GetConfigured(CancellationToken ct)
        => Ok(await _satService.GetConfiguredTranspondersAsync(ct));

    /// <summary>The built-in TLE source URLs the controller supports.</summary>
    [HttpGet("tle-sources")]
    public ActionResult<IReadOnlyList<SatTleSource>> GetTleSources() => Ok(SatControllerService.TleSources);

    /// <summary>Tell the controller to pull fresh TLEs from a source URL (http only).</summary>
    [HttpPost("update-tle")]
    public async Task<IActionResult> UpdateTle([FromBody] SatUpdateTleRequest request, CancellationToken ct)
    {
        var ok = await _satService.UpdateTleAsync(request.SourceUrl, ct);
        return ok ? Ok(new { ok = true }) : BadRequest(new { ok = false, error = "TLE update failed (controller unreachable or https URL)." });
    }

    /// <summary>Tell the controller to update its frequency database from the internet.</summary>
    [HttpPost("update-freqdb")]
    public async Task<IActionResult> UpdateFreqDb(CancellationToken ct)
    {
        var ok = await _satService.UpdateFreqDbAsync(ct);
        return ok ? Ok(new { ok = true }) : BadRequest(new { ok = false, error = "Freq DB update failed (controller unreachable)." });
    }
}
