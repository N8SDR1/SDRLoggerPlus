using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services.Sat;

namespace SDRLoggerPlus.Server.Controllers;

public record SatActiveRequest(bool Active);

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
}
