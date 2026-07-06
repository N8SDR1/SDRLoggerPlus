using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// HamQTH callbook integration endpoints. Currently just credential
/// verification for the Settings → HamQTH "Test Credentials" button
/// (mirrors what QrzController does for QRZ). Lookups themselves flow
/// through LogHub, not this controller.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class HamQthController : ControllerBase
{
    private readonly IHamQthService _hamQthService;

    public HamQthController(IHamQthService hamQthService)
    {
        _hamQthService = hamQthService;
    }

    public record HamQthTestRequest(string Username, string Password);

    [HttpPost("test")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<IActionResult> Test([FromBody] HamQthTestRequest request, CancellationToken ct)
    {
        var result = await _hamQthService.TestCredentialsAsync(request.Username, request.Password, ct);
        return Ok(new { Success = result.Success, Message = result.Message });
    }
}
