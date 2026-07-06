using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/eqsl")]
[Produces("application/json")]
public class EqslController : ControllerBase
{
    private readonly EqslService _eqsl;

    public EqslController(EqslService eqsl)
    {
        _eqsl = eqsl;
    }

    /// <summary>
    /// Verify eQSL credentials by posting an empty ADIF and inspecting the
    /// response. eQSL doesn't publish a dedicated auth-check endpoint, so
    /// the test uses the real ImportADIF.cfm endpoint but sends zero
    /// records — enough to reach the auth check without submitting a QSO.
    /// </summary>
    [HttpPost("test")]
    [ProducesResponseType(typeof(object), StatusCodes.Status200OK)]
    public async Task<ActionResult> Test(CancellationToken ct)
    {
        var (ok, message) = await _eqsl.TestAsync(ct);
        return Ok(new { success = ok, message });
    }
}
