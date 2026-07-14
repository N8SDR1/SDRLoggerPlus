using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/eqsl")]
[Produces("application/json")]
public class EqslController : ControllerBase
{
    private readonly EqslService _eqsl;
    private readonly IConfirmationSyncService _sync;

    public EqslController(EqslService eqsl, IConfirmationSyncService sync)
    {
        _eqsl = eqsl;
        _sync = sync;
    }

    /// <summary>
    /// Download the eQSL inbox (received QSLs) and merge it into the log —
    /// marks matching QSOs Confirmed. One-click "sync from eQSL".
    /// </summary>
    [HttpPost("download-confirmations")]
    [ProducesResponseType(typeof(ConfirmationMergeResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<ConfirmationMergeResponse>> DownloadConfirmations(CancellationToken ct)
    {
        try
        {
            return Ok(await _sync.SyncEqslAsync(ct));
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(ex.Message);
        }
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
