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
    private readonly IAdifService _adif;
    private readonly ISettingsService _settings;

    public EqslController(EqslService eqsl, IAdifService adif, ISettingsService settings)
    {
        _eqsl = eqsl;
        _adif = adif;
        _settings = settings;
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
            var adifText = await _eqsl.DownloadInboxAdifAsync(ct);
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(adifText));
            var result = await _adif.MergeConfirmationsAsync(stream, ConfirmationSource.Eqsl, ct);

            // Stamp the sync time so the next pull is incremental.
            var settings = await _settings.GetSettingsAsync();
            settings.Eqsl.LastConfirmationSync = DateTime.UtcNow;
            await _settings.SaveSettingsAsync(settings);

            return Ok(result);
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
