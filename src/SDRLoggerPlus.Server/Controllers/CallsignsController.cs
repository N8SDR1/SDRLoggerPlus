using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services.Contesting;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/callsigns")]
[Produces("application/json")]
public class CallsignsController : ControllerBase
{
    // The community Super Check Partial master list — the general (worldwide) file.
    private const string MasterScpUrl = "https://www.supercheckpartial.com/MASTER.SCP";

    private readonly ScpService _scp;
    private readonly HttpClient _http;
    private readonly ILogger<CallsignsController> _logger;

    public CallsignsController(ScpService scp, IHttpClientFactory httpFactory, ILogger<CallsignsController> logger)
    {
        _scp = scp;
        _http = httpFactory.CreateClient();
        _http.Timeout = TimeSpan.FromSeconds(30);
        // Some hosts reset connections that send no User-Agent — set one explicitly.
        _http.DefaultRequestHeaders.UserAgent.ParseAdd("SDRLoggerPlus/2");
        _logger = logger;
    }

    /// <summary>Current SCP status: how many calls are loaded and when the user file was last updated.</summary>
    [HttpGet("scp/status")]
    public IActionResult ScpStatus() =>
        Ok(new { count = _scp.MasterCalls.Count, updatedUtc = _scp.UserFileUpdatedUtc });

    /// <summary>
    /// Download the latest MASTER.SCP from supercheckpartial.com and install it (replacing the
    /// bundled seed for both contest and everyday logging). Server-side fetch — no browser CORS.
    /// </summary>
    [HttpPost("scp/update")]
    public async Task<IActionResult> UpdateScp()
    {
        try
        {
            var text = await _http.GetStringAsync(MasterScpUrl);

            // Sanity-check before overwriting: a real MASTER.SCP has thousands of plausible calls.
            var parsed = ScpService.Parse(text.Split('\n'));
            if (parsed.Count < 100)
                return StatusCode(502, new { error = "Downloaded file didn't look like a valid MASTER.SCP." });

            var count = _scp.ImportMaster(text);
            _logger.LogInformation("MASTER.SCP updated: {Count} calls", count);
            return Ok(new { count, updatedUtc = _scp.UserFileUpdatedUtc });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "MASTER.SCP update failed");
            return StatusCode(502, new { error = $"Couldn't download MASTER.SCP: {ex.Message}" });
        }
    }
}
