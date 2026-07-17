using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Rbn;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
public class RbnController : ControllerBase
{
    private readonly ILogger<RbnController> _logger;
    private readonly IRbnService _rbnService;

    public RbnController(
        ILogger<RbnController> logger,
        IRbnService rbnService)
    {
        _logger = logger;
        _rbnService = rbnService;
    }

    [HttpGet("spots")]
    public IActionResult GetSpots([FromQuery] int minutes = 5)
    {
        try
        {
            var spots = _rbnService.GetRecentSpots(minutes);
            return Ok(new
            {
                count = spots.Count,
                spots = spots
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving RBN spots");
            return StatusCode(500, new { error = "Failed to retrieve RBN spots" });
        }
    }

    [HttpGet("location/{callsign}")]
    public async Task<IActionResult> GetSkimmerLocation(string callsign)
    {
        try
        {
            var (grid, lat, lon, country) = await _rbnService.LookupSkimmerLocationAsync(callsign);

            // Grid is always null by design (RbnService resolves only lat/lon via QRZ/cty.dat);
            // presence is keyed off coordinates so a resolved skimmer isn't reported as 404.
            if (lat == null || lon == null)
            {
                return NotFound(new { error = "Location not found" });
            }

            return Ok(new
            {
                callsign = callsign,
                grid = grid,
                lat = lat,
                lon = lon,
                country = country
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error looking up skimmer location for {Callsign}", callsign);
            return StatusCode(500, new { error = "Failed to lookup skimmer location" });
        }
    }

    [HttpGet("heardme")]
    public async Task<IActionResult> GetHeardMe(
        [FromQuery] string callsign,
        [FromQuery] string? band = null,
        [FromQuery] int minutes = 30)
    {
        if (string.IsNullOrWhiteSpace(callsign))
            return BadRequest(new { error = "callsign is required" });

        minutes = RbnHeardMeLogic.ClampWindowMinutes(minutes);
        var matches = RbnHeardMeLogic.HeardBy(_rbnService.GetRecentSpots(minutes), callsign, band);

        var now = DateTime.UtcNow;
        var reports = new List<RbnHeardMeReport>();
        foreach (var s in matches)
        {
            var (_, lat, lon, _) = await _rbnService.LookupSkimmerLocationAsync(s.Callsign);
            if (lat is not { } la || lon is not { } lo) continue; // no location → can't draw an arc
            reports.Add(new RbnHeardMeReport
            {
                Skimmer = s.Callsign,
                Lat = la,
                Lon = lo,
                FreqKhz = s.Frequency,
                Band = s.Band,
                Mode = s.Mode,
                Snr = s.Snr ?? 0,
                AgeSeconds = (long)Math.Max(0, (now - s.Timestamp).TotalSeconds),
            });
        }
        return Ok(reports);
    }
}
