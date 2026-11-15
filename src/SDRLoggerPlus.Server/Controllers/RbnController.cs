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
            var location = await _rbnService.LookupStationLocationAsync(callsign);
            if (location is null)
            {
                return NotFound(new { error = "Location not found" });
            }

            // grid/country stay in the response shape for existing clients; the
            // lookup resolves coordinates only (QRZ or cty.dat centroid).
            return Ok(new
            {
                callsign = callsign,
                grid = (string?)null,
                lat = location.Lat,
                lon = location.Lon,
                country = (string?)null,
                precise = location.Precise
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
            if (await _rbnService.LookupStationLocationAsync(s.Callsign) is not { } loc) continue; // no location → can't draw an arc
            var (la, lo) = (loc.Lat, loc.Lon);
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
