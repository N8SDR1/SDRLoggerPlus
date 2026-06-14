using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

public record HotListAddRequest(List<string> Callsigns);
public record HotListFlagsRequest(bool? Enabled, bool? TtsEnabled);

[ApiController]
[Route("api/[controller]")]
public class HotListController : ControllerBase
{
    private readonly IHotListService _hotListService;
    private readonly ISettingsService _settingsService;

    public HotListController(IHotListService hotListService, ISettingsService settingsService)
    {
        _hotListService = hotListService;
        _settingsService = settingsService;
    }

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        var settings = (await _settingsService.GetSettingsAsync()).HotList;
        return Ok(new
        {
            enabled = settings.Enabled,
            ttsEnabled = settings.TtsEnabled,
            ttsCooldownMinutes = settings.TtsCooldownMinutes,
            callsigns = settings.Callsigns,
        });
    }

    [HttpPost("calls")]
    public async Task<IActionResult> Add([FromBody] HotListAddRequest request)
    {
        await _hotListService.AddAsync(request.Callsigns);
        return Ok(new { ok = true });
    }

    [HttpDelete("calls/{callsign}")]
    public async Task<IActionResult> Remove(string callsign)
    {
        await _hotListService.RemoveAsync(callsign);
        return Ok(new { ok = true });
    }

    [HttpDelete("calls")]
    public async Task<IActionResult> Clear()
    {
        await _hotListService.ClearAsync();
        return Ok(new { ok = true });
    }

    [HttpPut]
    public async Task<IActionResult> SetFlags([FromBody] HotListFlagsRequest request)
    {
        await _hotListService.SetFlagsAsync(request.Enabled, request.TtsEnabled);
        return Ok(new { ok = true });
    }
}
