using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services.Weather;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
public class WeatherController : ControllerBase
{
    private readonly WeatherAlertService _weatherService;
    private readonly LightningStrikeService _strikeService;

    public WeatherController(WeatherAlertService weatherService, LightningStrikeService strikeService)
    {
        _weatherService = weatherService;
        _strikeService = strikeService;
    }

    [HttpGet("lightning")]
    public ActionResult<LightningStatus> GetLightning() => Ok(_weatherService.GetLightningStatus());

    [HttpGet("wind")]
    public ActionResult<WindStatus> GetWind() => Ok(_weatherService.GetWindStatus());

    [HttpGet("lightning/strikes")]
    public ActionResult<IReadOnlyList<SDRLoggerPlus.Contracts.Events.LightningStrike>> GetLightningStrikes()
        => Ok(_strikeService.GetCurrent());
}
