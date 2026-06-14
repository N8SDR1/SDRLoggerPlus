using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services.Weather;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
public class WeatherController : ControllerBase
{
    private readonly WeatherAlertService _weatherService;

    public WeatherController(WeatherAlertService weatherService) => _weatherService = weatherService;

    [HttpGet("lightning")]
    public ActionResult<LightningStatus> GetLightning() => Ok(_weatherService.GetLightningStatus());

    [HttpGet("wind")]
    public ActionResult<WindStatus> GetWind() => Ok(_weatherService.GetWindStatus());
}
