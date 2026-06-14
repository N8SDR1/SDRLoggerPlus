using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ContestsController : ControllerBase
{
    private readonly ContestsService _contestsService;

    public ContestsController(ContestsService contestsService)
    {
        _contestsService = contestsService;
    }

    [HttpGet]
    public async Task<ActionResult<List<Contest>>> GetContests([FromQuery] int days = 7)
    {
        var contests = await _contestsService.GetUpcomingContestsAsync(days);
        return Ok(contests);
    }

    [HttpGet("live")]
    public ActionResult<List<Contest>> GetLiveContests()
    {
        var contests = _contestsService.GetLiveContests();
        return Ok(contests);
    }
}
