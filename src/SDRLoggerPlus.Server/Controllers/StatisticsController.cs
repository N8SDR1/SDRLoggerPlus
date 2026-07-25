using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class StatisticsController : ControllerBase
{
    private readonly IAwardsService _awardsService;

    public StatisticsController(IAwardsService awardsService)
    {
        _awardsService = awardsService;
    }

    /// <summary>
    /// Get DXCC statistics with worked/confirmed status by band
    /// </summary>
    [HttpGet("dxcc")]
    [ProducesResponseType(typeof(DxccStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<DxccStatistics>> GetDxccStatistics(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null,
        [FromQuery] string? continent = null,
        [FromQuery] string? status = null,
        [FromQuery] DateTime? fromDate = null,
        [FromQuery] DateTime? toDate = null)
    {
        var filters = new StatisticsFilters(
            Band: band,
            Mode: mode,
            Continent: continent,
            Status: status,
            FromDate: fromDate,
            ToDate: toDate
        );

        var statistics = await _awardsService.GetDxccStatisticsAsync(filters);
        return Ok(statistics);
    }

    /// <summary>
    /// Get VUCC grid square statistics with worked/confirmed status by band
    /// </summary>
    [HttpGet("vucc")]
    [ProducesResponseType(typeof(VuccStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<VuccStatistics>> GetVuccStatistics(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null,
        [FromQuery] string? status = null,
        [FromQuery] DateTime? fromDate = null,
        [FromQuery] DateTime? toDate = null)
    {
        var filters = new StatisticsFilters(
            Band: band,
            Mode: mode,
            Status: status,
            FromDate: fromDate,
            ToDate: toDate
        );

        var statistics = await _awardsService.GetVuccStatisticsAsync(filters);
        return Ok(statistics);
    }

    /// <summary>
    /// Worked 4-char grids across all bands (or a filtered band/mode) for the
    /// Grid Tracker map — worked + confirmed status per grid.
    /// </summary>
    [HttpGet("gridmap")]
    [ProducesResponseType(typeof(GridMapStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<GridMapStatistics>> GetGridMap(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null,
        [FromQuery] DateTime? fromDate = null,
        [FromQuery] DateTime? toDate = null)
    {
        var filters = new StatisticsFilters(Band: band, Mode: mode, FromDate: fromDate, ToDate: toDate);
        return Ok(await _awardsService.GetGridMapAsync(filters));
    }

    /// <summary>
    /// Get POTA (Parks on the Air) statistics
    /// </summary>
    [HttpGet("pota")]
    [ProducesResponseType(typeof(PotaStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<PotaStatistics>> GetPotaStatistics(
        [FromQuery] string? activityType = null,
        [FromQuery] DateTime? fromDate = null,
        [FromQuery] DateTime? toDate = null)
    {
        var filters = new PotaFilters(
            ActivityType: activityType,
            FromDate: fromDate,
            ToDate: toDate
        );

        var statistics = await _awardsService.GetPotaStatisticsAsync(filters);
        return Ok(statistics);
    }

    /// <summary>
    /// Get IOTA (Islands on the Air) statistics
    /// </summary>
    [HttpGet("iota")]
    [ProducesResponseType(typeof(IotaStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<IotaStatistics>> GetIotaStatistics(
        [FromQuery] string? continent = null,
        [FromQuery] string? status = null,
        [FromQuery] DateTime? fromDate = null,
        [FromQuery] DateTime? toDate = null)
    {
        var filters = new IotaFilters(
            Continent: continent,
            Status: status,
            FromDate: fromDate,
            ToDate: toDate
        );

        var statistics = await _awardsService.GetIotaStatisticsAsync(filters);
        return Ok(statistics);
    }

    /// <summary>
    /// Get WAS (Worked All States) statistics
    /// </summary>
    [HttpGet("was")]
    [ProducesResponseType(typeof(WasStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<WasStatistics>> GetWasStatistics(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null)
        => Ok(await _awardsService.GetWasStatisticsAsync(new StatisticsFilters(Band: band, Mode: mode)));

    /// <summary>
    /// Get satellite operating statistics: birds worked, plus the grids,
    /// states and DXCC entities worked through satellites.
    /// </summary>
    [HttpGet("satellites")]
    [ProducesResponseType(typeof(SatelliteStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<SatelliteStatistics>> GetSatelliteStatistics(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null)
        => Ok(await _awardsService.GetSatelliteStatisticsAsync(new StatisticsFilters(Band: band, Mode: mode)));

    /// <summary>
    /// Get USA-CA (US Counties Award) statistics: worked, confirmed and target
    /// counties for every state.
    /// </summary>
    [HttpGet("counties")]
    [ProducesResponseType(typeof(CountiesStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<CountiesStatistics>> GetCountiesStatistics(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null)
        => Ok(await _awardsService.GetCountiesStatisticsAsync(new StatisticsFilters(Band: band, Mode: mode)));

    /// <summary>
    /// Get every county in one state with its worked/confirmed status — the
    /// drilldown behind a row of the counties table, and the "still needed"
    /// list for that state.
    /// </summary>
    [HttpGet("counties/{state}")]
    [ProducesResponseType(typeof(List<CountyDetail>), StatusCodes.Status200OK)]
    public async Task<ActionResult<List<CountyDetail>>> GetCountyDetails(
        string state,
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null)
        => Ok(await _awardsService.GetCountyDetailsAsync(state, new StatisticsFilters(Band: band, Mode: mode)));

    /// <summary>
    /// FFMA (Fred Fish Memorial Award): 6 m, all 488 grids of the contiguous 48
    /// states, confirmed by LoTW or paper QSL. Band and mode are fixed by the award;
    /// only an optional date range is honoured.
    /// </summary>
    [HttpGet("ffma")]
    [ProducesResponseType(typeof(FfmaStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<FfmaStatistics>> GetFfmaStatistics(
        [FromQuery] DateTime? fromDate = null,
        [FromQuery] DateTime? toDate = null)
        => Ok(await _awardsService.GetFfmaStatisticsAsync(new StatisticsFilters(FromDate: fromDate, ToDate: toDate)));

    /// <summary>
    /// Get WAZ (Worked All Zones) statistics
    /// </summary>
    [HttpGet("waz")]
    [ProducesResponseType(typeof(WazStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<WazStatistics>> GetWazStatistics(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null)
        => Ok(await _awardsService.GetWazStatisticsAsync(new StatisticsFilters(Band: band, Mode: mode)));

    /// <summary>
    /// Get WPX (Worked Prefixes) statistics
    /// </summary>
    [HttpGet("wpx")]
    [ProducesResponseType(typeof(WpxStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<WpxStatistics>> GetWpxStatistics(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null)
        => Ok(await _awardsService.GetWpxStatisticsAsync(new StatisticsFilters(Band: band, Mode: mode)));

    /// <summary>
    /// Get WAC (Worked All Continents) statistics
    /// </summary>
    [HttpGet("wac")]
    [ProducesResponseType(typeof(WacStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<WacStatistics>> GetWacStatistics(
        [FromQuery] string? band = null,
        [FromQuery] string? mode = null)
        => Ok(await _awardsService.GetWacStatisticsAsync(new StatisticsFilters(Band: band, Mode: mode)));

    /// <summary>
    /// Get 5-Band WAS statistics (80/40/20/15/10)
    /// </summary>
    [HttpGet("5bwas")]
    [ProducesResponseType(typeof(FiveBandStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<FiveBandStatistics>> Get5BWasStatistics([FromQuery] string? mode = null)
        => Ok(await _awardsService.Get5BWasStatisticsAsync(mode));

    /// <summary>
    /// Get 5-Band DXCC statistics (80/40/20/15/10)
    /// </summary>
    [HttpGet("5bdxcc")]
    [ProducesResponseType(typeof(FiveBandStatistics), StatusCodes.Status200OK)]
    public async Task<ActionResult<FiveBandStatistics>> Get5BDxccStatistics([FromQuery] string? mode = null)
        => Ok(await _awardsService.Get5BDxccStatisticsAsync(mode));
}
