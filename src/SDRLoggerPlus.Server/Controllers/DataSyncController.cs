using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Entity-level data API for networked/shared logging (S1). A field station running in
/// <c>DatabaseProvider.RemoteHost</c> mode talks to THIS endpoint on the host instead of a local
/// database file — its <c>RemoteApiQsoRepository</c> is the mirror image of these routes.
///
/// Deliberately separate from <see cref="QsosController"/>: that one returns the view-model
/// <see cref="QsoResponse"/> for the UI; this one round-trips the full <see cref="Qso"/> ENTITY so a
/// remote client behaves byte-identically to a local one (no fidelity loss). Only the OPERATIONAL
/// surface is exposed — QRZ/LoTW/QSL sync is host-only (the host owns uploads for the shared log; you
/// never want three field laptops each uploading it), so those repository methods are not routed here.
///
/// Under <c>/api</c>, so the token-auth middleware gates it whenever the backend is bound remotely.
/// </summary>
[ApiController]
[Route("api/data/qsos")]
public class DataSyncController : ControllerBase
{
    private readonly IQsoRepository _repo;
    public DataSyncController(IQsoRepository repo) => _repo = repo;

    /// <summary>Page wrapper for search — mirrors the repository's (Items, TotalCount) tuple over JSON.</summary>
    public record QsoPage(List<Qso> Items, int TotalCount);
    public record ExistsRequest(string Callsign, DateTime QsoDate, string TimeOn, string Band, string Mode);

    [HttpPost]
    public async Task<ActionResult<Qso>> Create([FromBody] Qso qso) => Ok(await _repo.CreateAsync(qso));

    [HttpPost("bulk")]
    public async Task<ActionResult<List<Qso>>> CreateBulk([FromBody] List<Qso> qsos) =>
        Ok((await _repo.CreateBulkAsync(qsos)).ToList());

    [HttpGet("{id}")]
    public async Task<ActionResult<Qso>> GetById(string id) =>
        await _repo.GetByIdAsync(id) is { } q ? Ok(q) : NotFound();

    [HttpGet("recent")]
    public async Task<ActionResult<List<Qso>>> Recent([FromQuery] int limit = 100) =>
        Ok((await _repo.GetRecentAsync(limit)).ToList());

    [HttpGet("all")]
    public async Task<ActionResult<List<Qso>>> All() => Ok((await _repo.GetAllAsync()).ToList());

    [HttpPut("{id}")]
    public async Task<ActionResult<bool>> Update(string id, [FromBody] Qso qso) =>
        Ok(await _repo.UpdateAsync(id, qso));

    [HttpDelete("{id}")]
    public async Task<ActionResult<bool>> Delete(string id) => Ok(await _repo.DeleteAsync(id));

    [HttpPost("delete-many")]
    public async Task<ActionResult<int>> DeleteMany([FromBody] List<string> ids) =>
        Ok(await _repo.DeleteManyAsync(ids));

    [HttpPost("search")]
    public async Task<ActionResult<QsoPage>> Search([FromBody] QsoSearchRequest criteria)
    {
        var (items, total) = await _repo.SearchAsync(criteria);
        return Ok(new QsoPage(items.ToList(), total));
    }

    [HttpGet("statistics")]
    public async Task<ActionResult<QsoStatistics>> Statistics([FromQuery] string? myCall = null) =>
        Ok(await _repo.GetStatisticsAsync(myCall));

    [HttpGet("count")]
    public async Task<ActionResult<int>> Count() => Ok(await _repo.GetCountAsync());

    [HttpPost("exists")]
    public async Task<ActionResult<bool>> Exists([FromBody] ExistsRequest r) =>
        Ok(await _repo.ExistsAsync(r.Callsign, r.QsoDate, r.TimeOn, r.Band, r.Mode));

    [HttpPost("by-ids")]
    public async Task<ActionResult<List<Qso>>> ByIds([FromBody] List<string> ids) =>
        Ok((await _repo.GetByIdsAsync(ids)).ToList());

    [HttpGet("contest-session/{sessionId}")]
    public async Task<ActionResult<List<Qso>>> ByContestSession(string sessionId) =>
        Ok(await _repo.GetByContestSessionAsync(sessionId));

    [HttpGet("distinct-callsigns")]
    public async Task<ActionResult<List<string>>> DistinctCallsigns() =>
        Ok(await _repo.GetDistinctCallsignsAsync());

    [HttpGet("recent-by-callsign")]
    public async Task<ActionResult<Qso>> RecentByCallsign([FromQuery] string callsign) =>
        await _repo.GetMostRecentByCallsignAsync(callsign) is { } q ? Ok(q) : NoContent();

    [HttpGet("find-duplicate")]
    public async Task<ActionResult<Qso>> FindDuplicate(
        [FromQuery] string callsign, [FromQuery] string band,
        [FromQuery] string mode, [FromQuery] DateTime createdSinceUtc) =>
        await _repo.FindRecentDuplicateAsync(callsign, band, mode, createdSinceUtc) is { } q ? Ok(q) : NoContent();
}
