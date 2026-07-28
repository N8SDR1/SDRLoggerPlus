using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Backup;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Logbook health — opt-in maintenance the operator runs against their own log.
/// First tool: Verify QSO times (docs/design/timezone-architecture.md §5a).
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class LogbookHealthController : ControllerBase
{
    private readonly LogbookHealthService _health;
    private readonly BackupService _backup;
    private readonly ILogger<LogbookHealthController> _logger;

    public LogbookHealthController(
        LogbookHealthService health, BackupService backup, ILogger<LogbookHealthController> logger)
    {
        _health = health;
        _backup = backup;
        _logger = logger;
    }

    /// <summary>Read-only scan: bucket every QSO by whether its time is consistent / fixable / ambiguous.</summary>
    [HttpGet("qso-times")]
    public async Task<ActionResult<QsoTimeAuditResult>> ScanQsoTimes()
        => Ok(await _health.AuditQsoTimesAsync());

    /// <summary>
    /// Repair the chosen fixable rows. Takes a full backup snapshot first (so the change is always
    /// reversible), then reconstructs each row's lost time — sync flags preserved, so nothing is
    /// re-uploaded to QRZ/LoTW.
    /// </summary>
    [HttpPost("qso-times/repair")]
    public async Task<ActionResult<QsoTimeRepairResult>> RepairQsoTimes([FromBody] QsoTimeRepairRequest request)
    {
        if (request.Ids is null || request.Ids.Count == 0)
            return BadRequest("No QSO ids supplied.");

        var result = await _health.RepairFixableTimesAsync(
            request.Ids,
            snapshotBefore: async () =>
            {
                _logger.LogInformation("Verify QSO times: taking a backup before repairing {Count} row(s)", request.Ids.Count);
                await _backup.RunNowAsync("qso-time-repair");
            });

        _logger.LogInformation("Verify QSO times: repaired {Repaired}, skipped {Skipped} of {Requested}",
            result.Repaired, result.Skipped, result.Requested);
        return Ok(result);
    }

    /// <summary>Read-only scan: group the log into duplicate sets with a keep/delete decision per row.</summary>
    [HttpGet("duplicates")]
    public async Task<ActionResult<QsoDuplicateScanResult>> ScanDuplicates()
        => Ok(await _health.FindDuplicatesAsync());

    /// <summary>
    /// Delete the chosen redundant rows. Takes a full backup first; deletes are local-only (they never
    /// touch QRZ/LoTW), and the server re-validates that every id is a non-keeper before deleting.
    /// </summary>
    [HttpPost("duplicates/remove")]
    public async Task<ActionResult<QsoDuplicateRemoveResult>> RemoveDuplicates([FromBody] QsoDuplicateRemoveRequest request)
    {
        if (request.Ids is null || request.Ids.Count == 0)
            return BadRequest("No QSO ids supplied.");

        var result = await _health.RemoveDuplicatesAsync(
            request.Ids,
            snapshotBefore: async () =>
            {
                _logger.LogInformation("Find duplicates: taking a backup before removing up to {Count} row(s)", request.Ids.Count);
                await _backup.RunNowAsync("duplicate-removal");
            });

        _logger.LogInformation("Find duplicates: deleted {Deleted}, skipped {Skipped} of {Requested}",
            result.Deleted, result.Skipped, result.Requested);
        return Ok(result);
    }
}
