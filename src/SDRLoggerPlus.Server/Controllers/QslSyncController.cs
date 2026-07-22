using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Read-only view of the QSL upload ledger.
///
/// Answers the question that had no answer before: which QSOs actually
/// reached Club Log, HRDLog and eQSL, and which failed and why.
/// </summary>
[ApiController]
[Route("api/qsl-sync")]
public class QslSyncController : ControllerBase
{
    private static readonly string[] Services =
    {
        QslSyncLedger.ClubLogKey, QslSyncLedger.HrdLogKey, QslSyncLedger.EqslKey,
    };

    private readonly IQsoRepository _repository;
    private readonly ClubLogService _clubLog;

    public QslSyncController(IQsoRepository repository, ClubLogService clubLog)
    {
        _repository = repository;
        _clubLog = clubLog;
    }

    /// <summary>Per-service totals across the whole log.</summary>
    [HttpGet("summary")]
    public async Task<ActionResult<Dictionary<string, QslSyncSummaryDto>>> GetSummary()
    {
        var all = (await _repository.GetAllAsync()).ToList();
        var summary = new Dictionary<string, QslSyncSummaryDto>();

        foreach (var service in Services)
        {
            var entries = all.Select(q => q.QslSync?.For(service)).ToList();
            summary[service] = new QslSyncSummaryDto(
                Total: all.Count,
                Synced: entries.Count(e => e?.Status == SyncStatus.Synced),
                Failed: entries.Count(e => e != null && e.Status != SyncStatus.Synced),
                Retryable: entries.Count(e => e?.IsRetryable == true),
                // QSOs logged before the ledger existed. Their real state is
                // unknowable, so they are reported separately rather than
                // being counted as "not sent" — a bulk re-send of 24k QSOs to
                // realtime.php would earn an immediate Club Log IP ban.
                Untracked: entries.Count(e => e == null));
        }

        return Ok(summary);
    }

    /// <summary>QSOs whose last attempt failed in a way that a retry could fix.</summary>
    [HttpGet("failures/{service}")]
    public async Task<ActionResult<IEnumerable<object>>> GetFailures(string service)
    {
        if (!Services.Contains(service))
            return BadRequest($"Unknown service '{service}'. Expected one of: {string.Join(", ", Services)}");

        var failures = await _repository.GetQslFailuresAsync(service);
        return Ok(failures.Select(q => new
        {
            q.Id,
            q.Callsign,
            q.QsoDate,
            q.Band,
            q.Mode,
            Sync = QsoService.MapQslSync(q.QslSync),
        }));
    }

    /// <summary>Whether Club Log's one-strike auth block is currently tripped.</summary>
    [HttpGet("clublog-block")]
    public ActionResult<object> GetClubLogBlock() => Ok(new { blocked = _clubLog.IsBlocked });
}
