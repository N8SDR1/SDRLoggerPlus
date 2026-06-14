using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Server.Services.Backup;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
public class BackupController : ControllerBase
{
    private readonly BackupService _backupService;

    public BackupController(BackupService backupService) => _backupService = backupService;

    [HttpGet("status")]
    public async Task<ActionResult<BackupStatusDto>> GetStatus()
        => Ok(await _backupService.GetStatusAsync());

    [HttpPost("run")]
    public async Task<ActionResult<BackupRunResult>> RunNow()
        => Ok(await _backupService.RunNowAsync("manual"));
}
