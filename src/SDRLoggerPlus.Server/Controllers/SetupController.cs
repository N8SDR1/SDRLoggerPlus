using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class SetupController : ControllerBase
{
    private readonly IDbContext _dbContext;
    private readonly IUserConfigService _userConfigService;
    private readonly ILogger<SetupController> _logger;

    public SetupController(
        IDbContext dbContext,
        IUserConfigService userConfigService,
        ILogger<SetupController> logger)
    {
        _dbContext = dbContext;
        _userConfigService = userConfigService;
        _logger = logger;
    }

    /// <summary>
    /// Get the current setup status. SDRLoggerPlus is LiteDB-only.
    /// </summary>
    [HttpGet("status")]
    [ProducesResponseType(typeof(SetupStatusResponse), StatusCodes.Status200OK)]
    public async Task<ActionResult<SetupStatusResponse>> GetStatus()
    {
        var config = await _userConfigService.GetConfigAsync();

        return Ok(new SetupStatusResponse
        {
            IsConfigured = _userConfigService.IsConfigured(),
            IsConnected = _dbContext.IsConnected,
            Provider = "local",
            ConfiguredAt = config.ConfiguredAt,
            DatabaseName = _dbContext.DatabaseName
        });
    }

    /// <summary>
    /// Save the (local) database configuration. Cloud providers are not supported.
    /// </summary>
    [HttpPost("configure")]
    [ProducesResponseType(typeof(ConfigureResponse), StatusCodes.Status200OK)]
    public async Task<ActionResult<ConfigureResponse>> Configure([FromBody] ConfigureRequest request)
    {
        _logger.LogInformation("Configuring Local (LiteDB) provider");

        await _userConfigService.SaveConfigAsync(new UserConfig
        {
            Provider = DatabaseProvider.Local,
        });

        return Ok(new ConfigureResponse
        {
            Success = true,
            Message = "Local database configured.",
            RestartRequired = false,
        });
    }
}

// DTOs
public class SetupStatusResponse
{
    public bool IsConfigured { get; set; }
    public bool IsConnected { get; set; }
    public string? Provider { get; set; }
    public DateTime? ConfiguredAt { get; set; }
    public string? DatabaseName { get; set; }
}

public class ConfigureRequest
{
    public string? Provider { get; set; }
}

public class ConfigureResponse
{
    public bool Success { get; set; }
    public string Message { get; set; } = null!;
    public bool RestartRequired { get; set; }
}
