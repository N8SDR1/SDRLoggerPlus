using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api")]
[Produces("application/json")]
public class SystemController : ControllerBase
{
    private readonly IDbContext _dbContext;
    private readonly IConfiguration _configuration;
    private readonly IHostApplicationLifetime _lifetime;

    public SystemController(IDbContext dbContext,
        IConfiguration configuration, IHostApplicationLifetime lifetime)
    {
        _dbContext = dbContext;
        _configuration = configuration;
        _lifetime = lifetime;
    }

    /// <summary>
    /// Health check endpoint
    /// </summary>
    [HttpGet("health")]
    [ProducesResponseType(typeof(HealthResponse), StatusCodes.Status200OK)]
    public ActionResult<HealthResponse> GetHealth()
    {
        return Ok(new HealthResponse("Healthy", DateTime.UtcNow, _dbContext.IsConnected));
    }

    /// <summary>
    /// Graceful shutdown, used by the Electron shell so the host's StopAsync
    /// path runs (on-exit backups etc.) instead of a hard TerminateProcess.
    /// Inert (404) unless the launcher configured SDRLOGGERPLUS_SHUTDOWN_TOKEN, and
    /// requires the exact token — otherwise any local webpage could POST a
    /// shutdown to a dev instance.
    /// </summary>
    [HttpPost("system/shutdown")]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult Shutdown([FromHeader(Name = "X-Shutdown-Token")] string? token)
    {
        var expected = _configuration["SDRLOGGERPLUS_SHUTDOWN_TOKEN"];
        if (string.IsNullOrEmpty(expected))
            return NotFound();

        var expectedBytes = System.Text.Encoding.UTF8.GetBytes(expected);
        var presentedBytes = System.Text.Encoding.UTF8.GetBytes(token ?? "");
        if (!System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(expectedBytes, presentedBytes))
            return Unauthorized();

        _lifetime.StopApplication();
        return Accepted();
    }

    /// <summary>
    /// Get available plugins
    /// </summary>
    [HttpGet("plugins")]
    [ProducesResponseType(typeof(IEnumerable<PluginInfo>), StatusCodes.Status200OK)]
    public ActionResult<IEnumerable<PluginInfo>> GetPlugins()
    {
        var plugins = new[]
        {
            new PluginInfo("cluster", "DX Cluster", "1.0.0", true),
            new PluginInfo("log-entry", "Log Entry", "1.0.0", true),
            new PluginInfo("log-history", "Log History", "1.0.0", true),
            new PluginInfo("map-globe", "Map/Globe", "1.0.0", true),
            new PluginInfo("antenna-genius", "Antenna Genius", "1.0.0", true)
        };

        return Ok(plugins);
    }
}

public record HealthResponse(string Status, DateTime Timestamp, bool DatabaseConnected);
public record PluginInfo(string Id, string Name, string Version, bool Enabled);
