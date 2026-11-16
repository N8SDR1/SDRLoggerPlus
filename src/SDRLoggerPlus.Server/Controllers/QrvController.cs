using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Qrv;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// QRV presence controls. The hosted client owns heartbeats. This API only
/// flips the opt-in, reports the log Mode select, and returns the cached roster.
/// </summary>
[ApiController]
[Route("api/qrv")]
[Produces("application/json")]
public class QrvController : ControllerBase
{
    private readonly QrvPresenceClient _client;
    private readonly QrvQsoTotalsClient _totals;
    private readonly IServiceScopeFactory _scopes;

    public QrvController(QrvPresenceClient client, QrvQsoTotalsClient totals, IServiceScopeFactory scopes)
    {
        _client = client;
        _totals = totals;
        _scopes = scopes;
    }

    /// <summary>Local totals opt-in state. This does not call qrvsdr.cc.</summary>
    [HttpGet("totals")]
    public QrvTotalsStatus Totals() => _totals.Status();

    [HttpGet]
    public QrvStatusSnapshot Get([FromQuery] bool watch = false)
    {
        if (watch) _client.TouchRosterWatch();
        return _client.Status();
    }

    /// <summary>Log entry Mode select. Band still comes from the radio; Hz is not published.</summary>
    [HttpPost("activity")]
    public IActionResult Activity([FromBody] QrvActivityRequest? body)
    {
        _client.ReportLoggingMode(body?.Mode, body?.RadioId);
        return NoContent();
    }

    [HttpPost("watch")]
    public IActionResult Watch([FromBody] QrvWatchRequest? body)
    {
        if (body?.Watching == true) _client.TouchRosterWatch();
        else _client.SetRosterWatching(false);
        return NoContent();
    }

    [HttpPut("settings")]
    public async Task<ActionResult<QrvStatusSnapshot>> UpdateSettings(
        [FromBody] QrvSettingsWrite? body,
        CancellationToken cancellationToken)
    {
        using var scope = _scopes.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var settings = await settingsService.GetSettingsAsync();
        settings.Qrv ??= new QrvSettings();
        if (body?.Enabled is bool enabled)
            settings.Qrv.Enabled = enabled;
        if (!string.IsNullOrWhiteSpace(body?.BaseUrl))
            settings.Qrv.BaseUrl = body!.BaseUrl!.Trim();
        await settingsService.SaveSettingsAsync(settings);

        _client.ApplySettings(
            settings.Qrv.Enabled,
            settings.Qrv.BaseUrl,
            settings.Station?.Callsign,
            settings.Qrv.InstallKey,
            settings.Qrv.BoundCall);
        await _client.TickAsync(cancellationToken);
        return Ok(_client.Status());
    }
}

public sealed class QrvActivityRequest
{
    public string? Mode { get; set; }
    public string? RadioId { get; set; }
}

public sealed class QrvWatchRequest
{
    public bool Watching { get; set; }
}

public sealed class QrvSettingsWrite
{
    public bool? Enabled { get; set; }
    public string? BaseUrl { get; set; }
}
