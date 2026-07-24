using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
public class SettingsController : ControllerBase
{
    private readonly ISettingsService _settingsService;
    private readonly IHotListService _hotListService;
    private readonly ILogger<SettingsController> _logger;
    private readonly ClubLogService? _clubLog;

    public SettingsController(ISettingsService settingsService, IHotListService hotListService,
        ILogger<SettingsController> logger, ClubLogService? clubLog = null)
    {
        _settingsService = settingsService;
        _hotListService = hotListService;
        _logger = logger;
        _clubLog = clubLog;
    }

    /// <summary>
    /// Clear Club Log's persisted one-strike auth block when its credentials
    /// change. The block stops all uploads after a single 403 (repeating
    /// failed POSTs triggers Club Log's IP firewall) and, being persisted, it
    /// survives restarts — so saving corrected credentials must be a way out,
    /// or the operator is stuck until they happen to press "Test".
    /// Changed-credentials-only: re-saving unrelated settings while still
    /// blocked must NOT re-arm uploads against known-bad credentials.
    /// </summary>
    private void ResetClubLogBlockIfCredentialsChanged(UserSettings existing, UserSettings incoming)
    {
        if (_clubLog is not { IsBlocked: true }) return;

        var before = existing.ClubLog;
        var after = incoming.ClubLog;
        var changed = before.Email != after.Email
                      || before.Password != after.Password
                      || before.ApiKey != after.ApiKey
                      || before.Callsign != after.Callsign;

        if (changed)
        {
            _clubLog.ResetBlock();
            _logger.LogInformation("Club Log credentials changed — upload block cleared");
        }
    }

    /// <summary>
    /// Get user settings
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public async Task<ActionResult<UserSettings>> GetSettings()
    {
        try
        {
            var settings = await _settingsService.GetSettingsAsync();
            return Ok(settings);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Database not available when retrieving settings");
            return StatusCode(StatusCodes.Status503ServiceUnavailable, new
            {
                error = "Database not connected. Please configure your database connection in Settings > Database."
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve settings");
            return StatusCode(StatusCodes.Status500InternalServerError, new
            {
                error = "Failed to retrieve settings: " + ex.Message
            });
        }
    }

    /// <summary>
    /// Save user settings
    /// </summary>
    [HttpPost]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public async Task<ActionResult<UserSettings>> SaveSettings([FromBody] UserSettings settings)
    {
        if (settings == null)
        {
            return BadRequest("Settings cannot be null");
        }

        _logger.LogInformation("Saving settings for station: {Callsign}", settings.Station?.Callsign);

        try
        {
            // Preserve fields managed by dedicated endpoints. The general
            // POST /api/settings bulk save from the frontend doesn't know
            // about these fields, so its outgoing JSON has null/empty
            // values that would otherwise wipe them out on Upsert.
            //   - SavedLayouts → managed by /api/settings/layouts subresource
            //   - LayoutJson   → managed by PUT /api/settings/layout
            // Load the existing record and reinject those values.
            var existing = await _settingsService.GetSettingsAsync(settings.Id);
            settings.SavedLayouts = existing.SavedLayouts ?? new();
            if (string.IsNullOrEmpty(settings.LayoutJson))
            {
                settings.LayoutJson = existing.LayoutJson;
            }

            ResetClubLogBlockIfCredentialsChanged(existing, settings);

            var saved = await _settingsService.SaveSettingsAsync(settings);
            // Hot list matching runs off an in-memory set — refresh it so
            // edits made through the settings panel take effect immediately
            await _hotListService.ReloadAsync();
            return Ok(saved);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Database not available when saving settings");
            return StatusCode(StatusCodes.Status503ServiceUnavailable, new
            {
                error = "Database not connected. Please configure your database connection in Settings > Database."
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save settings");
            return StatusCode(StatusCodes.Status500InternalServerError, new
            {
                error = "Failed to save settings: " + ex.Message
            });
        }
    }

    /// <summary>
    /// Update station settings only
    /// </summary>
    [HttpPut("station")]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    public async Task<ActionResult<UserSettings>> UpdateStationSettings([FromBody] StationSettings stationSettings)
    {
        var settings = await _settingsService.GetSettingsAsync();
        settings.Station = stationSettings;
        var saved = await _settingsService.SaveSettingsAsync(settings);
        return Ok(saved);
    }

    /// <summary>
    /// Update QRZ settings only
    /// </summary>
    [HttpPut("qrz")]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    public async Task<ActionResult<UserSettings>> UpdateQrzSettings([FromBody] QrzSettings qrzSettings)
    {
        var settings = await _settingsService.GetSettingsAsync();
        settings.Qrz = qrzSettings;
        var saved = await _settingsService.SaveSettingsAsync(settings);
        return Ok(saved);
    }

    /// <summary>
    /// Update appearance settings only
    /// </summary>
    [HttpPut("appearance")]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    public async Task<ActionResult<UserSettings>> UpdateAppearanceSettings([FromBody] AppearanceSettings appearanceSettings)
    {
        var settings = await _settingsService.GetSettingsAsync();
        settings.Appearance = appearanceSettings;
        var saved = await _settingsService.SaveSettingsAsync(settings);
        return Ok(saved);
    }

    /// <summary>
    /// Update map settings only
    /// </summary>
    [HttpPut("map")]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    public async Task<ActionResult<UserSettings>> UpdateMapSettings([FromBody] MapSettings mapSettings)
    {
        var settings = await _settingsService.GetSettingsAsync();
        settings.Map = mapSettings;
        var saved = await _settingsService.SaveSettingsAsync(settings);
        return Ok(saved);
    }

    /// <summary>
    /// Update AI settings only
    /// </summary>
    [HttpPut("ai")]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    public async Task<ActionResult<UserSettings>> UpdateAiSettings([FromBody] AiSettings aiSettings)
    {
        var settings = await _settingsService.GetSettingsAsync();
        settings.Ai = aiSettings;
        var saved = await _settingsService.SaveSettingsAsync(settings);
        return Ok(saved);
    }

    /// <summary>
    /// Update grid column state for a specific table
    /// </summary>
    [HttpPut("grid-state/{tableId}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult> UpdateGridState(string tableId, [FromBody] string columnStateJson)
    {
        var settings = await _settingsService.GetSettingsAsync();
        settings.GridStates ??= new Dictionary<string, string>();
        settings.GridStates[tableId] = columnStateJson;
        await _settingsService.SaveSettingsAsync(settings);
        return Ok();
    }

    /// <summary>
    /// Full-replace settings import — used by the operator-facing
    /// "Import Settings" button in Appearance. Distinct from the general
    /// POST /api/settings, which is called on every small settings change
    /// and preserves SavedLayouts/LayoutJson from the DB. Import replaces
    /// the whole document as-is so restoring a backup actually restores
    /// what the user exported.
    ///
    /// Contains credentials (QRZ/HamQTH/ClubLog/HRDLog passwords, API
    /// keys). The client warns before download and again before import.
    /// </summary>
    [HttpPost("import")]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<UserSettings>> ImportSettings([FromBody] UserSettings settings)
    {
        if (settings == null) return BadRequest("Settings cannot be null");
        // Pin the id so a backup exported from a different-id document
        // still lands on the caller's "default" settings row.
        settings.Id = string.IsNullOrEmpty(settings.Id) ? "default" : settings.Id;
        _logger.LogInformation("Importing settings for station: {Callsign} (full replace)",
            settings.Station?.Callsign);
        try
        {
            var saved = await _settingsService.SaveSettingsAsync(settings);
            await _hotListService.ReloadAsync();
            return Ok(saved);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to import settings");
            return StatusCode(StatusCodes.Status500InternalServerError, new
            {
                error = "Failed to import settings: " + ex.Message
            });
        }
    }

    /// <summary>
    /// Update layout JSON only
    /// </summary>
    [HttpPut("layout")]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    public async Task<ActionResult<UserSettings>> UpdateLayout([FromBody] string layoutJson)
    {
        _logger.LogInformation("Updating layout configuration");
        var settings = await _settingsService.GetSettingsAsync();
        settings.LayoutJson = layoutJson;
        var saved = await _settingsService.SaveSettingsAsync(settings);
        return Ok(saved);
    }

    // ── Named layout presets ─────────────────────────────────────────────
    // Up to 3 named preset slots stored on UserSettings.SavedLayouts.
    // Distinct from LayoutJson (the auto-saved live arrangement). Load =
    // client fetches by name and applies via setLayout, which flows back
    // through the normal auto-save path so the newly-applied preset
    // becomes the live layout too.

    private const int MaxSavedLayouts = 10;

    public record SaveLayoutRequest(string Name, string LayoutJson);

    /// <summary>
    /// List saved layout slots. Includes the full LayoutJson so a follow-up
    /// GET-then-apply flow can be done in one round trip — 3 slots × ~2 KB
    /// each is a tiny payload.
    /// </summary>
    [HttpGet("layouts")]
    [ProducesResponseType(typeof(List<SavedLayoutSlot>), StatusCodes.Status200OK)]
    public async Task<ActionResult<List<SavedLayoutSlot>>> GetSavedLayouts()
    {
        var settings = await _settingsService.GetSettingsAsync();
        return Ok(settings.SavedLayouts ?? new List<SavedLayoutSlot>());
    }

    /// <summary>
    /// Save (or overwrite) a named layout preset. If a slot with that name
    /// already exists it's replaced (updating SavedAt). If not, a new slot
    /// is appended — provided we're not already at MaxSavedLayouts, in which
    /// case a 409 is returned instructing the client to delete something
    /// first.
    /// </summary>
    [HttpPost("layouts")]
    [ProducesResponseType(typeof(List<SavedLayoutSlot>), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<ActionResult<List<SavedLayoutSlot>>> SaveNamedLayout([FromBody] SaveLayoutRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
            return BadRequest(new { error = "Name is required" });
        if (string.IsNullOrWhiteSpace(request.LayoutJson))
            return BadRequest(new { error = "LayoutJson is required" });

        var settings = await _settingsService.GetSettingsAsync();
        settings.SavedLayouts ??= new List<SavedLayoutSlot>();
        var trimmedName = request.Name.Trim();

        var existing = settings.SavedLayouts.FirstOrDefault(l =>
            string.Equals(l.Name, trimmedName, StringComparison.OrdinalIgnoreCase));
        if (existing != null)
        {
            existing.LayoutJson = request.LayoutJson;
            existing.SavedAt = DateTime.UtcNow;
            _logger.LogInformation("Overwrote saved layout preset '{Name}'", trimmedName);
        }
        else
        {
            if (settings.SavedLayouts.Count >= MaxSavedLayouts)
            {
                return Conflict(new
                {
                    error = $"Already at the {MaxSavedLayouts}-slot limit — delete one first",
                    slots = settings.SavedLayouts.Select(l => l.Name).ToArray(),
                });
            }
            settings.SavedLayouts.Add(new SavedLayoutSlot
            {
                Name = trimmedName,
                LayoutJson = request.LayoutJson,
                SavedAt = DateTime.UtcNow,
            });
            _logger.LogInformation("Saved new layout preset '{Name}' ({Count}/{Max})",
                trimmedName, settings.SavedLayouts.Count, MaxSavedLayouts);
        }

        var saved = await _settingsService.SaveSettingsAsync(settings);
        return Ok(saved.SavedLayouts ?? new List<SavedLayoutSlot>());
    }

    /// <summary>
    /// Delete a named layout preset by name. Case-insensitive match on the
    /// stored Name. Idempotent — deleting something that doesn't exist
    /// still returns 200 with the current list.
    /// </summary>
    [HttpDelete("layouts/{name}")]
    [ProducesResponseType(typeof(List<SavedLayoutSlot>), StatusCodes.Status200OK)]
    public async Task<ActionResult<List<SavedLayoutSlot>>> DeleteNamedLayout(string name)
    {
        var settings = await _settingsService.GetSettingsAsync();
        settings.SavedLayouts ??= new List<SavedLayoutSlot>();
        var removed = settings.SavedLayouts.RemoveAll(l =>
            string.Equals(l.Name, name, StringComparison.OrdinalIgnoreCase));
        if (removed > 0)
        {
            _logger.LogInformation("Deleted saved layout preset '{Name}'", name);
            await _settingsService.SaveSettingsAsync(settings);
        }
        return Ok(settings.SavedLayouts);
    }

    /// <summary>
    /// Update desktop window geometry only
    /// </summary>
    [HttpPut("window")]
    [ProducesResponseType(typeof(UserSettings), StatusCodes.Status200OK)]
    public async Task<ActionResult<UserSettings>> UpdateWindow([FromBody] WindowState window)
    {
        var settings = await _settingsService.GetSettingsAsync();
        settings.Window = window;
        var saved = await _settingsService.SaveSettingsAsync(settings);
        return Ok(saved);
    }
}
