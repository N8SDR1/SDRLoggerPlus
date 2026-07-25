using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/satellites")]
[Produces("application/json")]
public class SatellitesController : ControllerBase
{
    private readonly ILogger<SatellitesController> _logger;
    private readonly HttpClient _httpClient;

    // Cache for TLE data (refresh every 6 hours)
    private static Dictionary<string, TLEData>? _cachedTLEData;
    private static DateTime _lastFetch = DateTime.MinValue;
    private static readonly TimeSpan CacheExpiration = TimeSpan.FromHours(6);

    // NORAD ids SatNOGS reports as re-entered or dead — hidden from the picker so the list
    // isn't cluttered with the provably-gone. Refreshed daily; empty if SatNOGS is
    // unreachable, in which case nothing is hidden (the full in-orbit feed shows).
    private static HashSet<int>? _hiddenNorads;
    private static DateTime _satnogsFetch = DateTime.MinValue;
    private static readonly TimeSpan SatnogsCacheExpiration = TimeSpan.FromHours(24);

    // Popular amateur radio satellites with their NORAD catalog numbers
    public SatellitesController(ILogger<SatellitesController> logger, IHttpClientFactory httpClientFactory)
    {
        _logger = logger;
        _httpClient = httpClientFactory.CreateClient();
        _httpClient.Timeout = TimeSpan.FromSeconds(30);
    }

    /// <summary>
    /// Get TLE (Two-Line Element) data for specified satellites
    /// Data sourced from Celestrak
    /// </summary>
    [HttpPost("tle")]
    [ProducesResponseType(typeof(Dictionary<string, TLEData>), StatusCodes.Status200OK)]
    public async Task<ActionResult<Dictionary<string, TLEData>>> GetTLEData([FromBody] TLERequest request)
    {
        try
        {
            var catalog = await GetCachedCatalogAsync();

            // Match each requested name (or NORAD number) against the live feed, keyed by
            // exactly what was requested so the client maps positions back onto its selection.
            var result = new Dictionary<string, TLEData>();
            foreach (var requested in request.Satellites ?? new List<string>())
            {
                var match = MatchRequested(catalog, requested);
                if (match != null) result[requested] = match;
            }
            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to fetch TLE data");
            return StatusCode(500, new { error = "Failed to fetch TLE data" });
        }
    }

    /// <summary>
    /// Get list of available amateur radio satellites
    /// </summary>
    [HttpGet("list")]
    [ProducesResponseType(typeof(List<SatelliteInfo>), StatusCodes.Status200OK)]
    public async Task<ActionResult<List<SatelliteInfo>>> GetAvailableSatellites()
    {
        // The current amateur-satellite feed IS the list — dead birds fall off it and new
        // ones appear, so there's no hardcoded roster to go stale. Empty on total fetch
        // failure; the UI then falls back to whatever the operator already has selected.
        try
        {
            var catalog = await GetCachedCatalogAsync();
            var hidden = await GetHiddenNoradsAsync();
            var satellites = catalog.Values
                .Select(t => new SatelliteInfo { Name = t.Name, NoradId = NoradOf(t.Line1) })
                // Keep unknown-NORAD entries (can't judge them); drop the ones SatNOGS
                // confirms are re-entered or dead.
                .Where(s => s.NoradId <= 0 || !hidden.Contains(s.NoradId))
                .OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
            return Ok(satellites);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not list satellites from the live feed");
            return Ok(new List<SatelliteInfo>());
        }
    }

    // Amateur TLEs live at Celestrak; AMSAT publishes the same set and is reachable when
    // Celestrak is blocked or rate-limiting (which it does aggressively to ham IPs and some
    // VPN egress ranges). Try Celestrak first, fall back to AMSAT — either fills the map.
    private const string CelestrakGroupUrl = "https://celestrak.org/NORAD/elements/gp.php?GROUP=amateur&FORMAT=tle";
    private const string AmsatTleUrl = "https://www.amsat.org/tle/current/nasabare.txt";

    /// <summary>
    /// NORAD ids SatNOGS marks re-entered or dead, cached 24 h. SatNOGS reliably tracks what
    /// has decayed; it's conservative about calling a still-orbiting bird "dead", so this
    /// trims the provably-gone, not every silent satellite (hence the picker's caution).
    /// Returns empty if SatNOGS is unreachable — then nothing is hidden.
    /// </summary>
    private async Task<HashSet<int>> GetHiddenNoradsAsync()
    {
        if (_hiddenNorads != null && DateTime.UtcNow - _satnogsFetch < SatnogsCacheExpiration)
            return _hiddenNorads;

        var hidden = new HashSet<int>();
        foreach (var status in new[] { "re-entered", "dead" })
        {
            try
            {
                var json = await _httpClient.GetStringAsync($"https://db.satnogs.org/api/satellites/?status={status}&format=json");
                using var doc = JsonDocument.Parse(json);
                foreach (var el in doc.RootElement.EnumerateArray())
                {
                    if (el.TryGetProperty("norad_cat_id", out var n) && n.ValueKind == JsonValueKind.Number)
                        hidden.Add(n.GetInt32());
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "SatNOGS '{Status}' status fetch failed", status);
            }
        }

        _hiddenNorads = hidden;
        _satnogsFetch = DateTime.UtcNow;
        return hidden;
    }

    /// <summary>The full current amateur-satellite catalog, cached for 6 h and shared by /list and /tle.</summary>
    private async Task<Dictionary<string, TLEData>> GetCachedCatalogAsync()
    {
        if (_cachedTLEData != null && DateTime.UtcNow - _lastFetch < CacheExpiration)
            return _cachedTLEData;

        var catalog = await FetchFullCatalogAsync();
        _cachedTLEData = catalog;
        _lastFetch = DateTime.UtcNow;
        return catalog;
    }

    /// <summary>Fetch the whole amateur feed — every satellite, not a fixed roster.</summary>
    private async Task<Dictionary<string, TLEData>> FetchFullCatalogAsync()
    {
        var catalog = new Dictionary<string, TLEData>(StringComparer.OrdinalIgnoreCase);

        try
        {
            ParseTleInto(await _httpClient.GetStringAsync(CelestrakGroupUrl), catalog);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Celestrak amateur-group TLE fetch failed; trying AMSAT");
        }

        // AMSAT carries the same set and is reachable when Celestrak is blocked. Only needed
        // if Celestrak gave us nothing (it's the more complete feed when it works).
        if (catalog.Count == 0)
        {
            try
            {
                ParseTleInto(await _httpClient.GetStringAsync(AmsatTleUrl), catalog);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "AMSAT TLE fallback fetch failed");
            }
        }

        if (catalog.Count == 0)
            throw new InvalidOperationException("No TLE data available from Celestrak or AMSAT");

        return catalog;
    }

    /// <summary>
    /// Parse a TLE feed and add EVERY satellite to the catalog, keyed by its feed name.
    /// Line-anchored (find "1 …"/"2 …" pairs and take the preceding name line) so it
    /// survives blank lines or odd spacing rather than assuming clean 3-line groups.
    /// </summary>
    private static void ParseTleInto(string response, Dictionary<string, TLEData> catalog)
    {
        var lines = response.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        for (int i = 1; i + 1 < lines.Length; i++)
        {
            var l1 = lines[i].Trim();
            var l2 = lines[i + 1].Trim();
            if (!l1.StartsWith("1 ", StringComparison.Ordinal) || !l2.StartsWith("2 ", StringComparison.Ordinal))
                continue;

            var name = lines[i - 1].Trim();
            if (name.Length == 0 || name.StartsWith("1 ", StringComparison.Ordinal)) continue;
            catalog.TryAdd(name, new TLEData { Name = name, Line1 = l1, Line2 = l2 });
        }
    }

    /// <summary>
    /// Resolve a requested satellite (name or NORAD number) against the live catalog:
    /// exact name, then a name that starts-with / contains it (so "ISS" finds
    /// "ISS (ZARYA)"), then the NORAD catalog number.
    /// </summary>
    private static TLEData? MatchRequested(Dictionary<string, TLEData> catalog, string? requested)
    {
        var r = requested?.Trim();
        if (string.IsNullOrEmpty(r)) return null;

        if (catalog.TryGetValue(r, out var exact)) return exact;

        var ru = r.ToUpperInvariant();
        var byName = catalog.Values.FirstOrDefault(t => t.Name.ToUpperInvariant().StartsWith(ru, StringComparison.Ordinal))
                  ?? catalog.Values.FirstOrDefault(t => t.Name.ToUpperInvariant().Contains(ru));
        if (byName != null) return byName;

        if (int.TryParse(r, out var norad))
            return catalog.Values.FirstOrDefault(t => NoradOf(t.Line1) == norad);

        return null;
    }

    /// <summary>NORAD catalog number from TLE line 1 (columns 3–7), or -1 if unparseable.</summary>
    private static int NoradOf(string? line1) =>
        line1 != null && line1.Length >= 7 && int.TryParse(line1.Substring(2, 5), out var n) ? n : -1;
}

public class TLERequest
{
    public List<string> Satellites { get; set; } = new();
}

public class TLEData
{
    public string Name { get; set; } = string.Empty;
    public string Line1 { get; set; } = string.Empty;
    public string Line2 { get; set; } = string.Empty;
}

public class SatelliteInfo
{
    public string Name { get; set; } = string.Empty;
    public int NoradId { get; set; }
}
