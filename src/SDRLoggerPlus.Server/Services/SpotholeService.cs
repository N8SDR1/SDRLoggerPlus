using System.Text.Json;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Spothole.app spot source (SDRLogger+ port). Spothole is a READ-ONLY REST
/// aggregator (DX clusters, POTA, SOTA, RBN…) — no telnet, no spot submission.
/// Polls GET https://spothole.app/api/v1/spots every 30 s (source=Cluster,
/// limit=300; first poll max_age=600, then received_since=&lt;newest seen&gt;)
/// and feeds mapped spots into the same DxClusterService pipeline as telnet
/// clusters, sharing its deduplication and hot-list handling.
///
/// A spotter-country filter (cty.dat prefix lookup, default "United States")
/// drops spots heard by spotters outside the configured country at ingest.
/// </summary>
public class SpotholeService : BackgroundService
{
    private const string ApiUrl = "https://spothole.app/api/v1/spots";

    private readonly ILogger<SpotholeService> _logger;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly DxClusterService _cluster;
    private readonly HttpClient _http;
    private double? _cursor; // newest received_time (unix epoch) seen

    public SpotholeService(ILogger<SpotholeService> logger, IServiceScopeFactory scopeFactory,
        DxClusterService cluster, HttpClient http)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _cluster = cluster;
        _http = http;
    }

    private static string? Str(JsonElement el, string name) =>
        el.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String ? p.GetString() : null;

    /// <summary>
    /// Maps one spothole API spot (freq in Hz) to the cluster pipeline's ParsedSpot
    /// (kHz). The API supplies dx_country/dx_continent/dx_grid/dx_dxcc_id directly,
    /// with a cty.dat fallback when absent.
    /// </summary>
    internal static ParsedSpot? MapSpot(JsonElement spot)
    {
        var dxCall = Str(spot, "dx_call")?.ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(dxCall)) return null;

        if (!spot.TryGetProperty("freq", out var freqEl) || !freqEl.TryGetDouble(out var freqHz) || freqHz <= 0)
            return null;

        var spotter = Str(spot, "de_call")?.ToUpperInvariant() ?? "?";
        var mode = Str(spot, "mode");
        var comment = Str(spot, "comment") ?? "";
        if (Str(spot, "sig") is { Length: > 0 } sig)
            comment = string.Join(' ', new[] { comment, $"[{sig}]" }.Where(s => !string.IsNullOrWhiteSpace(s)));

        var country = Str(spot, "dx_country");
        var continent = Str(spot, "dx_continent");
        if (country is null)
            (country, continent) = CtyService.GetCountryFromCallsign(dxCall);

        int? dxcc = spot.TryGetProperty("dx_dxcc_id", out var d) && d.TryGetInt32(out var dxccId) ? dxccId : null;

        // Use the actual spot time (unix epoch seconds) — the first poll returns a
        // 10-minute backlog, and stamping those "now" would misorder the table.
        var timestamp = spot.TryGetProperty("time", out var t) && t.TryGetDouble(out var epoch) && epoch > 0
            ? DateTimeOffset.FromUnixTimeSeconds((long)epoch).UtcDateTime
            : DateTime.UtcNow;

        return new ParsedSpot(
            DxCall: dxCall,
            Spotter: spotter,
            Frequency: freqHz / 1000.0, // Hz → kHz (cluster pipeline convention)
            Mode: string.IsNullOrWhiteSpace(mode) ? null : mode,
            Comment: string.IsNullOrWhiteSpace(comment) ? null : comment,
            Timestamp: timestamp,
            Country: country,
            Continent: continent,
            Dxcc: dxcc,
            Grid: Str(spot, "dx_grid")
        );
    }

    /// <summary>
    /// True when the spotter's country matches the filter (empty filter = all).
    /// Prefers the API's de_country field; falls back to a cty.dat prefix lookup.
    /// </summary>
    internal static bool SpotterMatchesCountry(JsonElement spot, string? countryFilter)
    {
        if (string.IsNullOrWhiteSpace(countryFilter)) return true;

        var country = Str(spot, "de_country");
        if (country is null)
        {
            var call = (Str(spot, "de_call") ?? "").Split('-')[0];
            (country, _) = CtyService.GetCountryFromCallsign(call);
        }
        return string.Equals(country, countryFilter, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Advances the received_since cursor to the newest received_time seen.
    /// received_time is a NUMERIC unix epoch (e.g. 1781222727.633) — the real
    /// API sends a number, not a string.
    /// </summary>
    internal static double? AdvanceCursor(double? cursor, JsonElement spot)
    {
        if (spot.TryGetProperty("received_time", out var rt) && rt.TryGetDouble(out var received))
        {
            if (cursor is null || received > cursor)
                return received;
        }
        return cursor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Spothole service starting");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await TickAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Spothole poll failed: {Message}", ex.Message);
            }
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var cluster = (await scope.ServiceProvider.GetRequiredService<ISettingsService>().GetSettingsAsync()).Cluster;
        if (!cluster.SpotholeEnabled)
        {
            _cursor = null; // restart fresh when re-enabled
            return;
        }

        var url = $"{ApiUrl}?limit=300&source=Cluster" +
                  (_cursor is null
                      ? "&max_age=600"
                      : $"&received_since={_cursor.Value.ToString("F6", System.Globalization.CultureInfo.InvariantCulture)}");

        using var response = await _http.GetAsync(url, ct);
        if (!response.IsSuccessStatusCode)
        {
            _logger.LogWarning("Spothole: HTTP {Status}", (int)response.StatusCode);
            return;
        }

        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        if (doc.RootElement.ValueKind != JsonValueKind.Array) return;

        var forwarded = 0;
        foreach (var spotEl in doc.RootElement.EnumerateArray())
        {
            _cursor = AdvanceCursor(_cursor, spotEl);

            if (!SpotterMatchesCountry(spotEl, cluster.SpotholeSpotterCountry)) continue;

            var spot = MapSpot(spotEl);
            if (spot is null) continue;

            await _cluster.IngestExternalSpotAsync(spot, "spothole", "Spothole.app");
            forwarded++;
        }

        if (forwarded > 0)
            _logger.LogInformation("Spothole: forwarded {Count} spot(s)", forwarded);
    }
}
