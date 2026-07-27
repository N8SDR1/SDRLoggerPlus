using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services;

public interface ISpotStatusService
{
    string? GetSpotStatus(string dxCall, string? country, double frequencyKhz, string? mode);
    /// <summary>
    /// CQ-zone (WAZ) status for a spot: the resolved CQ zone plus whether it's a
    /// new zone entirely ("newZone") or a worked zone on a new band ("newZoneBand"),
    /// or null if already worked on this band / unresolvable. Independent of the
    /// DXCC status — a worked country+band can still be a new zone (5BWAZ).
    /// </summary>
    (int? Zone, string? Status) GetZoneStatus(string dxCall, double frequencyKhz);
    /// <summary>
    /// Maidenhead grid status for a spot/decode carrying a locator: "newGrid"
    /// (this 4-char grid never worked), "newGridBand" (worked but not on this
    /// band), or null (already worked on this band / no grid). Grid comes from
    /// the FT8 message or a callbook lookup, not cty.dat.
    /// </summary>
    string? GetGridStatus(string? grid, double frequencyKhz);
    void OnQsoLogged(string callsign, string? country, string band, string mode, string? grid = null);
    Task InvalidateCacheAsync();
}

public class SpotStatusService : ISpotStatusService, IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<SpotStatusService> _logger;

    // Thread-safe cache structures — keyed by country name (from QSO log)
    private HashSet<string> _workedCountries = new(StringComparer.OrdinalIgnoreCase);
    private HashSet<string> _workedCountryBands = new(StringComparer.OrdinalIgnoreCase);
    private HashSet<string> _workedCountryBandModes = new(StringComparer.OrdinalIgnoreCase);
    // CQ-zone (WAZ) worked sets: zones ever worked, and zone+band for 5BWAZ.
    private HashSet<int> _workedZones = new();
    private HashSet<string> _workedZoneBands = new(StringComparer.OrdinalIgnoreCase);
    // Maidenhead grid (VUCC / grid-chasing) worked sets: 4-char grids ever
    // worked, and grid+band.
    private HashSet<string> _workedGrids = new(StringComparer.OrdinalIgnoreCase);
    private HashSet<string> _workedGridBands = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _cacheLock = new();

    public SpotStatusService(
        IServiceProvider serviceProvider,
        ILogger<SpotStatusService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    // Await this to know the initial cache build has finished (used by tests).
    internal Task CacheReady => _cacheReady.Task;
    private readonly TaskCompletionSource _cacheReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly CancellationTokenSource _cacheRetryCts = new();

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("SpotStatusService starting, building cache in background...");

        // Build the cache in the background so a slow database query
        // (e.g. 4000+ QSOs) never blocks app startup.
        _ = Task.Run(async () =>
        {
            if (!await BuildCacheAsync())
            {
                await RetryBuildCacheAsync(_cacheRetryCts.Token);
            }
            _cacheReady.TrySetResult();
        });

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cacheRetryCts.Cancel();
        _cacheReady.TrySetResult();
        return Task.CompletedTask;
    }

    private async Task RetryBuildCacheAsync(CancellationToken ct)
    {
        var delaySeconds = 5;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            _logger.LogInformation("SpotStatusService retrying cache build...");
            if (await BuildCacheAsync())
            {
                _logger.LogInformation("SpotStatusService cache build succeeded on retry");
                return;
            }

            delaySeconds = Math.Min(delaySeconds * 2, 60);
        }
    }

    public string? GetSpotStatus(string dxCall, string? country, double frequencyKhz, string? mode)
    {
        // Until the initial cache build completes, _workedCountries is empty and every
        // lookup would return "newDxcc" — which then sticks on the spot in the UI even
        // after the cache is ready. Return null (no status) during that window so spots
        // arriving early show no indicator rather than a wrong one.
        if (!_cacheReady.Task.IsCompleted)
            return null;

        if (string.IsNullOrEmpty(country))
            return null;

        var band = BandHelper.GetBand((long)(frequencyKhz * 1000));
        if (band == "Unknown")
            return null;

        var normalizedCountry = NormalizeCountryName(country);

        lock (_cacheLock)
        {
            // New DXCC - never worked this country/entity
            if (!_workedCountries.Contains(normalizedCountry))
                return "newDxcc";

            // New Band - worked this country but not on this band
            var countryBandKey = $"{normalizedCountry}:{band}";
            if (!_workedCountryBands.Contains(countryBandKey))
                return "newBand";

            // Worked - country+band+mode match (already worked this entity on this band+mode)
            if (mode != null)
            {
                var normalizedMode = NormalizeMode(mode);
                var countryBandModeKey = $"{normalizedCountry}:{band}:{normalizedMode}";
                if (_workedCountryBandModes.Contains(countryBandModeKey))
                    return "worked";
            }
        }

        // Country+band worked but not with this mode (or mode unknown)
        return null;
    }

    public (int? Zone, string? Status) GetZoneStatus(string dxCall, double frequencyKhz)
    {
        // Same warm-up guard as GetSpotStatus — no verdict until the cache is ready.
        if (!_cacheReady.Task.IsCompleted)
            return (null, null);

        // CQ zone comes straight from cty.dat (per-call/prefix, exceptions included).
        var (_, _, cqZone) = CtyService.GetEntityFromCallsign(dxCall);
        if (cqZone is null)
            return (null, null);

        var band = BandHelper.GetBand((long)(frequencyKhz * 1000));
        if (band == "Unknown")
            return (cqZone, null);

        lock (_cacheLock)
        {
            // New zone — never worked this CQ zone at all (basic WAZ).
            if (!_workedZones.Contains(cqZone.Value))
                return (cqZone, "newZone");

            // Worked zone, but not on this band (5-band WAZ fill).
            if (!_workedZoneBands.Contains($"{cqZone.Value}:{band}"))
                return (cqZone, "newZoneBand");
        }

        return (cqZone, null);
    }

    public string? GetGridStatus(string? grid, double frequencyKhz)
    {
        if (!_cacheReady.Task.IsCompleted)
            return null;

        var g = NormalizeGrid(grid);
        if (g is null)
            return null;

        var band = BandHelper.GetBand((long)(frequencyKhz * 1000));

        lock (_cacheLock)
        {
            // New grid — never worked this 4-char square at all.
            if (!_workedGrids.Contains(g))
                return "newGrid";

            // Worked grid, but not on this band.
            if (band != "Unknown" && !_workedGridBands.Contains($"{g}:{band}"))
                return "newGridBand";
        }

        return null;
    }

    public void OnQsoLogged(string callsign, string? country, string band, string mode, string? grid = null)
    {
        lock (_cacheLock)
        {
            var normalizedMode = NormalizeMode(mode);

            if (!string.IsNullOrEmpty(country))
            {
                var normalizedCountry = NormalizeCountryName(country);
                _workedCountries.Add(normalizedCountry);
                _workedCountryBands.Add($"{normalizedCountry}:{band}");

                if (!string.IsNullOrEmpty(normalizedMode))
                {
                    _workedCountryBandModes.Add($"{normalizedCountry}:{band}:{normalizedMode}");
                }
            }

            // Also index by CtyService-resolved country name
            var (ctyCountry, _) = CtyService.GetCountryFromCallsign(callsign);
            if (!string.IsNullOrEmpty(ctyCountry) && !string.Equals(ctyCountry, country, StringComparison.OrdinalIgnoreCase))
            {
                var normalizedCtyCountry = NormalizeCountryName(ctyCountry);
                _workedCountries.Add(normalizedCtyCountry);
                _workedCountryBands.Add($"{normalizedCtyCountry}:{band}");

                if (!string.IsNullOrEmpty(normalizedMode))
                {
                    _workedCountryBandModes.Add($"{normalizedCtyCountry}:{band}:{normalizedMode}");
                }
            }

            // CQ zone (WAZ) — resolve from the callsign via cty.dat so it's
            // present even when the QSO didn't store a zone.
            var (_, _, cqZone) = CtyService.GetEntityFromCallsign(callsign);
            if (cqZone is not null && !string.IsNullOrEmpty(band))
            {
                _workedZones.Add(cqZone.Value);
                _workedZoneBands.Add($"{cqZone.Value}:{band}");
            }

            var normalizedGrid = NormalizeGrid(grid);
            if (normalizedGrid is not null)
            {
                _workedGrids.Add(normalizedGrid);
                if (!string.IsNullOrEmpty(band))
                    _workedGridBands.Add($"{normalizedGrid}:{band}");
            }
        }

        _logger.LogDebug("SpotStatusService cache updated for {Callsign} on {Band} {Mode}", callsign, band, mode);
    }

    public async Task InvalidateCacheAsync()
    {
        _logger.LogInformation("SpotStatusService cache invalidation requested, rebuilding...");
        await BuildCacheAsync();
    }

    private async Task<bool> BuildCacheAsync()
    {
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var qsoRepo = scope.ServiceProvider.GetRequiredService<IQsoRepository>();
            var allQsos = await qsoRepo.GetAllAsync();
            // Worked-before needed-status is a PERSONAL judgement — a club/special contest run must
            // not mark your countries/zones/grids as worked. Exclude non-personal-call QSOs.
            //
            // Resolved optionally on purpose. The operating callsign only refines which QSOs count;
            // without it every QSO counts, which is the pre-contest-session behaviour and perfectly
            // usable. GetRequiredService would throw instead, and the catch below turns any throw
            // into a permanent retry loop that never completes _cacheReady — so a missing or broken
            // settings service would take the whole spot-status cache down with it.
            var myCall = scope.ServiceProvider.GetService<ISettingsService>() is { } settingsService
                ? (await settingsService.GetSettingsAsync())?.Station?.Callsign
                : null;

            var newCountries = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var newCountryBands = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var newCountryBandModes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var newZones = new HashSet<int>();
            var newZoneBands = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var newGrids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var newGridBands = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var qso in allQsos)
            {
                if (!QsoOwnership.IsPersonalQso(qso, myCall)) continue;
                var country = qso.Country ?? qso.Station?.Country;
                var band = qso.Band;
                var mode = NormalizeMode(qso.Mode);
                var callsign = qso.Callsign?.ToUpperInvariant();

                if (string.IsNullOrEmpty(band) || string.IsNullOrEmpty(callsign))
                    continue;

                // Grids live in Station.Grid (populated by QRZ/callbook lookups &
                // the QRZ import), with the top-level Grid as fallback — matching
                // GridOf() in LiteQsoRepository, the source of the log's grid stat.
                var grid = NormalizeGrid(qso.Station?.Grid ?? qso.Grid);
                if (grid is not null)
                {
                    newGrids.Add(grid);
                    newGridBands.Add($"{grid}:{band}");
                }

                // CQ zone (WAZ): prefer the logged zone, else resolve from the
                // callsign via cty.dat so older QSOs without a stored zone count.
                var cqZone = qso.Station?.CqZone ?? CtyService.GetEntityFromCallsign(callsign).CqZone;
                if (cqZone is not null)
                {
                    newZones.Add(cqZone.Value);
                    newZoneBands.Add($"{cqZone.Value}:{band}");
                }

                if (!string.IsNullOrEmpty(country))
                {
                    var normalizedCountry = NormalizeCountryName(country);
                    newCountries.Add(normalizedCountry);
                    newCountryBands.Add($"{normalizedCountry}:{band}");

                    if (!string.IsNullOrEmpty(mode))
                    {
                        newCountryBandModes.Add($"{normalizedCountry}:{band}:{mode}");
                    }
                }

                // Also index by CtyService-resolved country name to handle naming
                // mismatches (e.g. QSO stores "Germany" but cty.dat uses "Fed. Rep. of Germany")
                var (ctyCountry, _) = CtyService.GetCountryFromCallsign(callsign);
                if (!string.IsNullOrEmpty(ctyCountry) && !string.Equals(ctyCountry, country, StringComparison.OrdinalIgnoreCase))
                {
                    var normalizedCtyCountry = NormalizeCountryName(ctyCountry);
                    newCountries.Add(normalizedCtyCountry);
                    newCountryBands.Add($"{normalizedCtyCountry}:{band}");

                    if (!string.IsNullOrEmpty(mode))
                    {
                        newCountryBandModes.Add($"{normalizedCtyCountry}:{band}:{mode}");
                    }
                }
            }

            lock (_cacheLock)
            {
                _workedCountries = newCountries;
                _workedCountryBands = newCountryBands;
                _workedCountryBandModes = newCountryBandModes;
                _workedZones = newZones;
                _workedZoneBands = newZoneBands;
                _workedGrids = newGrids;
                _workedGridBands = newGridBands;
            }

            _logger.LogInformation(
                "SpotStatusService cache built: {CountryCount} countries, {BandCount} country+band combos, {ModeCount} country+band+mode entries, {ZoneCount} CQ zones, {GridCount} grids",
                newCountries.Count, newCountryBands.Count, newCountryBandModes.Count, newZones.Count, newGrids.Count);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to build SpotStatusService cache");
            return false;
        }
    }

    /// <summary>
    /// Maps alternative country names (from CC cluster feeds or other sources) to the ADIF entity
    /// names stored in the QSO database. CtyService now returns ADIF-standard names directly,
    /// so these aliases handle CC cluster abbreviations and informal naming differences.
    /// </summary>
    private static readonly Dictionary<string, string> CountryAliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["UAE"] = "United Arab Emirates",
        ["Trinidad & Tobago"] = "Trinidad and Tobago",
        ["Ivory Coast"] = "Cote d'Ivoire",
    };

    private static string NormalizeCountryName(string country)
    {
        return CountryAliases.TryGetValue(country, out var normalized) ? normalized : country;
    }

    /// <summary>
    /// Reduce a locator to its 4-char field+square (the VUCC/grid-award unit),
    /// uppercased. Returns null for anything shorter or malformed.
    /// </summary>
    private static string? NormalizeGrid(string? grid)
    {
        if (string.IsNullOrWhiteSpace(grid) || grid.Length < 4)
            return null;
        var g = grid.Trim().ToUpperInvariant();
        if (g[0] < 'A' || g[0] > 'R' || g[1] < 'A' || g[1] > 'R' ||
            g[2] < '0' || g[2] > '9' || g[3] < '0' || g[3] > '9')
            return null;
        return g[..4];
    }

    private static string NormalizeMode(string mode)
    {
        if (string.IsNullOrEmpty(mode))
            return mode;

        var upper = mode.ToUpperInvariant();
        return upper switch
        {
            "USB" or "LSB" => "SSB",
            "PSK31" or "PSK63" or "PSK125" => "PSK",
            _ => upper,
        };
    }
}
