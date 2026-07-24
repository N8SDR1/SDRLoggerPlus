using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services.Sat;

/// <summary>One configured transponder read from the controller's /f.txt table.</summary>
public sealed record SatConfiguredTransponder(
    string CatalogNumber,
    long UplinkHz,
    long DownlinkHz,
    string UplinkMode,
    string DownlinkMode,
    string Name);

/// <summary>A built-in TLE source the controller can pull from.</summary>
public sealed record SatTleSource(string Label, string Url);

/// <summary>
/// CSN S.A.T. controller management — reads the configured transponder list and
/// triggers the controller's own TLE / frequency-database updates. These reproduce
/// exactly what the controller's built-in web UI does:
///   - configured list: HTTP GET /f.txt  (fixed-width transponder table)
///   - update TLE:       HTTP GET /cmd?a=Y|&lt;http-url&gt;
///   - update freq DB:   HTTP GET /cmd?a=Z|f|
/// The command protocol is undocumented — derived from the controller's web-UI
/// JavaScript. HTTPS TLE URLs are rejected by the controller (http only). The
/// destructive "factory reset freq DB" command (Z|h|) is deliberately NOT exposed.
/// </summary>
public partial class SatControllerService
{
    /// <summary>The TLE sources the controller's own web UI offers (http only).</summary>
    public static readonly IReadOnlyList<SatTleSource> TleSources = new[]
    {
        new SatTleSource("AMSAT (nasabare)", "http://www.amsat.org/tle/current/nasabare.txt"),
        new SatTleSource("CSN nasabare", "http://www.csntechnologies.net/SAT/nasabare.txt"),
        new SatTleSource("CSN bare", "http://www.csntechnologies.net/SAT/csnbare.txt"),
        new SatTleSource("CSN active", "http://www.csntechnologies.net/SAT/csnactive.txt"),
    };

    /// <summary>Fetch and parse the controller's configured transponder table (/f.txt).</summary>
    public async Task<IReadOnlyList<SatConfiguredTransponder>> GetConfiguredTranspondersAsync(CancellationToken ct = default)
    {
        var ip = await GetControllerIpAsync();
        if (string.IsNullOrWhiteSpace(ip)) return Array.Empty<SatConfiguredTransponder>();
        try
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(5);
            var text = await client.GetStringAsync($"http://{ip}/f.txt", ct);
            return ParseFreqTable(text);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read configured sats from S.A.T. controller");
            return Array.Empty<SatConfiguredTransponder>();
        }
    }

    /// <summary>Tell the controller to pull fresh TLEs from a source URL (http only).</summary>
    public async Task<bool> UpdateTleAsync(string sourceUrl, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(sourceUrl)) return false;
        if (sourceUrl.StartsWith("https", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning("S.A.T. TLE update rejected: controller does not support HTTPS URLs");
            return false;
        }
        return await SendControllerCommandAsync($"Y|{sourceUrl}", ct);
    }

    /// <summary>Tell the controller to update its frequency database from the internet.</summary>
    public Task<bool> UpdateFreqDbAsync(CancellationToken ct = default)
        => SendControllerCommandAsync("Z|f|", ct);

    // -- internals ----------------------------------------------------------

    // Fixed-width columns of /f.txt, verified against a live controller:
    //   [0,5)   catalog number
    //   [13,24) uplink   — a 2-char prefix then the Hz ("100" = no uplink)
    //   [25,34) downlink — Hz
    //   [43,51) uplink mode      [51,59) downlink mode      [59,87) transponder name
    private static IReadOnlyList<SatConfiguredTransponder> ParseFreqTable(string text)
    {
        var list = new List<SatConfiguredTransponder>();
        foreach (var raw in text.Split('\n'))
        {
            var line = raw.TrimEnd('\r');
            if (line.Length < 34) continue;
            var catno = line[..5].Trim();
            if (catno.Length == 0 || !catno.All(char.IsDigit)) continue;

            var up = Field(line, 13, 24).Trim();
            var uplinkHz = up.Length > 2 && long.TryParse(up[2..], out var u) ? u : 0;
            var downlinkHz = long.TryParse(Field(line, 25, 34).Trim(), out var d) ? d : 0;

            list.Add(new SatConfiguredTransponder(
                CatalogNumber: catno,
                UplinkHz: uplinkHz,
                DownlinkHz: downlinkHz,
                UplinkMode: Field(line, 43, 51).Trim(),
                DownlinkMode: Field(line, 51, 59).Trim(),
                Name: Field(line, 59, 87).Trim()));
        }
        return list;
    }

    private static string Field(string s, int start, int end)
        => start >= s.Length ? "" : s[start..Math.Min(end, s.Length)];

    private async Task<bool> SendControllerCommandAsync(string command, CancellationToken ct)
    {
        var ip = await GetControllerIpAsync();
        if (string.IsNullOrWhiteSpace(ip)) return false;
        try
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(15);
            // The controller URL-decodes the query, so encoding the pipe-delimited
            // command is safe and matches how the web UI's XHR sends it.
            var resp = await client.GetAsync($"http://{ip}/cmd?a={Uri.EscapeDataString(command)}", ct);
            if (resp.IsSuccessStatusCode) return true;
            _logger.LogWarning("S.A.T. command '{Command}' returned {Status}", command, (int)resp.StatusCode);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "S.A.T. command '{Command}' failed", command);
            return false;
        }
    }

    private async Task<string?> GetControllerIpAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settings = await scope.ServiceProvider.GetRequiredService<ISettingsRepository>().GetAsync();
        return settings?.Sat?.ControllerIp;
    }
}
