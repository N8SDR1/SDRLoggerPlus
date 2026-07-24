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

/// <summary>
/// Reads the CSN S.A.T. controller's configured transponder table (HTTP GET /f.txt,
/// a fixed-width table). Changing the controller's state — TLE / frequency-database
/// updates, picking a satellite to track — is deliberately NOT done here: SDRLogger+
/// embeds the controller's own web UI (S.A.T. Web panel), which does all of that
/// natively with the operator's real data and no undocumented command guesswork.
/// </summary>
public partial class SatControllerService
{
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

    private async Task<string?> GetControllerIpAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settings = await scope.ServiceProvider.GetRequiredService<ISettingsRepository>().GetAsync();
        return settings?.Sat?.ControllerIp;
    }
}
