using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// SDRLogger+-ported award trackers: WAS, WAZ, WPX, WAC, 5BWAS, 5BDXCC.
/// All counting is worked-based (no QSL confirmation gating) so totals match
/// what SDRLogger+ showed. See docs/design/awards-dashboards-design.md.
/// </summary>
public partial class AwardsService
{
    private static readonly string[] FiveBandBands = ["80m", "40m", "20m", "15m", "10m"];

    private static readonly string[] WacBaseContinents = ["NA", "SA", "EU", "AS", "AF", "OC"];
    private static readonly string[] WacExtraContinents = ["AN"];

    private static readonly Dictionary<string, string> WacContinentNames = new()
    {
        ["NA"] = "North America", ["SA"] = "South America", ["EU"] = "Europe",
        ["AS"] = "Asia", ["AF"] = "Africa", ["OC"] = "Oceania", ["AN"] = "Antarctica",
    };

    public async Task<WasStatistics> GetWasStatisticsAsync(StatisticsFilters? filters = null)
    {
        var qsos = await GetFilteredQsosAsync(filters);

        var states = new Dictionary<string, (Dictionary<string, HashSet<string>> Bands, int QsoCount)>();
        foreach (var q in qsos)
        {
            var state = ResolveUsState(q);
            if (state == null) continue;

            if (!states.TryGetValue(state, out var entry))
            {
                entry = (new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase), 0);
                states[state] = entry;
            }
            AddBandMode(entry.Bands, q);
            states[state] = (entry.Bands, entry.QsoCount + 1);
        }

        var list = states
            .OrderBy(kv => kv.Key)
            .Select(kv => new WasStateStatus(kv.Key, ToBandModeLists(kv.Value.Bands), kv.Value.QsoCount))
            .ToList();

        return new WasStatistics(list.Count, 50, list);
    }

    public async Task<WazStatistics> GetWazStatisticsAsync(StatisticsFilters? filters = null)
    {
        var qsos = await GetFilteredQsosAsync(filters);

        var zones = new Dictionary<int, (Dictionary<string, HashSet<string>> Bands, HashSet<string> Entities)>();
        foreach (var q in qsos)
        {
            var zone = q.Station?.CqZone ?? CtyService.GetEntityFromCallsign(NormalizeCall(q)).CqZone;
            if (zone is null or <= 0 or > 40) continue;

            if (!zones.TryGetValue(zone.Value, out var entry))
            {
                entry = (new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase),
                         new HashSet<string>(StringComparer.OrdinalIgnoreCase));
                zones[zone.Value] = entry;
            }
            AddBandMode(entry.Bands, q);
            var country = ResolveCountry(q);
            if (country != null) entry.Entities.Add(country);
        }

        var list = zones
            .OrderBy(kv => kv.Key)
            .Select(kv => new WazZoneStatus(kv.Key, ToBandModeLists(kv.Value.Bands),
                kv.Value.Entities.OrderBy(e => e).Take(10).ToList()))
            .ToList();

        return new WazStatistics(list.Count, 40, list);
    }

    public async Task<WpxStatistics> GetWpxStatisticsAsync(StatisticsFilters? filters = null)
    {
        var qsos = await GetFilteredQsosAsync(filters);

        var prefixes = new Dictionary<string, (Dictionary<string, HashSet<string>> Bands, HashSet<string> Calls)>(StringComparer.OrdinalIgnoreCase);
        foreach (var q in qsos)
        {
            var pfx = WpxPrefixExtractor.Extract(q.Callsign ?? "");
            if (pfx == null) continue;

            if (!prefixes.TryGetValue(pfx, out var entry))
            {
                entry = (new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase),
                         new HashSet<string>(StringComparer.OrdinalIgnoreCase));
                prefixes[pfx] = entry;
            }
            AddBandMode(entry.Bands, q);
            entry.Calls.Add(NormalizeCall(q));
        }

        var list = prefixes
            .OrderBy(kv => kv.Key)
            .Select(kv => new WpxPrefixStatus(kv.Key, ToBandModeLists(kv.Value.Bands),
                kv.Value.Calls.OrderBy(c => c).Take(5).ToList(), kv.Value.Bands.Count))
            .ToList();

        return new WpxStatistics(list.Count, list);
    }

    public async Task<WacStatistics> GetWacStatisticsAsync(StatisticsFilters? filters = null)
    {
        var qsos = await GetFilteredQsosAsync(filters);

        var conts = new Dictionary<string, (Dictionary<string, HashSet<string>> Bands, HashSet<string> Entities)>(StringComparer.OrdinalIgnoreCase);
        foreach (var q in qsos)
        {
            var cont = q.Continent
                ?? q.Station?.Continent
                ?? CtyService.GetEntityFromCallsign(NormalizeCall(q)).Continent;
            if (string.IsNullOrWhiteSpace(cont)) continue;
            cont = cont.Trim().ToUpperInvariant();

            if (!conts.TryGetValue(cont, out var entry))
            {
                entry = (new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase),
                         new HashSet<string>(StringComparer.OrdinalIgnoreCase));
                conts[cont] = entry;
            }
            AddBandMode(entry.Bands, q);
            var country = ResolveCountry(q);
            if (country != null) entry.Entities.Add(country);
        }

        var statuses = new List<WacContinentStatus>();
        foreach (var code in WacBaseContinents.Concat(WacExtraContinents))
        {
            if (!conts.TryGetValue(code, out var entry)) continue;
            statuses.Add(new WacContinentStatus(
                code,
                WacContinentNames[code],
                WacExtraContinents.Contains(code),
                ToBandModeLists(entry.Bands),
                entry.Entities.OrderBy(e => e).Take(8).ToList()));
        }

        var baseWorked = statuses.Count(s => !s.IsExtra);
        var extraWorked = statuses.Count(s => s.IsExtra);
        return new WacStatistics(baseWorked, extraWorked, baseWorked >= 6, statuses);
    }

    public Task<FiveBandStatistics> Get5BWasStatisticsAsync(string? mode = null)
        => GetFiveBandAsync(mode, threshold: 50, ResolveUsState);

    public Task<FiveBandStatistics> Get5BDxccStatisticsAsync(string? mode = null)
        => GetFiveBandAsync(mode, threshold: 100, ResolveCountry);

    /// <summary>Shared 5-band counting: distinct items (states/entities) per award band.</summary>
    private async Task<FiveBandStatistics> GetFiveBandAsync(string? mode, int threshold, Func<Qso, string?> itemSelector)
    {
        var allQsos = await AllQsosAsync();

        var bands = FiveBandBands.ToDictionary(
            b => b, _ => new HashSet<string>(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);

        foreach (var q in allQsos)
        {
            var band = q.Band?.Trim().ToLowerInvariant();
            if (band == null || !bands.TryGetValue(band, out var set)) continue;
            if (!string.IsNullOrEmpty(mode) &&
                !string.Equals(q.Mode, mode, StringComparison.OrdinalIgnoreCase)) continue;

            var item = itemSelector(q);
            if (item != null) set.Add(item);
        }

        var union = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var statuses = new List<FiveBandBandStatus>();
        foreach (var b in FiveBandBands)
        {
            var items = bands[b].OrderBy(s => s).ToList();
            union.UnionWith(items);
            statuses.Add(new FiveBandBandStatus(b, items.Count, threshold, items.Count >= threshold, items));
        }

        return new FiveBandStatistics(statuses.All(s => s.Achieved), union.Count, statuses);
    }

    // ─── helpers ─────────────────────────────────────────────────────────

    private async Task<List<Qso>> GetFilteredQsosAsync(StatisticsFilters? filters)
    {
        var qsos = (await AllQsosAsync()).ToList();
        if (filters == null) return qsos;

        if (!string.IsNullOrEmpty(filters.Band))
            qsos = qsos.Where(q => string.Equals(q.Band, filters.Band, StringComparison.OrdinalIgnoreCase)).ToList();
        if (!string.IsNullOrEmpty(filters.Mode))
            qsos = qsos.Where(q => string.Equals(q.Mode, filters.Mode, StringComparison.OrdinalIgnoreCase)).ToList();
        return qsos;
    }

    private static string NormalizeCall(Qso q) => (q.Callsign ?? "").Trim().ToUpperInvariant();

    private static string? ResolveCountry(Qso q) =>
        !string.IsNullOrWhiteSpace(q.Country)
            ? q.Country
            : CtyService.GetEntityFromCallsign(NormalizeCall(q)).Country;

    private static string? ResolveUsState(Qso q) =>
        UsStateResolver.Resolve(q.Station?.State, q.Station?.Qth, ResolveCountry(q));

    private static void AddBandMode(Dictionary<string, HashSet<string>> bands, Qso q)
    {
        var band = q.Band?.Trim().ToLowerInvariant();
        if (string.IsNullOrEmpty(band)) return;
        if (!bands.TryGetValue(band, out var modes))
        {
            modes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            bands[band] = modes;
        }
        var mode = q.Mode?.Trim().ToUpperInvariant();
        if (!string.IsNullOrEmpty(mode)) modes.Add(mode);
    }

    private static Dictionary<string, List<string>> ToBandModeLists(Dictionary<string, HashSet<string>> bands) =>
        bands.ToDictionary(kv => kv.Key, kv => kv.Value.OrderBy(m => m).ToList());
}
