using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services.Weather;

/// <summary>
/// Pure rolling buffer of lightning strikes. Dedupes, prunes by age (local window
/// vs. global window), and caps total size by trimming the farthest-from-station
/// strikes first while always keeping local ones. No IO, no timers — unit-tested.
/// </summary>
public sealed class StrikeBuffer
{
    private readonly TimeSpan _localWindow;
    private readonly TimeSpan _globalWindow;
    private readonly int _cap;
    private readonly Dictionary<string, LightningStrike> _byKey = new();

    public StrikeBuffer(TimeSpan localWindow, TimeSpan globalWindow, int cap)
    {
        _localWindow = localWindow;
        _globalWindow = globalWindow;
        _cap = cap;
    }

    private static string Key(LightningStrike s) =>
        $"{s.Lat:F4}|{s.Lon:F4}|{s.TimestampUtc.Ticks}";

    /// <summary>Add strikes; returns only the ones not already present (for pushing).</summary>
    public IReadOnlyList<LightningStrike> Add(IEnumerable<LightningStrike> incoming, DateTime nowUtc, double? stationLat, double? stationLon)
    {
        var added = new List<LightningStrike>();
        foreach (var s in incoming)
        {
            var key = Key(s);
            if (_byKey.ContainsKey(key)) continue;
            _byKey[key] = s;
            added.Add(s);
        }
        Prune(nowUtc, stationLat, stationLon);
        return added;
    }

    /// <summary>Current pruned snapshot (does not mutate).</summary>
    public IReadOnlyList<LightningStrike> Current(DateTime nowUtc)
    {
        return _byKey.Values.Where(s => !IsExpired(s, nowUtc)).ToList();
    }

    private bool IsExpired(LightningStrike s, DateTime nowUtc)
    {
        var window = s.Local ? _localWindow : _globalWindow;
        return nowUtc - s.TimestampUtc > window;
    }

    private void Prune(DateTime nowUtc, double? stationLat, double? stationLon)
    {
        // Drop expired.
        foreach (var kv in _byKey.Where(kv => IsExpired(kv.Value, nowUtc)).ToList())
            _byKey.Remove(kv.Key);

        if (_byKey.Count <= _cap) return;

        // Over cap: keep all local; among globals keep the nearest to the station.
        var locals = _byKey.Values.Where(s => s.Local).ToList();
        var globals = _byKey.Values.Where(s => !s.Local).ToList();
        if (stationLat.HasValue && stationLon.HasValue)
            globals = globals.OrderBy(s => Haversine(stationLat.Value, stationLon.Value, s.Lat, s.Lon)).ToList();
        else
            globals = globals.OrderByDescending(s => s.TimestampUtc).ToList();

        var keepGlobals = Math.Max(0, _cap - locals.Count);
        var kept = locals.Concat(globals.Take(keepGlobals));
        _byKey.Clear();
        foreach (var s in kept) _byKey[Key(s)] = s;
    }

    private static double Haversine(double lat1, double lon1, double lat2, double lon2)
    {
        const double R = 6371;
        double ToRad(double d) => d * Math.PI / 180;
        var dLat = ToRad(lat2 - lat1);
        var dLon = ToRad(lon2 - lon1);
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2)
              + Math.Cos(ToRad(lat1)) * Math.Cos(ToRad(lat2)) * Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        return R * 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
    }
}
