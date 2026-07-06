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

        // Over cap: prioritise local strikes, then nearest-to-station (newest as
        // the tiebreak / no-station fallback), and hard-trim to _cap. Locals are
        // kept ahead of globals but are themselves trimmed if they alone exceed
        // the cap — otherwise a dense local storm could grow the buffer without
        // bound, defeating the cap during exactly the high-load case it guards.
        var hasStation = stationLat.HasValue && stationLon.HasValue;
        var kept = _byKey.Values
            .OrderByDescending(s => s.Local)
            .ThenBy(s => hasStation
                ? PropagationService.HaversineDistanceKm(stationLat!.Value, stationLon!.Value, s.Lat, s.Lon)
                : 0)
            .ThenByDescending(s => s.TimestampUtc)
            .Take(_cap)
            .ToList();
        _byKey.Clear();
        foreach (var s in kept) _byKey[Key(s)] = s;
    }
}
