using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Host-side "who's on what band+mode" board for multi-op (S-COORD). Each station reports its band/mode
/// on change; a newly-joined station fetches the current board. Entries that stop refreshing are pruned
/// as stale (the station left / powered off), so the RF-collision view doesn't warn about a rig that's
/// no longer on. Thread-safe singleton.
/// </summary>
public sealed class StationPresenceTracker
{
    private static readonly TimeSpan StaleAfter = TimeSpan.FromMinutes(15);

    private readonly object _gate = new();
    private readonly Dictionary<string, StationPresenceEvent> _byStation = new(StringComparer.OrdinalIgnoreCase);

    public void Update(StationPresenceEvent presence)
    {
        lock (_gate) _byStation[presence.StationId] = presence;
    }

    /// <summary>Current presence across the group, minus anything gone stale.</summary>
    public IReadOnlyList<StationPresenceEvent> Current()
    {
        var cutoff = DateTime.UtcNow - StaleAfter;
        lock (_gate) return _byStation.Values.Where(p => p.UpdatedUtc >= cutoff).ToList();
    }
}
