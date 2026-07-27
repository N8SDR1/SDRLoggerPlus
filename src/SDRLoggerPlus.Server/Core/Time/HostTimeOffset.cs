namespace SDRLoggerPlus.Server.Core.Time;

/// <summary>
/// This machine's clock offset from the multi-op HOST (S4). The host is the time authority so every
/// station agrees even with no internet (Field Day): a client measures how far its own clock is from
/// the host (SNTP-style) and timestamps are corrected into the host's frame. On the host itself
/// <see cref="IsHost"/> stays true and the offset is zero.
///
/// Thread-safe singleton; written by <c>HostTimeSyncService</c> (client mode), read by the time API,
/// the logging path, and the UI sync indicator.
/// </summary>
public sealed class HostTimeOffset
{
    private readonly object _gate = new();
    private TimeSpan _offset = TimeSpan.Zero;
    private DateTime? _lastSyncUtc;

    /// <summary>True until this machine syncs to a host — i.e. it IS the authority (host / standalone).</summary>
    public bool IsHost { get; set; } = true;

    public TimeSpan Offset { get { lock (_gate) return _offset; } }
    public DateTime? LastSyncUtc { get { lock (_gate) return _lastSyncUtc; } }

    public void Update(TimeSpan offset)
    {
        lock (_gate)
        {
            _offset = offset;
            _lastSyncUtc = DateTime.UtcNow;
        }
        IsHost = false;
    }

    /// <summary>Current UTC corrected into the host's time frame (local clock + measured offset).</summary>
    public DateTime NowInHostFrame() => DateTime.UtcNow + Offset;
}
