using System.Text.Json;
using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Host-side atomic serial allocator for multi-op shared logging (S5). Every field station requests its
/// next contest serial from HERE (over the API) instead of a local counter, so a fast op and a slow op
/// drawing from one shared sequence never collide — the one thing N1MM has to peer-sync, we get from one
/// authoritative counter.
///
/// Counters are keyed by (contestId, scope): the band for a PerBand contest, or "all" for an AllBand
/// one — the same semantics <see cref="ContestSerials"/> uses per-session, just centralized. Persisted
/// beside the config so a host restart continues the sequence rather than restarting it (which would
/// hand out already-used numbers). Thread-safe; the host process serializes access.
/// </summary>
public sealed class SharedSerialAllocator
{
    private readonly string _path;
    private readonly object _gate = new();
    private Dictionary<string, int> _next;

    public SharedSerialAllocator(string path)
    {
        _path = path;
        _next = Load();
    }

    /// <summary>Return the next serial for this contest/band and advance the shared counter.</summary>
    public int Next(string contestId, SerialMode mode, string band)
    {
        if (mode == SerialMode.None) return 0;
        var key = Key(contestId, mode, band);
        lock (_gate)
        {
            var cur = _next.TryGetValue(key, out var n) ? n : 1;
            _next[key] = cur + 1;
            Save();
            return cur;
        }
    }

    /// <summary>The serial the next request would get, without advancing.</summary>
    public int Peek(string contestId, SerialMode mode, string band)
    {
        if (mode == SerialMode.None) return 0;
        lock (_gate) return _next.TryGetValue(Key(contestId, mode, band), out var n) ? n : 1;
    }

    private static string Key(string contestId, SerialMode mode, string band) =>
        (mode == SerialMode.PerBand ? $"{contestId}|{band}" : $"{contestId}|all").ToLowerInvariant();

    private Dictionary<string, int> Load()
    {
        try
        {
            return File.Exists(_path)
                ? JsonSerializer.Deserialize<Dictionary<string, int>>(File.ReadAllText(_path)) ?? new()
                : new();
        }
        catch
        {
            // A corrupt file must not stop the contest; worst case the sequence restarts (rare, and the
            // operator would see it). Failing to an empty set is safer than crashing mid-run.
            return new();
        }
    }

    private void Save()
    {
        // caller holds _gate
        var dir = Path.GetDirectoryName(_path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        File.WriteAllText(_path, JsonSerializer.Serialize(_next));
    }
}
