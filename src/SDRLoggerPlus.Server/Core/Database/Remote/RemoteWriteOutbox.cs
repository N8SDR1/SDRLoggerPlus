using System.Text.Json;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Serialization;

namespace SDRLoggerPlus.Server.Core.Database.Remote;

/// <summary>Shared JSON options for the remote-data path: web defaults (camelCase, matching the API)
/// plus the BsonDocument converter so Qso.AdifExtra round-trips faithfully.</summary>
public static class RemoteJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new BsonDocumentJsonConverter() }
    };
}

/// <summary>
/// A durable queue of QSOs a field CLIENT logged while its host was unreachable (S3 offline resilience).
/// When a create can't reach the host, it's enqueued here and returned to the operator as logged; the
/// <c>OutboxFlushService</c> re-sends each one when the host comes back — safe to re-send because the
/// host's create is idempotent on the origin-minted Id (S2), so a retry never double-logs.
///
/// Persisted as a small JSON file beside the config (same pattern as AuthTokenStore), so a blip that
/// spans a restart still doesn't lose the contact. Deduped on QSO Id so the same QSO is queued once.
/// </summary>
public sealed class RemoteWriteOutbox
{
    private readonly string _path;
    private readonly object _gate = new();
    private List<Qso> _pending;

    public RemoteWriteOutbox(string path)
    {
        _path = path;
        _pending = Load();
    }

    public int Count { get { lock (_gate) return _pending.Count; } }

    public void Enqueue(Qso qso)
    {
        lock (_gate)
        {
            if (_pending.Any(q => q.Id == qso.Id)) return; // already queued
            _pending.Add(qso);
            Save();
        }
    }

    public IReadOnlyList<Qso> Snapshot()
    {
        lock (_gate) return _pending.ToList();
    }

    public void Remove(string id)
    {
        lock (_gate)
        {
            if (_pending.RemoveAll(q => q.Id == id) > 0) Save();
        }
    }

    private List<Qso> Load()
    {
        try
        {
            if (!File.Exists(_path)) return new();
            return JsonSerializer.Deserialize<List<Qso>>(File.ReadAllText(_path), RemoteJson.Options) ?? new();
        }
        catch
        {
            // A corrupt outbox must not crash the client; worst case a few un-acked QSOs need re-logging,
            // which the operator's own USB/ADIF copies (S6) still cover.
            return new();
        }
    }

    private void Save()
    {
        // caller holds _gate
        var dir = Path.GetDirectoryName(_path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        File.WriteAllText(_path, JsonSerializer.Serialize(_pending, RemoteJson.Options));
    }
}
