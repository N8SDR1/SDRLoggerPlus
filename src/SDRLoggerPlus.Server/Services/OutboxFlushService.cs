using System.Net.Http.Headers;
using System.Net.Http.Json;
using SDRLoggerPlus.Server.Core.Database.Remote;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Drains the <see cref="RemoteWriteOutbox"/> on a field CLIENT: every few seconds, if there are QSOs
/// that were logged while the host was unreachable, it re-sends each to the host and removes it on
/// success. Safe to re-send because the host's create is idempotent on the origin-minted Id (S2), so a
/// QSO that actually landed (but whose ack was lost) is not double-logged. Registered only in client
/// mode. Uses a plain HttpClient built from the saved host URL/token, with the shared JSON options so
/// AdifExtra survives.
/// </summary>
public sealed class OutboxFlushService : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    private readonly RemoteWriteOutbox _outbox;
    private readonly IUserConfigService _config;
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger<OutboxFlushService> _log;

    public OutboxFlushService(RemoteWriteOutbox outbox, IUserConfigService config,
        IHttpClientFactory httpFactory, ILogger<OutboxFlushService> log)
    {
        _outbox = outbox;
        _config = config;
        _httpFactory = httpFactory;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (_outbox.Count > 0) await FlushOnce(stoppingToken);
            }
            catch (Exception ex)
            {
                _log.LogDebug("Outbox flush cycle error: {Msg}", ex.Message);
            }

            try { await Task.Delay(Interval, stoppingToken); } catch { }
        }
    }

    private async Task FlushOnce(CancellationToken ct)
    {
        var cfg = await _config.GetConfigAsync();
        if (string.IsNullOrWhiteSpace(cfg.HostUrl)) return;

        var client = _httpFactory.CreateClient();
        client.BaseAddress = new Uri(cfg.HostUrl.TrimEnd('/') + "/");
        client.Timeout = TimeSpan.FromSeconds(15);
        if (!string.IsNullOrWhiteSpace(cfg.HostToken))
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", cfg.HostToken);

        foreach (var qso in _outbox.Snapshot())
        {
            if (ct.IsCancellationRequested) return;
            try
            {
                var res = await client.PostAsJsonAsync("api/data/qsos", qso, RemoteJson.Options, ct);
                if (res.IsSuccessStatusCode)
                {
                    _outbox.Remove(qso.Id);
                    _log.LogInformation("Outbox: QSO {Id} synced to the host.", qso.Id);
                }
                // A reached-but-errored response (e.g. bad data) is left in the outbox rather than lost;
                // the operator sees the queue not draining. Dead-lettering is a later refinement.
            }
            catch
            {
                // Host still unreachable — stop this cycle and retry the whole queue next time.
                return;
            }
        }
    }
}
