using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Runs on a field CLIENT (DatabaseProvider.RemoteHost). Connects to the HOST's SignalR hub as a
/// client and relays live QSO events to this machine's own hub — so the local UI, which is wired only
/// to its own backend, refetches the shared log and sees contacts other stations just logged (S1 item 2,
/// "live event bus"). Registered only in client mode, so a host/normal install never starts it.
///
/// Dedup-free by design: the frontend treats OnQsoLogged as "refetch the shared log", so a doubled
/// event is harmless. Auto-reconnects; a missed window is covered because dupe/score also query the
/// shared log on demand at log time.
/// </summary>
public sealed class HostBridgeService : BackgroundService
{
    private readonly IUserConfigService _config;
    private readonly IHubContext<LogHub, ILogHubClient> _localHub;
    private readonly ILogger<HostBridgeService> _log;
    private HubConnection? _conn;

    public HostBridgeService(IUserConfigService config,
        IHubContext<LogHub, ILogHubClient> localHub, ILogger<HostBridgeService> log)
    {
        _config = config;
        _localHub = localHub;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var cfg = await _config.GetConfigAsync();
        if (string.IsNullOrWhiteSpace(cfg.HostUrl)) return;
        var hubUrl = $"{cfg.HostUrl.TrimEnd('/')}/hubs/log";

        _conn = new HubConnectionBuilder()
            .WithUrl(hubUrl, options =>
            {
                // Same token scheme the host's TokenAuthMiddleware reads from ?access_token= on /hubs.
                if (!string.IsNullOrWhiteSpace(cfg.HostToken))
                    options.AccessTokenProvider = () => Task.FromResult<string?>(cfg.HostToken);
            })
            .WithAutomaticReconnect()
            .Build();

        // Relay the host's new-QSO events to our own frontend, which then refetches the shared log.
        _conn.On<QsoLoggedEvent>("OnQsoLogged", async evt =>
        {
            try { await _localHub.BroadcastQso(evt); }
            catch (Exception ex) { _log.LogDebug("Relay of host QSO event failed: {Msg}", ex.Message); }
        });

        _conn.Reconnected += _ => { _log.LogInformation("Reconnected to host hub {Url}", hubUrl); return Task.CompletedTask; };

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _conn.StartAsync(stoppingToken);
                _log.LogInformation("Connected to host hub {Url} — live QSO relay active.", hubUrl);
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                _log.LogWarning("Host hub not reachable yet ({Msg}); retrying in 5s.", ex.Message);
                try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); } catch { }
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_conn is not null) await _conn.DisposeAsync();
        await base.StopAsync(cancellationToken);
    }
}
