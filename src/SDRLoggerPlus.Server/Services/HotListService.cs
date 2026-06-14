using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Hubs;
using Microsoft.AspNetCore.SignalR;

namespace SDRLoggerPlus.Server.Services;

public interface IHotListService
{
    bool IsHot(string? callsign);
    Task<IReadOnlyCollection<string>> GetCallsignsAsync();
    Task AddAsync(IEnumerable<string> callsigns);
    Task RemoveAsync(string callsign);
    Task ClearAsync();
    Task SetFlagsAsync(bool? enabled, bool? ttsEnabled);
    Task ReloadAsync();
}

/// <summary>
/// Watched-callsign Hot List (SDRLogger+ port). Holds the watch set in
/// memory for fast spot matching; mutations persist to UserSettings and
/// broadcast HotListChanged so all panels stay in sync (replaces
/// SDRLogger+'s BroadcastChannel cross-tab mechanism).
/// </summary>
public class HotListService : IHotListService, IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IHubContext<LogHub, ILogHubClient> _hubContext;
    private readonly ILogger<HotListService> _logger;

    private HashSet<string> _callsigns = new(StringComparer.OrdinalIgnoreCase);
    private bool _enabled;
    private readonly object _lock = new();

    public HotListService(
        IServiceProvider serviceProvider,
        IHubContext<LogHub, ILogHubClient> hubContext,
        ILogger<HotListService> logger)
    {
        _serviceProvider = serviceProvider;
        _hubContext = hubContext;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await ReloadAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Hot list initial load failed (database may not be ready)");
            }
        }, cancellationToken);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public bool IsHot(string? callsign)
    {
        if (string.IsNullOrWhiteSpace(callsign)) return false;
        lock (_lock)
        {
            return _enabled && _callsigns.Contains(callsign.Trim());
        }
    }

    public async Task<IReadOnlyCollection<string>> GetCallsignsAsync()
    {
        var settings = await GetSettingsAsync();
        return settings.HotList.Callsigns.AsReadOnly();
    }

    public async Task AddAsync(IEnumerable<string> callsigns)
    {
        await MutateAsync(hotList =>
        {
            foreach (var raw in callsigns)
            {
                var call = raw.Trim().ToUpperInvariant();
                if (call.Length > 0 && !hotList.Callsigns.Contains(call))
                    hotList.Callsigns.Add(call);
            }
        });
    }

    public async Task RemoveAsync(string callsign)
    {
        var call = callsign.Trim().ToUpperInvariant();
        await MutateAsync(hotList => hotList.Callsigns.RemoveAll(c => c == call));
    }

    public Task ClearAsync() => MutateAsync(hotList => hotList.Callsigns.Clear());

    public async Task SetFlagsAsync(bool? enabled, bool? ttsEnabled)
    {
        await MutateAsync(hotList =>
        {
            if (enabled.HasValue) hotList.Enabled = enabled.Value;
            if (ttsEnabled.HasValue) hotList.TtsEnabled = ttsEnabled.Value;
        });
    }

    public async Task ReloadAsync()
    {
        var settings = await GetSettingsAsync();
        lock (_lock)
        {
            _enabled = settings.HotList.Enabled;
            _callsigns = new HashSet<string>(
                settings.HotList.Callsigns.Select(c => c.Trim().ToUpperInvariant()).Where(c => c.Length > 0),
                StringComparer.OrdinalIgnoreCase);
        }
    }

    private async Task MutateAsync(Action<Contracts.Models.HotListSettings> mutate)
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        var settings = await settingsService.GetSettingsAsync();
        mutate(settings.HotList);
        await settingsService.SaveSettingsAsync(settings);
        await ReloadAsync();
        await _hubContext.Clients.All.OnHotListChanged(new HotListChangedEvent(
            settings.HotList.Enabled,
            settings.HotList.TtsEnabled,
            settings.HotList.Callsigns));
    }

    private async Task<Contracts.Models.UserSettings> GetSettingsAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        return await settingsService.GetSettingsAsync();
    }
}
