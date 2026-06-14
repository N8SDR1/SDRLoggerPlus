using System.Net.Sockets;
using System.Text;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services.Weather;

namespace SDRLoggerPlus.Server.Services.BandOpening;

/// <summary>
/// RBN band-opening alerts (SDRLogger+ port). Maintains its own telnet session
/// to the Reverse Beacon Network (separate concern from RbnService's cluster
/// spots): logs in with the station callsign, filters to the selected VHF/UHF
/// bands with DXSpider accept/reject commands, and for each spot resolves the
/// SKIMMER's grid (QRZ, cached incl. negatives). When a skimmer within the
/// configured distance of the station hears anyone on a selected band — the
/// band is open here — a per-band-cooldown-gated SignalR event fires (toast +
/// optional voice on the frontend).
/// </summary>
public class BandOpeningService : BackgroundService
{
    private readonly ILogger<BandOpeningService> _logger;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;

    private readonly Dictionary<string, string?> _gridCache = new(); // normalized skimmer → grid (null = lookup failed)
    private readonly BandCooldownGate _cooldown = new();

    public BandOpeningService(ILogger<BandOpeningService> logger, IServiceScopeFactory scopeFactory,
        IHubContext<LogHub, ILogHubClient> hub)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _hub = hub;
    }

    private async Task<UserSettings> GetSettingsAsync()
    {
        using var scope = _scopeFactory.CreateScope();
        return await scope.ServiceProvider.GetRequiredService<ISettingsService>().GetSettingsAsync();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Band-opening alert service starting");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await GetSettingsAsync();
                var rbn = settings.RbnAlerts;
                var callsign = settings.Station.Callsign;

                if (!rbn.Enabled || string.IsNullOrWhiteSpace(callsign))
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                    continue;
                }

                await RunSessionAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Band-opening session error: {Message}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken); // reconnect backoff
        }
    }

    private async Task RunSessionAsync(CancellationToken ct)
    {
        var settings = await GetSettingsAsync();
        var rbn = settings.RbnAlerts;
        var callsign = settings.Station.Callsign!.ToUpperInvariant();

        var bands = SelectedBands(rbn);
        if (bands.Count == 0) return;

        _logger.LogInformation("Band-opening: connecting to {Server}:{Port} as {Call} for {Bands}",
            rbn.Server, rbn.Port, callsign, string.Join(',', bands));

        using var client = new TcpClient();
        await client.ConnectAsync(rbn.Server ?? "telnet.reversebeacon.net", rbn.Port, ct);
        using var stream = client.GetStream();
        using var reader = new StreamReader(stream, Encoding.ASCII);
        using var writer = new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true };

        var loggedIn = false;
        var buffer = new char[4096];
        var lineBuf = new StringBuilder();

        while (!ct.IsCancellationRequested)
        {
            // Re-read settings each pass so disable/band changes take effect
            var current = (await GetSettingsAsync()).RbnAlerts;
            if (!current.Enabled) return;

            var read = await reader.ReadAsync(buffer.AsMemory(0, buffer.Length), ct);
            if (read == 0) throw new IOException("RBN closed the connection");

            for (var i = 0; i < read; i++)
            {
                var ch = buffer[i];

                if (!loggedIn)
                {
                    lineBuf.Append(ch);
                    var sofar = lineBuf.ToString().ToLowerInvariant();
                    if (sofar.Contains("call") || sofar.Contains("login"))
                    {
                        await writer.WriteAsync(callsign + "\r\n");
                        loggedIn = true;
                        lineBuf.Clear();

                        // DXSpider filters: only our alert bands (faithful SDRLogger+ sequence)
                        await Task.Delay(1000, ct);
                        await writer.WriteAsync("set/noskimmer\r\n");
                        await Task.Delay(300, ct);
                        await writer.WriteAsync("reject/spots all\r\n");
                        await Task.Delay(300, ct);
                        var n = 1;
                        foreach (var band in bands)
                        {
                            var (lo, hi) = BandOpeningLogic.AlertBands[band];
                            await writer.WriteAsync($"accept/spots {n} on freq {lo:F0}/{hi:F0}\r\n");
                            await Task.Delay(200, ct);
                            n++;
                        }
                        _logger.LogInformation("Band-opening: logged in, filters set");
                    }
                    continue;
                }

                if (ch is '\r' or '\n')
                {
                    var line = lineBuf.ToString();
                    lineBuf.Clear();
                    if (line.Length > 0)
                        await ProcessLineAsync(line, current, ct);
                }
                else
                {
                    lineBuf.Append(ch);
                }
            }
        }
    }

    private async Task ProcessLineAsync(string line, RbnAlertSettings rbn, CancellationToken ct)
    {
        var spot = BandOpeningLogic.ParseSpotLine(line);
        if (spot is null) return;

        var band = BandOpeningLogic.AlertBandFor(spot.FrequencyKhz);
        if (band is null || !SelectedBands(rbn).Contains(band)) return;

        var settings = await GetSettingsAsync();
        var stationPos = StationPosition(settings.Station);
        if (stationPos is null) return; // no grid/coords — can't compute distance

        var skimmerGrid = await ResolveSkimmerGridAsync(spot.Skimmer);
        if (skimmerGrid is null) return;
        var skimmerPos = GeoMath.GridToLatLon(skimmerGrid);
        if (skimmerPos is null) return;

        var distKm = BandOpeningLogic.HaversineKm(stationPos.Value.Lat, stationPos.Value.Lon,
            skimmerPos.Value.Lat, skimmerPos.Value.Lon);
        var dist = rbn.DistanceUnit == "km" ? distKm : distKm * 0.621371;
        if (dist > Math.Max(1, rbn.Distance)) return;

        if (!_cooldown.ShouldAlert(band, Math.Max(1, rbn.CooldownMinutes), DateTime.UtcNow)) return;

        _logger.LogInformation("BAND OPENING: {Band} — {Dx} heard by {Skimmer} ({Dist:F0} {Unit}) {Snr}dB {Mode}",
            band, spot.DxCall, spot.Skimmer, dist, rbn.DistanceUnit, spot.Snr, spot.Mode);

        await _hub.Clients.All.OnBandOpening(new BandOpeningEvent(
            band, spot.DxCall, spot.Skimmer, Math.Round(dist), rbn.DistanceUnit ?? "mi", spot.Snr, spot.Mode));
    }

    private static (double Lat, double Lon)? StationPosition(StationSettings station)
    {
        if (station.Latitude is double lat && station.Longitude is double lon)
            return (lat, lon);
        return GeoMath.GridToLatLon(station.GridSquare);
    }

    private async Task<string?> ResolveSkimmerGridAsync(string skimmer)
    {
        var call = BandOpeningLogic.NormalizeSkimmerCall(skimmer);
        if (_gridCache.TryGetValue(call, out var cached)) return cached;

        string? grid = null;
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var qrz = scope.ServiceProvider.GetRequiredService<IQrzService>();
            var info = await qrz.LookupCallsignAsync(call);
            grid = string.IsNullOrWhiteSpace(info?.Grid) ? null : info!.Grid;
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Band-opening: grid lookup failed for {Call}: {Message}", call, ex.Message);
        }

        _gridCache[call] = grid; // cache negatives too — avoids hammering QRZ
        if (grid != null)
            _logger.LogDebug("Band-opening: skimmer {Call} grid {Grid}", call, grid);
        return grid;
    }

    private static List<string> SelectedBands(RbnAlertSettings rbn)
    {
        var bands = new List<string>();
        if (rbn.Band10m) bands.Add("10m");
        if (rbn.Band6m) bands.Add("6m");
        if (rbn.Band2m) bands.Add("2m");
        if (rbn.Band70cm) bands.Add("70cm");
        return bands;
    }
}
