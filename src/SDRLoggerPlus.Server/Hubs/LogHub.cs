using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Rig;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Native.Hamlib;

namespace SDRLoggerPlus.Server.Hubs;

public interface ILogHubClient
{
    Task OnCallsignFocused(CallsignFocusedEvent evt);
    Task OnCallsignLookedUp(CallsignLookedUpEvent evt);
    Task OnQsoLogged(QsoLoggedEvent evt);
    Task OnSpotReceived(SpotReceivedEvent evt);
    Task OnWsjtxDecode(WsjtxDecodeEvent evt);
    Task OnConfirmationSyncCompleted(ConfirmationSyncCompletedEvent evt);
    Task OnHotListChanged(HotListChangedEvent evt);
    Task OnLightningStatus(SDRLoggerPlus.Server.Services.Weather.LightningStatus status);
    Task OnLightningStrikes(LightningStrikesEvent evt);
    Task OnWindStatus(SDRLoggerPlus.Server.Services.Weather.WindStatus status);
    Task OnSatState(SDRLoggerPlus.Server.Services.Sat.SatState state);
    Task OnSpotSelected(SpotSelectedEvent evt);
    Task OnComboLinkChanged(ComboLinkChangedEvent evt);
    Task OnComboLogRequested(ComboLogRequestedEvent evt);
    Task OnRotatorPosition(RotatorPositionEvent evt);
    Task OnAdifMonitorImport(AdifMonitorImportEvent evt);
    Task OnBandOpening(BandOpeningEvent evt);
    Task OnRigStatus(RigStatusEvent evt);
    Task OnTciMeters(TciMetersEvent evt);
    Task OnStationLocation(StationLocationEvent evt);
    Task OnContestState(SDRLoggerPlus.Contracts.Api.ContestStateDto state);

    // Antenna Genius events
    Task OnAntennaGeniusDiscovered(AntennaGeniusDiscoveredEvent evt);
    Task OnAntennaGeniusDisconnected(AntennaGeniusDisconnectedEvent evt);
    Task OnAntennaGeniusStatus(AntennaGeniusStatusEvent evt);
    Task OnAntennaGeniusPortChanged(AntennaGeniusPortChangedEvent evt);

    // PGXL Amplifier events
    Task OnPgxlDiscovered(PgxlDiscoveredEvent evt);
    Task OnPgxlDisconnected(PgxlDisconnectedEvent evt);
    Task OnPgxlStatus(PgxlStatusEvent evt);

    // Tuner Genius events
    Task OnTunerGeniusDiscovered(TunerGeniusDiscoveredEvent evt);
    Task OnTunerGeniusDisconnected(TunerGeniusDisconnectedEvent evt);
    Task OnTunerGeniusStatus(TunerGeniusStatusEvent evt);
    Task OnTunerGeniusPortChanged(TunerGeniusPortChangedEvent evt);

    // Radio CAT Control events
    Task OnRadioDiscovered(RadioDiscoveredEvent evt);
    Task OnRadioRemoved(RadioRemovedEvent evt);
    Task OnRadioConnectionStateChanged(RadioConnectionStateChangedEvent evt);
    Task OnRadioStateChanged(RadioStateChangedEvent evt);

    // CW Keyer events
    Task OnCwKeyerStatus(CwKeyerStatusEvent evt);

    // Hamlib configuration events
    Task OnHamlibRigList(HamlibRigListEvent evt);
    Task OnHamlibRigCaps(HamlibRigCapsEvent evt);
    Task OnHamlibSerialPorts(HamlibSerialPortsEvent evt);
    Task OnHamlibConfigLoaded(HamlibConfigLoadedEvent evt);
    Task OnHamlibStatus(HamlibStatusEvent evt);

    // QRZ Sync events
    Task OnQrzSyncProgress(QrzSyncProgressEvent evt);

    // ADIF Import events
    Task OnAdifImportProgress(AdifImportProgressEvent evt);

    // LOTW upload events
    Task OnLotwUploadProgress(LotwUploadProgressEvent evt);

    // DX Cluster events
    Task OnClusterStatusChanged(ClusterStatusChangedEvent evt);

    // RBN events
    Task OnRbnSpot(RbnSpot spot);

    // Spectrum events
    Task OnSpectrumData(SpectrumDataEvent evt);
}

public class LogHub : Hub<ILogHubClient>
{
    private readonly ILogger<LogHub> _logger;
    private readonly AntennaGeniusService _antennaGeniusService;
    private readonly PgxlService _pgxlService;
    private readonly TunerGeniusService _tunerGeniusService;
    private readonly TciRadioService _tciRadioService;
    private readonly HamlibService _hamlibService;
    private readonly FlrigService _flrigService;
    private readonly IRigRegistry _rigRegistry;
    private readonly RotatorService _rotatorService;
    private readonly IQrzService _qrzService;
    private readonly IHamQthService _hamQthService;
    private readonly ISettingsRepository _settingsRepository;
    private readonly CwKeyerService _cwKeyerService;
    private readonly ICallsignImageRepository _imageRepository;
    private readonly IDbContext _dbContext;
    private readonly IRadioConfigRepository _radioConfigRepository;

    public LogHub(
        ILogger<LogHub> logger,
        AntennaGeniusService antennaGeniusService,
        PgxlService pgxlService,
        TunerGeniusService tunerGeniusService,
        TciRadioService tciRadioService,
        HamlibService hamlibService,
        FlrigService flrigService,
        IRigRegistry rigRegistry,
        RotatorService rotatorService,
        IQrzService qrzService,
        IHamQthService hamQthService,
        ISettingsRepository settingsRepository,
        CwKeyerService cwKeyerService,
        ICallsignImageRepository imageRepository,
        IDbContext dbContext,
        IRadioConfigRepository radioConfigRepository,
        DxClusterService? dxClusterService = null)
    {
        _dxClusterService = dxClusterService;
        _logger = logger;
        _antennaGeniusService = antennaGeniusService;
        _pgxlService = pgxlService;
        _tunerGeniusService = tunerGeniusService;
        _tciRadioService = tciRadioService;
        _hamlibService = hamlibService;
        _flrigService = flrigService;
        _rigRegistry = rigRegistry;
        _rotatorService = rotatorService;
        _qrzService = qrzService;
        _hamQthService = hamQthService;
        _settingsRepository = settingsRepository;
        _cwKeyerService = cwKeyerService;
        _imageRepository = imageRepository;
        _dbContext = dbContext;
        _radioConfigRepository = radioConfigRepository;
    }

    private readonly DxClusterService? _dxClusterService;

    public override async Task OnConnectedAsync()
    {
        _logger.LogInformation("Client connected: {ConnectionId}", Context.ConnectionId);

        // Replay recent spots so a client that connects after a broadcast burst
        // (app startup vs. spothole's immediate backlog poll) still sees them.
        if (_dxClusterService != null)
        {
            foreach (var spot in _dxClusterService.GetRecentSpots())
            {
                await Clients.Caller.OnSpotReceived(spot);
            }
        }

        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation("Client disconnected: {ConnectionId}", Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }

    // Client-to-server methods (called by frontend)

    public async Task FocusCallsign(CallsignFocusedEvent evt)
    {
        _logger.LogDebug("Callsign focused: {Callsign} from {Source}", evt.Callsign, evt.Source);
        await Clients.Others.OnCallsignFocused(evt);

        // Compound / portable calls (e.g. "F/HB9GUX") aren't in the callbooks
        // under the literal string — QRZ/HamQTH are keyed on the operator's
        // home ("base") call. Strip to the base call for the lookup so it
        // resolves instead of spinning; the DXCC entity still comes from the
        // FULL call via cty.dat below (France for "F/HB9GUX", not Switzerland).
        var baseCall = CallsignHelper.ExtractBaseCall(evt.Callsign);
        var isCompound = CallsignHelper.IsCompound(evt.Callsign);
        var isPrefixForm = CallsignHelper.IsPrefixForm(evt.Callsign);
        var compoundNote = CallsignHelper.DescribeCompound(
            evt.Callsign, c => CtyService.GetEntityFromCallsign(c).Country);

        // Lookup chain: QRZ → HamQTH → cty.dat centroid. Each source is tried
        // in order; whichever supplies coords first wins. The operator always
        // gets *some* lat/lon for the bearing line — even without QRZ or
        // HamQTH credentials configured.
        QrzCallsignInfo? info = null;
        try
        {
            // Hard 6 s ceiling — the QRZ XML call is on the UI hot path and a
            // slow/hanging response (or a session re-auth stall) must never
            // leave the profile panel spinning. Mirrors the HamQTH ceiling.
            info = await _qrzService.LookupCallsignAsync(baseCall)
                .WaitAsync(TimeSpan.FromSeconds(6));
        }
        catch (TimeoutException)
        {
            _logger.LogInformation("QRZ lookup timed out for {Callsign} — using HamQTH/cty.dat fallback", baseCall);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "QRZ lookup failed for {Callsign}", baseCall);
            // fall through — HamQTH + cty.dat fallbacks still apply
        }

        // HamQTH fallback for coords/name — only invoked when QRZ didn't
        // return them. Runs even if QRZ returned some fields (e.g. QRZ
        // handed back a Name but no lat/lon), backfilling only what's
        // missing so HamQTH data never overrides a paid QRZ subscription's
        // response. The call is on the UI hot path, so we impose a hard
        // 6 s ceiling around the whole HamQTH round trip — enough for a
        // healthy server + retry, small enough that a broken HamQTH can't
        // hang the callsign panel with a spinning wheel.
        HamQthCallsignInfo? hqInfo = null;
        if (info?.Latitude is null || info.Longitude is null)
        {
            try
            {
                using var hqCts = new CancellationTokenSource(TimeSpan.FromSeconds(6));
                hqInfo = await _hamQthService.LookupCallsignAsync(baseCall, hqCts.Token);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("HamQTH lookup timed out for {Callsign} — using cty.dat fallback", evt.Callsign);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "HamQTH lookup failed for {Callsign}", evt.Callsign);
                // fall through — cty.dat fallback still applies
            }
        }

        // Load station coords once — used for bearing/distance regardless of
        // which source supplies the target coords.
        var settings = await _settingsRepository.GetAsync();
        double? stationLat = null, stationLon = null;
        if (settings?.Station != null && settings.Station.Latitude.HasValue && settings.Station.Longitude.HasValue)
        {
            stationLat = NormalizeCoordinate(settings.Station.Latitude.Value, isLatitude: true);
            stationLon = NormalizeCoordinate(settings.Station.Longitude.Value, isLatitude: false);
            if (stationLat is 0 or null || stationLon is 0 or null)
            {
                stationLat = null;
                stationLon = null;
            }
            else if (stationLat != settings.Station.Latitude || stationLon != settings.Station.Longitude)
            {
                _logger.LogWarning("Station coordinates were in microdegree format: ({OrigLat}, {OrigLon}) -> ({NormLat}, {NormLon})",
                    settings.Station.Latitude, settings.Station.Longitude, stationLat, stationLon);
            }
        }

        // Decide the target coords by precedence: QRZ → HamQTH → cty.dat.
        // Only cty.dat coords are marked approximate; a real HamQTH hit is
        // the operator's actual QTH just like QRZ.
        double? targetLat = info?.Latitude ?? hqInfo?.Latitude;
        double? targetLon = info?.Longitude ?? hqInfo?.Longitude;
        bool isApproximate = false;
        string? country = info?.Country ?? hqInfo?.Country;
        int? cqZone = info?.CqZone ?? hqInfo?.CqZone;

        // A PREFIX-form compound ("F/HB9GUX") means the operator is portable in
        // the leading prefix's DXCC — so the base call's callbook country and
        // home coordinates are the wrong entity. Take the DXCC (country / CQ
        // zone) from the FULL call via cty.dat and drop the home coords so the
        // bearing falls through to the correct-country centroid below. (SUFFIX
        // forms like "HB9GUX/P" stay in the same entity, so we keep the precise
        // callbook coords untouched.)
        if (isPrefixForm)
        {
            var (ctyCountry, _, ctyCqZone) = CtyService.GetEntityFromCallsign(evt.Callsign);
            if (ctyCountry != null)
            {
                country = ctyCountry;
                cqZone = ctyCqZone ?? cqZone;
            }
            targetLat = null;
            targetLon = null;
        }

        if (!targetLat.HasValue || !targetLon.HasValue)
        {
            var centroid = CtyService.GetCentroidFromCallsign(evt.Callsign);
            if (centroid.HasValue)
            {
                targetLat = centroid.Value.Lat;
                targetLon = centroid.Value.Lon;
                isApproximate = true;
                // Fill in country/CQ zone from cty.dat if neither QRZ nor
                // HamQTH had them (rare — both usually surface these).
                var (ctyCountry, _, ctyCqZone) = CtyService.GetEntityFromCallsign(evt.Callsign);
                country ??= ctyCountry;
                cqZone ??= ctyCqZone;
                _logger.LogDebug("cty.dat fallback: {Callsign} -> centroid ({Lat}, {Lon}) [{Country}]",
                    evt.Callsign, targetLat, targetLon, country ?? "?");
            }
        }

        double? bearing = null;
        double? distance = null;
        if (targetLat.HasValue && targetLon.HasValue && stationLat.HasValue && stationLon.HasValue)
        {
            bearing = CalculateBearing(stationLat.Value, stationLon.Value, targetLat.Value, targetLon.Value);
            distance = CalculateDistance(stationLat.Value, stationLon.Value, targetLat.Value, targetLon.Value);
        }

        // Compose the event with source precedence QRZ → HamQTH for every
        // non-coord field. QRZ always wins when it has data; HamQTH backfills
        // whatever QRZ didn't provide. This keeps a paid QRZ subscription
        // fully authoritative even when HamQTH would have had richer data
        // for a particular field.
        string? name = info != null
            ? BuildFullName(info.FirstName, info.Name)
            : (hqInfo != null ? BuildFullName(hqInfo.FirstName, hqInfo.Name) : null);

        var lookedUpEvent = new CallsignLookedUpEvent(
            // Always echo the ORIGINAL focused call ("F/HB9GUX"), not the base
            // call the callbook was queried with — the frontend clears its
            // lookup spinner only when the returned callsign matches the
            // focused one, and the QSO should log the compound call as worked.
            Callsign: evt.Callsign,
            Name: name,
            Grid: info?.Grid ?? hqInfo?.Grid,
            Latitude: targetLat,
            Longitude: targetLon,
            Country: country,
            Dxcc: info?.Dxcc,
            CqZone: cqZone,
            ItuZone: info?.ItuZone ?? hqInfo?.ItuZone,
            State: info?.State ?? hqInfo?.State,
            City: info?.City ?? hqInfo?.City,
            County: info?.County ?? hqInfo?.County,
            ImageUrl: info?.ImageUrl ?? hqInfo?.ImageUrl,
            Bearing: bearing,
            Distance: distance,
            LatLonIsApproximate: isApproximate,
            BaseCallsign: isCompound ? baseCall : null,
            CompoundNote: compoundNote
        );

        _logger.LogDebug(
            "Callsign lookup: {Callsign} -> Name={Name}, Country={Country}, Bearing={Bearing}°, Sources: QRZ={Qrz} HamQTH={Hq} Cty={Cty}",
            evt.Callsign, name ?? "-", country ?? "-", bearing?.ToString("F0") ?? "N/A",
            info != null, hqInfo != null, isApproximate);

        await Clients.All.OnCallsignLookedUp(lookedUpEvent);

        // Stage A′ name-back (docs/COMBO_LINK.md): if a Lyra Combo link is
        // active, push the resolved contact back over the existing TCI socket so
        // Lyra's CW Console {NAME}/{GRID} tokens fill. Only when the callbook
        // actually resolved new info (a name or grid) — a bare call, likely the
        // very one Lyra just sent us, carries nothing new and must not bounce
        // back (the echo guard). PushComboContactAsync no-ops on any radio whose
        // Combo link is off, and Lyra applies our src=sdrlog frame under its own
        // echo guard, so this can't loop.
        // CW ops address each other by first name, so the {NAME} token should
        // carry just the given name — not the composed "First Last". Prefer the
        // callbook's dedicated first-name field (QRZ <fname> / HamQTH <nick>);
        // fall back to the first token of the composed name. Note QRZ's `Name`
        // is the SURNAME, so `name` alone (when <fname> is absent) is the last
        // name — hence the fallback still takes only the first token. Sending
        // the whole "John Smith" would make a "TNX {NAME} 73" macro read
        // "TNX JOHN SMITH 73".
        var cwName = info?.FirstName ?? hqInfo?.FirstName ?? name;
        cwName = cwName?.Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();

        // Stage C — reverse call push. A deliberate SELECT (spot / globe click,
        // combo populate — any non-'log-entry' source) carries a COMPLETE call,
        // so push it to Lyra's His Call even when the callbook resolved nothing,
        // so His Call tracks SDRLogger+ for unlisted/DX calls too. Manual typing
        // ('log-entry') fires FocusCallsign on every keystroke ≥3 chars, so it's
        // pushed ONLY when a name/grid resolved (the A′ path) — otherwise a
        // partial call would spam Lyra's His Call as the operator types.
        // Loop-safe: Lyra applies our src=sdrlog frame under its own echo guard,
        // so a call it originally sent us doesn't bounce back.
        var isDeliberateSelect = !string.Equals(evt.Source, "log-entry", StringComparison.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(cwName) || !string.IsNullOrWhiteSpace(lookedUpEvent.Grid) || isDeliberateSelect)
        {
            try
            {
                await _tciRadioService.PushComboContactAsync(evt.Callsign, cwName, lookedUpEvent.Grid);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Combo call/name push failed for {Call}", evt.Callsign);
            }
        }
    }

    private static double CalculateBearing(double lat1, double lon1, double lat2, double lon2)
    {
        var dLon = ToRadians(lon2 - lon1);
        var lat1Rad = ToRadians(lat1);
        var lat2Rad = ToRadians(lat2);

        var y = Math.Sin(dLon) * Math.Cos(lat2Rad);
        var x = Math.Cos(lat1Rad) * Math.Sin(lat2Rad) - Math.Sin(lat1Rad) * Math.Cos(lat2Rad) * Math.Cos(dLon);

        var bearing = Math.Atan2(y, x);
        return (ToDegrees(bearing) + 360) % 360;
    }

    private static double CalculateDistance(double lat1, double lon1, double lat2, double lon2)
    {
        const double EarthRadiusKm = 6371;
        var dLat = ToRadians(lat2 - lat1);
        var dLon = ToRadians(lon2 - lon1);

        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(ToRadians(lat1)) * Math.Cos(ToRadians(lat2)) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);

        var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        return EarthRadiusKm * c;
    }

    private static double ToRadians(double degrees) => degrees * Math.PI / 180;
    private static double ToDegrees(double radians) => radians * 180 / Math.PI;

    /// <summary>
    /// Normalize coordinates that may be in microdegree format (degrees * 1,000,000).
    /// </summary>
    private static double? NormalizeCoordinate(double value, bool isLatitude)
    {
        var maxValid = isLatitude ? 90.0 : 180.0;

        // Check if value is already in valid range
        if (Math.Abs(value) <= maxValid)
        {
            return value;
        }

        // Check if value looks like microdegrees (within valid range when divided by 1,000,000)
        var normalized = value / 1_000_000.0;
        if (Math.Abs(normalized) <= maxValid)
        {
            return normalized;
        }

        // Value is invalid even after normalization
        return null;
    }

    private static string? BuildFullName(string? firstName, string? lastName)
    {
        var hasFirst = !string.IsNullOrWhiteSpace(firstName);
        var hasLast = !string.IsNullOrWhiteSpace(lastName);

        if (hasFirst && hasLast)
            return $"{firstName} {lastName}";
        if (hasFirst)
            return firstName;
        if (hasLast)
            return lastName;
        return null;
    }

    private async Task SaveCallsignMapImageAsync(QrzCallsignInfo info)
    {
        if (!_dbContext.IsConnected) return;

        await _imageRepository.UpsertAsync(new CallsignMapImage
        {
            Callsign = info.Callsign,
            ImageUrl = info.ImageUrl,
            Latitude = info.Latitude!.Value,
            Longitude = info.Longitude!.Value,
            Name = BuildFullName(info.FirstName, info.Name),
            Country = info.Country,
            Grid = info.Grid,
            SavedAt = DateTime.UtcNow
        });
        _logger.LogDebug("Saved callsign map image for {Callsign}", info.Callsign);
    }

    /// <summary>
    /// Persist a callsign map image to the database. Called by the frontend after a QSO is logged,
    /// so only actually worked callsigns are saved to the map overlay.
    /// </summary>
    public async Task PersistCallsignMapImage(CallsignMapImage image)
    {
        if (!_dbContext.IsConnected) return;
        if (string.IsNullOrEmpty(image.Callsign)) return;

        await _imageRepository.UpsertAsync(image);
        _logger.LogDebug("Persisted callsign map image for {Callsign} after QSO logged", image.Callsign);
    }

    public async Task SelectSpot(SpotSelectedEvent evt)
    {
        _logger.LogInformation("Spot selected: {DxCall} on {FrequencyMHz} MHz ({Mode})", evt.DxCall, evt.Frequency / 1000.0, evt.Mode ?? "unknown");

        // Broadcast to ALL clients (including caller) so the log entry gets populated
        await Clients.All.OnSpotSelected(evt);

        // Convert spot frequency from kHz to Hz
        var frequencyHz = (long)(evt.Frequency * 1000);

        // Tune the active rig. Registry precedence is TCI → Hamlib → flrig; the
        // per-protocol freq/mode ORDER lives inside each backend's TuneAsync:
        //   - TCI (Lyra / Thetis / ExpertSDR3): frequency BEFORE mode — TCI collapses
        //     CWU/CWL → "CW" and re-derives the sideband from the CURRENT dial (CWU
        //     above 10 MHz, CWL below; same for USB↔LSB), so the new frequency must be
        //     set first or the wrong sideband sticks and the spot needs a second click.
        //   - Hamlib / flrig: mode BEFORE frequency — their rigs apply a CW pitch
        //     offset on mode change that would shift the dial ±700 Hz if done after.
        if (_rigRegistry.ActiveTuner() is { } target)
        {
            var mode = string.IsNullOrEmpty(evt.Mode) ? null : evt.Mode;
            var tuned = await target.Backend.TuneAsync(target.RadioId, frequencyHz, mode);
            if (tuned)
            {
                _logger.LogInformation("Tuned {Type} radio {RadioId} to {FrequencyMHz} MHz{Mode}",
                    target.Backend.Type, target.RadioId, frequencyHz / 1000000.0,
                    mode is null ? "" : $" ({mode})");
            }
        }
    }

    /// <summary>
    /// Send a spot to the operator-picked primary DX cluster. Returns a
    /// tuple the client can toast — Sent=true with the target cluster
    /// name on success, Sent=false with a human-readable reason (e.g.
    /// "multiple clusters connected, pick a primary") on refusal.
    /// </summary>
    public async Task<SendSpotResult> SendDxSpot(string callsign, double frequencyKhz, string? comment)
    {
        if (_dxClusterService is null)
            return new SendSpotResult(false, 0, "DX cluster service not available");
        if (string.IsNullOrWhiteSpace(callsign) || frequencyKhz <= 0)
            return new SendSpotResult(false, 0, "Callsign and frequency are required");
        try
        {
            return await _dxClusterService.SendSpotAsync(callsign, frequencyKhz, comment);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SendDxSpot failed for {Callsign} @ {FreqKhz}", callsign, frequencyKhz);
            return new SendSpotResult(false, 0, $"Spot failed: {ex.Message}");
        }
    }


    public async Task TuneToFrequency(long frequencyHz)
    {
        _logger.LogInformation("Tune to frequency: {FrequencyMHz} MHz", frequencyHz / 1000000.0);

        // Active rig, frequency only (no mode change). Registry precedence TCI → Hamlib → flrig.
        if (_rigRegistry.ActiveTuner() is { } target)
        {
            var tuned = await target.Backend.SetFrequencyAsync(target.RadioId, frequencyHz);
            if (tuned)
            {
                _logger.LogInformation("Tuned {Type} radio {RadioId} to {FrequencyMHz} MHz",
                    target.Backend.Type, target.RadioId, frequencyHz / 1000000.0);
            }
        }
    }

    /// <summary>
    /// Set the active rig's mode. Tries TCI → Hamlib → flrig in order,
    /// same precedence as SelectSpot and TuneToFrequency. Used by the
    /// Log Entry mode dropdown when following the rig — without this
    /// the follow-radio effect overwrites the dropdown back to the rig's
    /// state every poll cycle.
    /// </summary>
    public async Task SetRadioMode(string mode)
    {
        if (string.IsNullOrWhiteSpace(mode)) return;
        _logger.LogInformation("Set radio mode: {Mode}", mode);

        // Active rig, mode only. Registry precedence TCI → Hamlib → flrig. Pass the
        // active radio's CURRENT dial so TCI can pick the CW/SSB sideband; it's the
        // frequency the rig is already on, so Hamlib/flrig don't move (flrig ignores it).
        if (_rigRegistry.ActiveTuner() is { } target)
        {
            var currentHz = target.Backend.GetRadioStates()
                .FirstOrDefault(s => s.RadioId == target.RadioId)?.FrequencyHz ?? 0;
            await target.Backend.SetModeAsync(target.RadioId, mode, currentHz);
        }
    }

    /// <summary>
    /// Tune the active rig to a sensible default frequency for the given
    /// band. Used by the Log Entry band dropdown — pick a band, land near
    /// the middle of the SSB portion (or the CW portion when the current
    /// mode is CW). Reuses TuneToFrequency's TCI → Hamlib → flrig chain.
    /// </summary>
    public async Task TuneToBand(string band, string? mode = null)
    {
        if (string.IsNullOrWhiteSpace(band)) return;
        long? freqHz = DefaultFrequencyForBand(band.ToLowerInvariant(), mode);
        if (freqHz is null)
        {
            _logger.LogDebug("TuneToBand: no default freq for band {Band}", band);
            return;
        }
        _logger.LogInformation("Tune to band {Band} → {FreqMHz} MHz", band, freqHz.Value / 1_000_000.0);
        await TuneToFrequency(freqHz.Value);
    }

    // Per-band default landing frequency. SSB portion centres for the phone
    // bands, CW section for the CW-only bands. Matches "where would a ham
    // typically drop the VFO when they pick this band cold" — the user can
    // always tweak from there.
    private static long? DefaultFrequencyForBand(string band, string? mode)
    {
        var isCw = string.Equals(mode, "CW", StringComparison.OrdinalIgnoreCase)
                || string.Equals(mode, "CWU", StringComparison.OrdinalIgnoreCase)
                || string.Equals(mode, "CWL", StringComparison.OrdinalIgnoreCase);
        return band switch
        {
            "160m" => isCw ? 1_820_000L  : 1_845_000L,
            "80m"  => isCw ? 3_540_000L  : 3_780_000L,
            "60m"  => 5_357_000L,
            "40m"  => isCw ? 7_020_000L  : 7_180_000L,
            "30m"  => 10_130_000L,
            "20m"  => isCw ? 14_040_000L : 14_250_000L,
            "17m"  => isCw ? 18_075_000L : 18_140_000L,
            "15m"  => isCw ? 21_040_000L : 21_300_000L,
            "12m"  => isCw ? 24_895_000L : 24_940_000L,
            "10m"  => isCw ? 28_040_000L : 28_400_000L,
            "6m"   => isCw ? 50_090_000L : 50_150_000L,
            "2m"   => isCw ? 144_050_000L : 144_200_000L,
            "1.25m" => 223_500_000L,
            "70cm" => isCw ? 432_050_000L : 432_100_000L,
            _ => null,
        };
    }

    public async Task CommandRotator(RotatorCommandEvent evt)
    {
        _logger.LogInformation("Rotator command: {Azimuth}° from {Source}", evt.TargetAzimuth, evt.Source);
        await _rotatorService.SetPositionAsync(evt.TargetAzimuth);
    }

    public async Task StopRotator()
    {
        _logger.LogInformation("Rotator stop command");
        await _rotatorService.StopAsync();
    }

    public async Task RequestRotatorStatus()
    {
        _logger.LogDebug("Client requested rotator status");
        var status = _rotatorService.GetCurrentStatus();
        await Clients.Caller.OnRotatorPosition(status);
    }

    // Antenna Genius methods

    public async Task SelectAntenna(SelectAntennaCommand cmd)
    {
        _logger.LogInformation("Selecting antenna {AntennaId} for port {PortId} on device {Serial}",
            cmd.AntennaId, cmd.PortId, cmd.DeviceSerial);

        await _antennaGeniusService.SelectAntennaAsync(cmd.DeviceSerial, cmd.PortId, cmd.AntennaId);
    }

    public async Task RequestAntennaGeniusStatus()
    {
        _logger.LogDebug("Client requested Antenna Genius status");

        foreach (var status in _antennaGeniusService.GetAllDeviceStatuses())
        {
            await Clients.Caller.OnAntennaGeniusStatus(status);
        }
    }

    // PGXL Amplifier methods

    public async Task SetPgxlOperate(SetPgxlOperateCommand cmd)
    {
        _logger.LogInformation("Setting PGXL {Serial} to OPERATE", cmd.Serial);
        await _pgxlService.SetOperateAsync(cmd.Serial);
    }

    public async Task SetPgxlStandby(SetPgxlStandbyCommand cmd)
    {
        _logger.LogInformation("Setting PGXL {Serial} to STANDBY", cmd.Serial);
        await _pgxlService.SetStandbyAsync(cmd.Serial);
    }

    public async Task RequestPgxlStatus()
    {
        _logger.LogDebug("Client requested PGXL status");

        foreach (var status in _pgxlService.GetAllStatuses())
        {
            await Clients.Caller.OnPgxlStatus(status);
        }
    }

    // Tuner Genius methods

    public async Task TuneTunerGenius(TuneTunerGeniusCommand cmd)
    {
        _logger.LogInformation("Tuning port {PortId} on Tuner Genius {Serial}",
            cmd.PortId, cmd.DeviceSerial);

        await _tunerGeniusService.TuneAsync(cmd.DeviceSerial, cmd.PortId);
    }

    public async Task BypassTunerGenius(BypassTunerGeniusCommand cmd)
    {
        _logger.LogInformation("Setting bypass={Bypass} for port {PortId} on Tuner Genius {Serial}",
            cmd.Bypass, cmd.PortId, cmd.DeviceSerial);

        await _tunerGeniusService.SetBypassAsync(cmd.DeviceSerial, cmd.PortId, cmd.Bypass);
    }

    public async Task OperateTunerGenius(OperateTunerGeniusCommand cmd)
    {
        _logger.LogInformation("Setting operate={Operate} on Tuner Genius {Serial}",
            cmd.Operate, cmd.DeviceSerial);

        await _tunerGeniusService.SetOperateAsync(cmd.DeviceSerial, cmd.Operate);
    }

    public async Task ActivateChannelTunerGenius(ActivateChannelTunerGeniusCommand cmd)
    {
        _logger.LogInformation("Activating channel {Channel} on Tuner Genius {Serial}",
            cmd.Channel, cmd.DeviceSerial);

        await _tunerGeniusService.ActivateChannelAsync(cmd.DeviceSerial, cmd.Channel);
    }

    public async Task RequestTunerGeniusStatus()
    {
        _logger.LogDebug("Client requested Tuner Genius status");

        foreach (var status in _tunerGeniusService.GetAllDeviceStatuses())
        {
            await Clients.Caller.OnTunerGeniusStatus(status);
        }
    }

    // Radio CAT Control methods

    public async Task StartRadioDiscovery(StartRadioDiscoveryCommand cmd)
    {
        _logger.LogInformation("Starting radio discovery for {Type}", cmd.Type);

        if (cmd.Type == RadioType.Tci)
        {
            await _tciRadioService.StartDiscoveryAsync();
        }
    }

    public async Task StopRadioDiscovery(StopRadioDiscoveryCommand cmd)
    {
        _logger.LogInformation("Stopping radio discovery for {Type}", cmd.Type);

        if (cmd.Type == RadioType.Tci)
        {
            await _tciRadioService.StopDiscoveryAsync();
        }
    }

    public async Task ConnectRadio(ConnectRadioCommand cmd)
    {
        _logger.LogInformation("Connecting to radio {RadioId}", cmd.RadioId);

        // TCI first (Hamlib is handled separately via config)
        if (_tciRadioService.HasRadio(cmd.RadioId))
        {
            await _tciRadioService.ConnectAsync(cmd.RadioId);
        }
        else if (cmd.RadioId.StartsWith("tci-"))
        {
            // Saved TCI rig not in live discovery — parse host:port from ID and connect directly
            var hostPort = cmd.RadioId["tci-".Length..];
            var parts = hostPort.Split(':');
            var host = parts[0];
            var port = parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : 50001;

            // Load saved name from radio_configs repo
            var savedConfig = await _radioConfigRepository.GetByRadioIdAsync(cmd.RadioId);
            var name = savedConfig?.TciName;

            await _tciRadioService.ConnectDirectAsync(host, port, !string.IsNullOrEmpty(name) ? name : null);
        }
        else if (cmd.RadioId == _hamlibService.RadioId)
        {
            _logger.LogDebug("Hamlib radio {RadioId} is already connected", cmd.RadioId);
        }
        else if (cmd.RadioId.StartsWith("hamlib-") && !_hamlibService.IsConnected)
        {
            // Saved Hamlib rig that's disconnected — load config and reconnect
            var config = await _hamlibService.LoadConfigAsync();
            if (config != null)
            {
                _logger.LogInformation("Reconnecting to saved Hamlib rig: {ModelName}", config.ModelName);
                await _hamlibService.ConnectAsync(config);
            }
            else
            {
                _logger.LogWarning("No saved Hamlib config found for {RadioId}", cmd.RadioId);
            }
        }
        else
        {
            _logger.LogWarning("Radio {RadioId} not found", cmd.RadioId);
        }
    }

    public async Task DisconnectRadio(DisconnectRadioCommand cmd)
    {
        _logger.LogInformation("Disconnecting from radio {RadioId}", cmd.RadioId);

        if (_tciRadioService.HasRadio(cmd.RadioId))
        {
            await _tciRadioService.DisconnectAsync(cmd.RadioId);
        }
        else if (cmd.RadioId == _hamlibService.RadioId)
        {
            await _hamlibService.DisconnectAsync();
        }
    }

    // ===== Hamlib Configuration Methods =====

    /// <summary>
    /// Get list of all available Hamlib rig models
    /// </summary>
    public async Task GetHamlibRigList()
    {
        _logger.LogInformation("Client requested Hamlib rig list");

        // Try to get rigs regardless of initialization state - this triggers lazy loading
        List<HamlibRigModelInfo> rigs;
        try
        {
            rigs = _hamlibService.GetAvailableRigs()
                .Select(r => new HamlibRigModelInfo(r.ModelId, r.Manufacturer, r.Model, r.Version, r.DisplayName))
                .ToList();

            _logger.LogInformation("Returning {Count} Hamlib rig models (lib loaded: {Loaded}, path: {Path})",
                rigs.Count,
                Native.Hamlib.HamlibNative.IsLoaded,
                Native.Hamlib.HamlibNative.LoadedLibraryPath ?? "none");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get Hamlib rig list");
            rigs = new List<HamlibRigModelInfo>();
        }

        // If no rigs found, check for errors
        if (rigs.Count == 0)
        {
            var error = Native.Hamlib.HamlibRigList.InitError;
            if (!string.IsNullOrEmpty(error))
            {
                _logger.LogWarning("Hamlib initialization error: {Error}", error);
            }
        }

        await Clients.Caller.OnHamlibRigList(new HamlibRigListEvent(rigs));
    }

    /// <summary>
    /// Get capabilities for a specific Hamlib rig model
    /// </summary>
    public async Task GetHamlibRigCaps(int modelId)
    {
        _logger.LogDebug("Client requested Hamlib rig caps for model {ModelId}", modelId);

        var caps = _hamlibService.GetRigCapabilities(modelId);
        var capsDto = new HamlibRigCapabilities(
            caps.CanGetFreq,
            caps.CanGetMode,
            caps.CanGetVfo,
            caps.CanGetPtt,
            caps.CanGetPower,
            caps.CanGetRit,
            caps.CanGetXit,
            caps.CanGetKeySpeed,
            caps.CanSendMorse,
            caps.DefaultDataBits,
            caps.DefaultStopBits,
            caps.IsNetworkOnly,
            caps.SupportsSerial,
            caps.SupportsNetwork
        );

        await Clients.Caller.OnHamlibRigCaps(new HamlibRigCapsEvent(modelId, capsDto));
    }

    /// <summary>
    /// Get list of available serial ports
    /// </summary>
    public async Task GetHamlibSerialPorts()
    {
        _logger.LogDebug("Client requested serial ports list");

        var ports = _hamlibService.GetSerialPorts();
        await Clients.Caller.OnHamlibSerialPorts(new HamlibSerialPortsEvent(ports));
    }

    /// <summary>
    /// Get saved Hamlib configuration
    /// </summary>
    public async Task GetHamlibConfig()
    {
        _logger.LogDebug("Client requested Hamlib config");

        var config = await _hamlibService.LoadConfigAsync();
        HamlibRigConfigDto? configDto = null;

        if (config != null)
        {
            configDto = new HamlibRigConfigDto(
                config.ModelId,
                config.ModelName,
                (Contracts.Events.HamlibConnectionType)(int)config.ConnectionType,
                config.SerialPort,
                config.BaudRate,
                (Contracts.Events.HamlibDataBits)(int)config.DataBits,
                (Contracts.Events.HamlibStopBits)(int)config.StopBits,
                (Contracts.Events.HamlibFlowControl)(int)config.FlowControl,
                (Contracts.Events.HamlibParity)(int)config.Parity,
                config.Hostname,
                config.NetworkPort,
                (Contracts.Events.HamlibPttType)(int)config.PttType,
                config.PttPort,
                config.GetFrequency,
                config.GetMode,
                config.GetVfo,
                config.GetPtt,
                config.GetPower,
                config.GetRit,
                config.GetXit,
                config.GetKeySpeed,
                config.PollIntervalMs
            );
        }

        await Clients.Caller.OnHamlibConfigLoaded(new HamlibConfigLoadedEvent(configDto));
    }

    /// <summary>
    /// Get Hamlib initialization status
    /// </summary>
    public async Task GetHamlibStatus()
    {
        _logger.LogDebug("Client requested Hamlib status");

        await Clients.Caller.OnHamlibStatus(new HamlibStatusEvent(
            _hamlibService.IsInitialized,
            _hamlibService.IsConnected,
            _hamlibService.RadioId,
            null
        ));
    }

    /// <summary>
    /// Connect to a Hamlib rig with full configuration
    /// </summary>
    public async Task ConnectHamlibRig(HamlibRigConfigDto configDto)
    {
        _logger.LogInformation("Connecting to Hamlib rig: {ModelName}", configDto.ModelName);

        var config = new HamlibRigConfig
        {
            ModelId = configDto.ModelId,
            ModelName = configDto.ModelName,
            ConnectionType = (Native.Hamlib.HamlibConnectionType)(int)configDto.ConnectionType,
            SerialPort = configDto.SerialPort,
            BaudRate = configDto.BaudRate,
            DataBits = (Native.Hamlib.HamlibDataBits)(int)configDto.DataBits,
            StopBits = (Native.Hamlib.HamlibStopBits)(int)configDto.StopBits,
            FlowControl = (Native.Hamlib.HamlibFlowControl)(int)configDto.FlowControl,
            Parity = (Native.Hamlib.HamlibParity)(int)configDto.Parity,
            Hostname = configDto.Hostname,
            NetworkPort = configDto.NetworkPort,
            PttType = (Native.Hamlib.HamlibPttType)(int)configDto.PttType,
            PttPort = configDto.PttPort,
            GetFrequency = configDto.GetFrequency,
            GetMode = configDto.GetMode,
            GetVfo = configDto.GetVfo,
            GetPtt = configDto.GetPtt,
            GetPower = configDto.GetPower,
            GetRit = configDto.GetRit,
            GetXit = configDto.GetXit,
            GetKeySpeed = configDto.GetKeySpeed,
            PollIntervalMs = configDto.PollIntervalMs
        };

        try
        {
            await _hamlibService.ConnectAsync(config);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect Hamlib rig");
            await Clients.Caller.OnHamlibStatus(new HamlibStatusEvent(
                _hamlibService.IsInitialized,
                false,
                null,
                ex.Message
            ));
        }
    }

    /// <summary>
    /// Save Hamlib rig configuration without connecting.
    /// The rig will appear in the saved list but won't be connected.
    /// </summary>
    public async Task SaveHamlibConfig(HamlibRigConfigDto configDto)
    {
        _logger.LogInformation("Saving Hamlib config (no connect): {ModelName}", configDto.ModelName);

        var config = new HamlibRigConfig
        {
            ModelId = configDto.ModelId,
            ModelName = configDto.ModelName,
            ConnectionType = (Native.Hamlib.HamlibConnectionType)(int)configDto.ConnectionType,
            SerialPort = configDto.SerialPort,
            BaudRate = configDto.BaudRate,
            DataBits = (Native.Hamlib.HamlibDataBits)(int)configDto.DataBits,
            StopBits = (Native.Hamlib.HamlibStopBits)(int)configDto.StopBits,
            FlowControl = (Native.Hamlib.HamlibFlowControl)(int)configDto.FlowControl,
            Parity = (Native.Hamlib.HamlibParity)(int)configDto.Parity,
            Hostname = configDto.Hostname,
            NetworkPort = configDto.NetworkPort,
            PttType = (Native.Hamlib.HamlibPttType)(int)configDto.PttType,
            PttPort = configDto.PttPort,
            GetFrequency = configDto.GetFrequency,
            GetMode = configDto.GetMode,
            GetVfo = configDto.GetVfo,
            GetPtt = configDto.GetPtt,
            GetPower = configDto.GetPower,
            GetRit = configDto.GetRit,
            GetXit = configDto.GetXit,
            GetKeySpeed = configDto.GetKeySpeed,
            PollIntervalMs = configDto.PollIntervalMs
        };

        await _hamlibService.SaveConfigOnlyAsync(config);
        await RequestRadioStatus();
    }

    /// <summary>
    /// Disconnect from the Hamlib rig
    /// </summary>
    public async Task DisconnectHamlibRig()
    {
        _logger.LogInformation("Disconnecting from Hamlib rig");
        await _hamlibService.DisconnectAsync();
    }

    /// <summary>
    /// Delete saved Hamlib configuration
    /// </summary>
    public async Task DeleteHamlibConfig()
    {
        _logger.LogInformation("Deleting saved Hamlib configuration");
        await _hamlibService.DeleteConfigAsync();
        
        // Request updated radio status to reflect the removal
        await RequestRadioStatus();
    }

    /// <summary>
    /// Save TCI configuration to the radio_configs collection
    /// </summary>
    public async Task SaveTciConfig(string host, int port, string? name)
    {
        _logger.LogInformation("Saving TCI config: {Host}:{Port}", host, port);
        await _tciRadioService.SaveTciConfigAsync(host, port, name);
        await RequestRadioStatus();
    }

    /// <summary>
    /// Delete saved TCI configuration
    /// </summary>
    public async Task DeleteTciConfig(string? radioId = null)
    {
        _logger.LogInformation("Deleting saved TCI configuration: {RadioId}", radioId ?? "(auto-detect)");

        if (!string.IsNullOrEmpty(radioId))
        {
            // Delete from radio_configs and remove from discovered
            await _tciRadioService.DeleteTciConfigAsync(radioId);
            await _tciRadioService.RemoveRadioAsync(radioId);
        }
        else
        {
            // Fallback: delete all TCI configs from the repo
            var tciConfigs = await _radioConfigRepository.GetByTypeAsync("tci");
            foreach (var config in tciConfigs)
            {
                await _radioConfigRepository.DeleteByRadioIdAsync(config.RadioId);
                await _tciRadioService.RemoveRadioAsync(config.RadioId);
            }
        }

        // Also clear the legacy settings.Radio.Tci.Host field to prevent migration recreating the entry on restart
        var settings = await _settingsRepository.GetAsync();
        if (settings?.Radio?.Tci != null && !string.IsNullOrEmpty(settings.Radio.Tci.Host))
        {
            settings.Radio.Tci.Host = null;
            settings.Radio.Tci.Name = null;
            await _settingsRepository.UpsertAsync(settings);
        }

        // Request updated radio status to reflect the removal
        await RequestRadioStatus();
    }

    /// <summary>
    /// Connect directly to a TCI server without discovery
    /// </summary>
    public async Task ConnectTci(string host, int port = 50001, string? name = null)
    {
        _logger.LogInformation("Connecting to TCI at {Host}:{Port}", host, port);
        await _tciRadioService.ConnectDirectAsync(host, port, name);
    }

    /// <summary>
    /// Disconnect from a TCI server
    /// </summary>
    public async Task DisconnectTci(string radioId)
    {
        _logger.LogInformation("Disconnecting from TCI {RadioId}", radioId);
        await _tciRadioService.DisconnectAsync(radioId);
    }

    // ── flrig (XML-RPC) ─────────────────────────────────────────────────
    // flrig runs as a separate desktop bridge to the physical rig, so
    // there's no "discover" step — the operator just enters the host+port
    // (defaults 127.0.0.1:12345) in Settings and toggles Enabled. The
    // FlrigService background poller handles connect/reconnect on its own
    // schedule; these methods only need to save config + push a status
    // request so the UI reflects the change immediately.

    /// <summary>
    /// Save flrig connection config to user settings. Enabling turns on
    /// the background poller; disabling gracefully takes it offline.
    /// </summary>
    public async Task SaveFlrigConfig(string host, int port, bool enabled, string? digitalMode = null, string? rttyMode = null)
    {
        _logger.LogInformation("Saving flrig config: {Host}:{Port} enabled={Enabled}", host, port, enabled);
        var settings = await _settingsRepository.GetAsync() ?? new SDRLoggerPlus.Contracts.Models.UserSettings();
        settings.Radio ??= new();
        settings.Radio.Flrig ??= new();
        settings.Radio.Flrig.Host = string.IsNullOrWhiteSpace(host) ? "127.0.0.1" : host.Trim();
        settings.Radio.Flrig.Port = port > 0 ? port : 12345;
        settings.Radio.Flrig.Enabled = enabled;
        settings.Radio.Flrig.DigitalMode = string.IsNullOrWhiteSpace(digitalMode) ? null : digitalMode!.Trim();
        settings.Radio.Flrig.RttyMode = string.IsNullOrWhiteSpace(rttyMode) ? null : rttyMode!.Trim();
        await _settingsRepository.UpsertAsync(settings);
        await RequestRadioStatus();
    }

    /// <summary>
    /// Set the rig frequency via flrig XML-RPC.
    /// </summary>
    public async Task SetFlrigFrequency(long frequencyHz)
    {
        _logger.LogDebug("flrig set freq {Hz} Hz", frequencyHz);
        await _flrigService.SetFrequencyAsync(frequencyHz);
    }

    /// <summary>
    /// Set the rig mode via flrig XML-RPC. Accepts app-normalized modes
    /// (USB/LSB/CWU/CWL/FT8/DIGU/etc.); FlrigService handles the rig-
    /// specific translation.
    /// </summary>
    public async Task SetFlrigMode(string mode)
    {
        _logger.LogDebug("flrig set mode {Mode}", mode);
        await _flrigService.SetModeAsync(mode);
    }

    public async Task SelectRadioInstance(SelectRadioInstanceCommand cmd)
    {
        _logger.LogInformation("Selecting instance {Instance} on radio {RadioId}", cmd.Instance, cmd.RadioId);
        await _tciRadioService.SelectInstanceAsync(cmd.RadioId, cmd.Instance);
    }

    public async Task RequestRadioStatus()
    {
        _logger.LogDebug("Client requested radio status");

        // Send all discovered radios
        foreach (var radio in await _tciRadioService.GetDiscoveredRadiosAsync())
        {
            await Clients.Caller.OnRadioDiscovered(radio);
        }

        foreach (var radio in await _hamlibService.GetDiscoveredRadiosAsync())
        {
            await Clients.Caller.OnRadioDiscovered(radio);
        }

        // Send current radio states
        foreach (var state in _tciRadioService.GetRadioStates())
        {
            await Clients.Caller.OnRadioStateChanged(state);
        }

        foreach (var state in _hamlibService.GetRadioStates())
        {
            await Clients.Caller.OnRadioStateChanged(state);
        }

        // Send current connection states so UI reflects actual connection status
        foreach (var connState in _tciRadioService.GetConnectionStates())
        {
            await Clients.Caller.OnRadioConnectionStateChanged(connState);
        }
    }

    // CW Keyer methods

    public async Task SendCwKey(SendCwKeyCommand cmd)
    {
        _logger.LogInformation("Sending CW message for radio {RadioId}: {Message}", cmd.RadioId, cmd.Message);
        await _cwKeyerService.SendCwAsync(cmd.RadioId, cmd.Message, cmd.SpeedWpm);
    }

    public async Task StopCwKey(StopCwKeyCommand cmd)
    {
        _logger.LogInformation("Stopping CW keying for radio {RadioId}", cmd.RadioId);
        await _cwKeyerService.StopCwAsync(cmd.RadioId);
    }

    public async Task SetCwSpeed(SetCwSpeedCommand cmd)
    {
        _logger.LogInformation("Setting CW speed for radio {RadioId}: {Wpm} WPM", cmd.RadioId, cmd.SpeedWpm);
        await _cwKeyerService.SetSpeedAsync(cmd.RadioId, cmd.SpeedWpm);
    }

    public async Task RequestCwKeyerStatus(string radioId)
    {
        _logger.LogDebug("Client requested CW keyer status for radio {RadioId}", radioId);
        var status = _cwKeyerService.GetStatus(radioId);
        if (status != null)
        {
            await Clients.Caller.OnCwKeyerStatus(status);
        }
    }

}

// Extension method for broadcasting events from services
public static class LogHubExtensions
{
    public static async Task BroadcastSpot(this IHubContext<LogHub, ILogHubClient> hub, SpotReceivedEvent evt)
    {
        await hub.Clients.All.OnSpotReceived(evt);
    }

    public static async Task BroadcastSpotSelected(this IHubContext<LogHub, ILogHubClient> hub, SpotSelectedEvent evt)
    {
        await hub.Clients.All.OnSpotSelected(evt);
    }

    public static async Task BroadcastComboLinkChanged(this IHubContext<LogHub, ILogHubClient> hub, ComboLinkChangedEvent evt)
    {
        await hub.Clients.All.OnComboLinkChanged(evt);
    }

    public static async Task BroadcastComboLogRequested(this IHubContext<LogHub, ILogHubClient> hub, ComboLogRequestedEvent evt)
    {
        await hub.Clients.All.OnComboLogRequested(evt);
    }

    public static async Task BroadcastQso(this IHubContext<LogHub, ILogHubClient> hub, QsoLoggedEvent evt)
    {
        await hub.Clients.All.OnQsoLogged(evt);
    }

    public static async Task BroadcastContestState(this IHubContext<LogHub, ILogHubClient> hub, SDRLoggerPlus.Contracts.Api.ContestStateDto state)
    {
        await hub.Clients.All.OnContestState(state);
    }

    public static async Task BroadcastCallsignLookup(this IHubContext<LogHub, ILogHubClient> hub, CallsignLookedUpEvent evt)
    {
        await hub.Clients.All.OnCallsignLookedUp(evt);
    }

    public static async Task BroadcastRotatorPosition(this IHubContext<LogHub, ILogHubClient> hub, RotatorPositionEvent evt)
    {
        await hub.Clients.All.OnRotatorPosition(evt);
    }

    // Radio CAT Control extensions
    public static async Task BroadcastRadioDiscovered(this IHubContext<LogHub, ILogHubClient> hub, RadioDiscoveredEvent evt)
    {
        await hub.Clients.All.OnRadioDiscovered(evt);
    }

    public static async Task BroadcastRadioRemoved(this IHubContext<LogHub, ILogHubClient> hub, RadioRemovedEvent evt)
    {
        await hub.Clients.All.OnRadioRemoved(evt);
    }

    public static async Task BroadcastRadioConnectionStateChanged(this IHubContext<LogHub, ILogHubClient> hub, RadioConnectionStateChangedEvent evt)
    {
        await hub.Clients.All.OnRadioConnectionStateChanged(evt);
    }

    public static async Task BroadcastRadioStateChanged(this IHubContext<LogHub, ILogHubClient> hub, RadioStateChangedEvent evt)
    {
        await hub.Clients.All.OnRadioStateChanged(evt);
    }

    public static async Task BroadcastTciMeters(this IHubContext<LogHub, ILogHubClient> hub, TciMetersEvent evt)
    {
        await hub.Clients.All.OnTciMeters(evt);
    }

    public static async Task BroadcastQrzSyncProgress(this IHubContext<LogHub, ILogHubClient> hub, QrzSyncProgressEvent evt)
    {
        await hub.Clients.All.OnQrzSyncProgress(evt);
    }

    public static async Task BroadcastAdifImportProgress(this IHubContext<LogHub, ILogHubClient> hub, AdifImportProgressEvent evt)
    {
        await hub.Clients.All.OnAdifImportProgress(evt);
    }

    public static async Task BroadcastLotwUploadProgress(this IHubContext<LogHub, ILogHubClient> hub, LotwUploadProgressEvent evt)
    {
        await hub.Clients.All.OnLotwUploadProgress(evt);
    }

    public static async Task BroadcastClusterStatusChanged(this IHubContext<LogHub, ILogHubClient> hub, ClusterStatusChangedEvent evt)
    {
        await hub.Clients.All.OnClusterStatusChanged(evt);
    }

    public static async Task BroadcastSpectrumData(this IHubContext<LogHub, ILogHubClient> hub, SpectrumDataEvent evt)
    {
        await hub.Clients.All.OnSpectrumData(evt);
    }

    public static async Task BroadcastLightningStrikes(this IHubContext<LogHub, ILogHubClient> hub, LightningStrikesEvent evt)
    {
        await hub.Clients.All.OnLightningStrikes(evt);
    }
}
