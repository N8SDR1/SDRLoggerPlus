using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Orchestrates the live contest flow: logging QSOs from the contest entry
/// window (CTY enrichment + engine evaluation + serial allocation + persistence),
/// live dupe/mult checks while typing, and pushing <see cref="ContestStateDto"/>
/// over SignalR after each change. Pure rule logic stays in
/// <see cref="ContestScoringEngine"/>; this class is the I/O glue.
/// </summary>
public class ContestService
{
    private readonly IQsoRepository _qsos;
    private readonly IContestSessionRepository _sessions;
    private readonly ContestDefinitionService _definitions;
    private readonly ISettingsService _settings;
    private readonly ScpService _scp;
    private readonly CallHistoryService _callHistory;
    private readonly ContestBroadcastService _broadcast;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;
    private readonly ILogger<ContestService> _logger;

    public ContestService(
        IQsoRepository qsos,
        IContestSessionRepository sessions,
        ContestDefinitionService definitions,
        ISettingsService settings,
        ScpService scp,
        CallHistoryService callHistory,
        ContestBroadcastService broadcast,
        IHubContext<LogHub, ILogHubClient> hub,
        ILogger<ContestService> logger)
    {
        _qsos = qsos;
        _sessions = sessions;
        _definitions = definitions;
        _settings = settings;
        _scp = scp;
        _callHistory = callHistory;
        _broadcast = broadcast;
        _hub = hub;
        _logger = logger;
    }

    /// <summary>
    /// Super Check Partial call set: the master.scp file merged with the
    /// operator's own logged callsigns, so suggestions work even without a file.
    /// The client caches this and matches partial calls locally.
    /// </summary>
    public async Task<List<string>> GetScpCallsAsync()
    {
        var logged = await _qsos.GetDistinctCallsignsAsync();
        return _scp.MasterCalls.Concat(logged)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(c => c, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>Current state for the active session, or null when none is active.</summary>
    public async Task<ContestStateDto?> GetStateAsync()
    {
        var session = await _sessions.GetActiveAsync();
        if (session == null) return null;
        var def = _definitions.Get(session.DefinitionId);
        if (def == null) return null;
        var log = await _qsos.GetByContestSessionAsync(session.Id);
        return BuildState(session, def, log);
    }

    /// <summary>Live dupe/mult check for a call at the given band/mode (typing feedback).</summary>
    public async Task<ContestCheckResponse> CheckAsync(string callsign, string band, string mode)
    {
        var session = await _sessions.GetActiveAsync()
            ?? throw new ContestDefinitionException("No active contest session.");
        var def = _definitions.Get(session.DefinitionId)
            ?? throw new ContestDefinitionException($"Unknown contest '{session.DefinitionId}'.");

        var log = await _qsos.GetByContestSessionAsync(session.Id);
        var candidate = BuildQso(session, def, new LogContestQsoRequest(callsign, band, mode), enrich: true);
        var eval = ContestScoringEngine.Evaluate(def, session.MyExchange, log, candidate);

        var call = callsign.Trim().ToUpperInvariant();
        var workedCount = log.Count(q => string.Equals(q.Callsign, call, StringComparison.OrdinalIgnoreCase));
        var prefill = await BuildPrefillAsync(def, call);
        return new ContestCheckResponse(eval.IsDupe, workedCount, eval.Mults, prefill);
    }

    /// <summary>
    /// Exchange prefill for a call: the call-history file takes priority, then the
    /// most recent prior QSO. Only fields the active definition actually receives
    /// are returned, keyed by field key. Null when nothing is known.
    /// </summary>
    private async Task<Dictionary<string, string>?> BuildPrefillAsync(ContestDefinition def, string call)
    {
        var wanted = def.RcvdExchange
            .Where(f => f.Type != ContestFieldType.Rst && f.Type != ContestFieldType.Serial)
            .Select(f => f.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (wanted.Count == 0) return null;

        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        // 1) Call-history file.
        var fromFile = _callHistory.Lookup(call);
        if (fromFile != null)
            foreach (var (k, v) in fromFile)
                if (wanted.Contains(k)) result[k] = v;

        // 2) Fill gaps from the most recent prior QSO with this call.
        if (result.Count < wanted.Count)
        {
            var prior = await _qsos.GetMostRecentByCallsignAsync(call);
            if (prior != null)
            {
                void Fill(string key, string? value)
                {
                    if (wanted.Contains(key) && !result.ContainsKey(key) && !string.IsNullOrWhiteSpace(value))
                        result[key] = value!;
                }
                Fill("name", prior.Contest?.RcvdName ?? prior.Name ?? prior.Station?.Name);
                Fill("state", prior.Contest?.RcvdState ?? prior.Station?.State);
                Fill("section", prior.Contest?.RcvdSection);
                Fill("grid", prior.Contest?.RcvdGrid ?? prior.Grid ?? prior.Station?.Grid);
                Fill("zone", prior.Contest?.RcvdZone ?? prior.Station?.CqZone?.ToString());
                Fill("power", prior.Contest?.RcvdPower);
            }
        }

        return result.Count > 0 ? result : null;
    }

    /// <summary>Log a contest QSO: enrich, evaluate, allocate serial, persist, broadcast.</summary>
    public async Task<ContestLogResult> LogQsoAsync(LogContestQsoRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Callsign))
            throw new ContestDefinitionException("Callsign is required.");

        var session = await _sessions.GetActiveAsync()
            ?? throw new ContestDefinitionException("No active contest session.");
        var def = _definitions.Get(session.DefinitionId)
            ?? throw new ContestDefinitionException($"Unknown contest '{session.DefinitionId}'.");

        var log = await _qsos.GetByContestSessionAsync(session.Id);
        var qso = BuildQso(session, def, request, enrich: true);
        var eval = ContestScoringEngine.Evaluate(def, session.MyExchange, log, qso);

        // Serial: allocated even for dupes (the number was sent on the air).
        if (def.Serial != SerialMode.None)
        {
            var serial = ContestSerials.Allocate(session, def.Serial, request.Band);
            qso.Contest!.SerialSent = ContestSerials.Format(serial);
            await _sessions.UpsertAsync(session);
        }

        // Snapshot the evaluation so exports don't need to recompute.
        qso.Contest!.QsoPoints = eval.Points;
        qso.Contest.IsDupe = eval.IsDupe;
        qso.Contest.Mults = eval.Mults.Count > 0 ? eval.Mults : null;

        var created = await _qsos.CreateAsync(qso);
        log.Add(created);
        _logger.LogInformation("Contest QSO {Call} on {Band} ({Points} pts, dupe={Dupe})",
            created.Callsign, created.Band, eval.Points, eval.IsDupe);

        var state = BuildState(session, def, log);

        // Broadcast like the normal log path so Log History etc. stay live,
        // plus the contest-specific state for score/mult panels.
        await _hub.BroadcastQso(new QsoLoggedEvent(
            created.Id, created.Callsign, created.QsoDate, created.TimeOn,
            created.Band, created.Mode, created.Frequency,
            created.RstSent, created.RstRcvd, created.Station?.Grid));
        await _hub.BroadcastContestState(state);

        // Fire-and-forget N1MM UDP / online-score interop — never block logging.
        // Snapshot settings in-scope; the background task holds no scoped services.
        var settings = await _settings.GetSettingsAsync();
        var myCall = settings.Station.Callsign;
        if (!string.IsNullOrWhiteSpace(myCall) && (settings.Contest.N1mmUdpEnabled || settings.Contest.OnlineScoreEnabled))
            _ = Task.Run(() => _broadcast.OnQsoLoggedAsync(settings.Contest, def, session, created, state, myCall!));

        return new ContestLogResult(created.Id, eval.IsDupe, eval.Points, eval.Mults, state);
    }

    /// <summary>Generate the Cabrillo log for a session. Returns (filename, content).</summary>
    public async Task<(string FileName, string Content)> GenerateCabrilloAsync(string sessionId)
    {
        var session = await _sessions.GetByIdAsync(sessionId)
            ?? throw new ContestDefinitionException($"No session '{sessionId}'.");
        var def = _definitions.Get(session.DefinitionId)
            ?? throw new ContestDefinitionException($"Unknown contest '{session.DefinitionId}'.");

        var qsos = await _qsos.GetByContestSessionAsync(session.Id);
        var settings = await _settings.GetSettingsAsync();
        var call = settings.Station.Callsign;
        if (string.IsNullOrWhiteSpace(call))
            throw new ContestDefinitionException("Set your station callsign in Settings before exporting Cabrillo.");

        var content = CabrilloExporter.Generate(def, session, qsos, call!, settings.Station.GridSquare);
        var fileName = $"{call!.ToUpperInvariant()}_{def.Id}.cbr";
        return (fileName, content);
    }

    /// <summary>Push current state to all clients (after session start/activate/stop).</summary>
    public async Task BroadcastStateAsync()
    {
        var state = await GetStateAsync();
        if (state != null)
            await _hub.BroadcastContestState(state);
    }

    // -- helpers -------------------------------------------------------------

    private static Qso BuildQso(ContestSession session, ContestDefinition def, LogContestQsoRequest request, bool enrich)
    {
        var now = DateTime.UtcNow;
        var exchange = request.Exchange ?? new Dictionary<string, string>();
        string? Ex(string key) => exchange.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v) ? v.Trim() : null;

        var qso = new Qso
        {
            Callsign = request.Callsign.Trim().ToUpperInvariant(),
            QsoDate = now.Date,
            TimeOn = now.ToString("HHmmss"),
            Band = request.Band,
            Mode = request.Mode,
            Frequency = request.Frequency,
            RstSent = request.RstSent,
            RstRcvd = Ex("rst"),
            Station = new StationInfo(),
            Contest = new ContestInfo
            {
                ContestId = def.Id,
                SessionId = session.Id,
                SerialRcvd = Ex("serial"),
                RcvdZone = Ex("zone"),
                RcvdState = Ex("state"),
                RcvdSection = Ex("section"),
                RcvdName = Ex("name"),
                RcvdPower = Ex("power"),
                RcvdGrid = Ex("grid")?.ToUpperInvariant(),
                Exchange = exchange.Count > 0
                    ? string.Join(" ", def.RcvdExchange
                        .Select(f => Ex(f.Key))
                        .Where(v => v != null))
                    : null,
            },
        };

        if (enrich)
        {
            // CTY lookup for country/continent/zone — the received zone (what the
            // other station actually sent) overrides the CTY estimate for scoring.
            var (country, continent, cqZone) = CtyService.GetEntityFromCallsign(qso.Callsign);
            qso.Country = country;
            qso.Continent = continent;
            qso.Station!.Country = country;
            qso.Station.Continent = continent;
            qso.Station.CqZone = int.TryParse(Ex("zone"), out var z) ? z : cqZone;
            if (Ex("state") != null) qso.Station.State = Ex("state")!.ToUpperInvariant();
        }

        return qso;
    }

    private ContestStateDto BuildState(ContestSession session, ContestDefinition def, List<Qso> log)
    {
        var summary = ContestScoringEngine.Recompute(def, session.MyExchange, log);

        // Rates from wall-clock timestamps (CreatedAt is set at log time).
        var now = DateTime.UtcNow;
        var lastHour = log.Count(q => q.CreatedAt >= now.AddHours(-1));
        double rate10 = 0;
        var recent = log.Count >= 2 ? log.Skip(Math.Max(0, log.Count - 10)).ToList() : null;
        if (recent is { Count: >= 2 })
        {
            var span = (now - recent[0].CreatedAt).TotalHours;
            if (span > 0.0005) rate10 = recent.Count / span;
        }

        return new ContestStateDto(
            SessionId: session.Id,
            DefinitionId: def.Id,
            DefinitionName: def.Name,
            Label: session.Label,
            SerialInUse: def.Serial != SerialMode.None,
            NextSerial: ContestSerials.Peek(session, def.Serial, session.BandFilter ?? ""),
            Qsos: summary.Qsos,
            Dupes: summary.Dupes,
            Points: summary.Points,
            Multipliers: summary.Multipliers,
            Score: summary.Score,
            RateLastHour: lastHour,
            RateLast10: Math.Round(rate10, 1),
            MultsBySource: summary.MultsBySource);
    }
}
