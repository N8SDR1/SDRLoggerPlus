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
        var workedClass = ContestScoringEngine.ClassifyWorked(def, candidate);
        return new ContestCheckResponse(eval.IsDupe, workedCount, eval.Mults, prefill, workedClass);
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

    /// <summary>Batch dupe/new-mult check for a set of spots (bandmap coloring).</summary>
    public async Task<List<BatchCheckEntry>> CheckBatchAsync(IReadOnlyList<BatchCheckItem> items)
    {
        var session = await _sessions.GetActiveAsync();
        var def = session == null ? null : _definitions.Get(session.DefinitionId);
        if (session == null || def == null)
            return items.Select(i => new BatchCheckEntry(i.Call, false, false)).ToList();

        var log = await _qsos.GetByContestSessionAsync(session.Id);
        var candidates = items.Select(i => BuildQso(session, def,
            new LogContestQsoRequest(i.Call, i.Band, i.Mode), enrich: true)).ToList();
        var evals = ContestScoringEngine.EvaluateBatch(def, session.MyExchange, log, candidates);

        // evals is aligned to items order.
        return items.Select((i, idx) =>
        {
            var e = evals[idx];
            return new BatchCheckEntry(i.Call, e.IsDupe, !e.IsDupe && e.Mults.Count > 0);
        }).ToList();
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

    /// <summary>Most-recent QSOs of the active session, for the entry window's edit strip.</summary>
    public async Task<List<ContestQsoDto>> GetSessionQsosAsync(int limit = 8)
    {
        var session = await _sessions.GetActiveAsync();
        if (session == null) return new();
        var log = await _qsos.GetByContestSessionAsync(session.Id);
        return log.AsEnumerable().Reverse().Take(limit).Select(q => new ContestQsoDto(
            q.Id, q.Callsign ?? string.Empty, q.Band ?? string.Empty, q.Mode ?? string.Empty,
            q.TimeOn ?? string.Empty, q.Contest?.QsoPoints ?? 0, q.Contest?.IsDupe ?? false,
            q.Contest?.RcvdFields)).ToList();
    }

    /// <summary>
    /// Correct a busted call / exchange on a logged QSO of the active session, then
    /// replay the whole session so dupe/points/mults (which depend on order and the
    /// other QSOs) stay consistent, and broadcast the refreshed state.
    /// </summary>
    public async Task<ContestStateDto> UpdateQsoAsync(string id, UpdateContestQsoRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Callsign))
            throw new ContestDefinitionException("Callsign is required.");

        var session = await _sessions.GetActiveAsync()
            ?? throw new ContestDefinitionException("No active contest session.");
        var def = _definitions.Get(session.DefinitionId)
            ?? throw new ContestDefinitionException($"Unknown contest '{session.DefinitionId}'.");

        var qso = await _qsos.GetByIdAsync(id)
            ?? throw new ContestDefinitionException("QSO not found.");
        if (qso.Contest?.SessionId != session.Id)
            throw new ContestDefinitionException("QSO is not part of the active session.");

        var exchange = request.Exchange ?? new Dictionary<string, string>();
        string? Ex(string key) => exchange.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v) ? v.Trim() : null;

        qso.Callsign = request.Callsign.Trim().ToUpperInvariant();
        var (country, continent, cqZone) = CtyService.GetEntityFromCallsign(qso.Callsign);
        qso.Country = country;
        qso.Continent = continent;
        qso.Station ??= new StationInfo();
        qso.Station.Country = country;
        qso.Station.Continent = continent;

        if (request.Exchange != null)
        {
            qso.RstRcvd = Ex("rst") ?? qso.RstRcvd;
            qso.Contest!.SerialRcvd = Ex("serial") ?? qso.Contest.SerialRcvd;
            qso.Contest.RcvdZone = Ex("zone");
            qso.Contest.RcvdState = Ex("state");
            qso.Contest.RcvdSection = Ex("section");
            qso.Contest.RcvdName = Ex("name");
            qso.Contest.RcvdPower = Ex("power");
            qso.Contest.RcvdGrid = Ex("grid")?.ToUpperInvariant();
            qso.Contest.RcvdFields = exchange.Count > 0
                ? exchange.Where(kv => !string.IsNullOrWhiteSpace(kv.Value))
                    .ToDictionary(kv => kv.Key, kv => kv.Value.Trim(), StringComparer.OrdinalIgnoreCase)
                : null;
            qso.Station.CqZone = int.TryParse(Ex("zone"), out var z) ? z : cqZone;
            if (Ex("state") != null) qso.Station.State = Ex("state")!.ToUpperInvariant();
        }
        await _qsos.UpdateAsync(id, qso);

        var state = await ReplayAndBroadcastAsync(session, def);
        _logger.LogInformation("Edited contest QSO {Id} -> {Call}", id, qso.Callsign);
        return state;
    }

    /// <summary>
    /// Delete a logged QSO of the active session (busted entry from the entry
    /// window), then replay so dupe/points/mults on the remaining QSOs stay
    /// consistent (a later QSO that was a dupe only because of the deleted one is no
    /// longer a dupe), and broadcast the refreshed state.
    /// </summary>
    public async Task<ContestStateDto> DeleteQsoAsync(string id)
    {
        var session = await _sessions.GetActiveAsync()
            ?? throw new ContestDefinitionException("No active contest session.");
        var def = _definitions.Get(session.DefinitionId)
            ?? throw new ContestDefinitionException($"Unknown contest '{session.DefinitionId}'.");

        var qso = await _qsos.GetByIdAsync(id)
            ?? throw new ContestDefinitionException("QSO not found.");
        if (qso.Contest?.SessionId != session.Id)
            throw new ContestDefinitionException("QSO is not part of the active session.");

        await _qsos.DeleteAsync(id);
        var state = await ReplayAndBroadcastAsync(session, def);
        _logger.LogInformation("Deleted contest QSO {Id} ({Call})", id, qso.Callsign);
        return state;
    }

    /// <summary>
    /// Recompute and rebroadcast the active session, keeping the per-QSO snapshots
    /// and score state in sync after a change made outside the contest entry window
    /// (a QSO deleted or edited in the logbook, or the operator's own exchange
    /// changed mid-session). No-op when no session is active. Returns the refreshed
    /// state, or null.
    /// </summary>
    public async Task<ContestStateDto?> RefreshActiveSessionAsync()
    {
        var session = await _sessions.GetActiveAsync();
        if (session == null) return null;
        var def = _definitions.Get(session.DefinitionId);
        if (def == null) return null;
        return await ReplayAndBroadcastAsync(session, def);
    }

    /// <summary>
    /// Replay the active session's QSOs in order, re-snapshotting each one's
    /// evaluation (points/dupe/mults depend on the QSOs before it), persist the
    /// refreshed snapshots, then build and broadcast the state.
    /// </summary>
    private async Task<ContestStateDto> ReplayAndBroadcastAsync(ContestSession session, ContestDefinition def)
    {
        var log = await _qsos.GetByContestSessionAsync(session.Id);
        for (var i = 0; i < log.Count; i++)
        {
            var eval = ContestScoringEngine.Evaluate(def, session.MyExchange, log.Take(i).ToList(), log[i]);
            if (log[i].Contest is not { } c) continue;
            c.QsoPoints = eval.Points;
            c.IsDupe = eval.IsDupe;
            c.Mults = eval.Mults.Count > 0 ? eval.Mults : null;
            await _qsos.UpdateAsync(log[i].Id, log[i]);
        }

        var state = BuildState(session, def, log);
        await _hub.BroadcastContestState(state);
        return state;
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
        // The Cabrillo entrant identity is the call OPERATED under this session (club /P / special),
        // NOT the operator's global station call — else a club Field Day is mis-headed as the op.
        var call = !string.IsNullOrWhiteSpace(session.OperatingCallsign)
            ? session.OperatingCallsign!
            : settings.Station.Callsign;
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

    internal static Qso BuildQso(ContestSession session, ContestDefinition def, LogContestQsoRequest request, bool enrich)
    {
        var now = DateTime.UtcNow;
        var exchange = request.Exchange ?? new Dictionary<string, string>();
        string? Ex(string key) => exchange.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v) ? v.Trim() : null;

        var qso = new Qso
        {
            Callsign = request.Callsign.Trim().ToUpperInvariant(),
            // Keep the time of day: ADIF export derives both QSO_DATE and TIME_ON
            // from QsoDate, so truncating to .Date here exported every contest QSO
            // as TIME_ON=000000 no matter what TimeOn held.
            QsoDate = now,
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
                // Stamp the session's operating call on every contest QSO (store-always, even when
                // it equals the personal call, so Stage-2 filtering by call is clean).
                StationCallsign = session.OperatingCallsign,
                SerialRcvd = Ex("serial"),
                RcvdZone = Ex("zone"),
                // Uppercased like RcvdGrid below: state/province codes are canonically
                // upper ("OH"), and the entry field's "uppercase" styling is CSS-only —
                // it doesn't touch the stored value, so an unshifted keystroke would
                // otherwise persist lowercase and silently miss exact-match lookups
                // (e.g. the Multipliers panel's needed-state grid).
                RcvdState = Ex("state")?.ToUpperInvariant(),
                RcvdSection = Ex("section"),
                RcvdName = Ex("name"),
                RcvdPower = Ex("power"),
                RcvdGrid = Ex("grid")?.ToUpperInvariant(),
                Exchange = exchange.Count > 0
                    ? string.Join(" ", def.RcvdExchange
                        .Select(f => Ex(f.Key))
                        .Where(v => v != null))
                    : null,
                // Keep every received field (incl. non-typed ones like age/check/
                // member#) so Cabrillo can emit the full exchange.
                RcvdFields = exchange.Count > 0
                    ? exchange.Where(kv => !string.IsNullOrWhiteSpace(kv.Value))
                        .ToDictionary(kv => kv.Key, kv => kv.Value.Trim(), StringComparer.OrdinalIgnoreCase)
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

            // Now that CTY enrichment has resolved the worked station's country, we
            // know its class, so drop any received field that doesn't apply to it.
            SanitizeReceivedExchange(def, qso);
        }

        return qso;
    }

    /// <summary>
    /// Strip received-exchange values that don't apply to the worked station's class
    /// on per-QSO-branching contests (ARRL 10 m / 160 m / RTTY Roundup). The client
    /// prunes too, but its classification comes from a debounced check that can lag a
    /// fast log entry — so a DX station could arrive carrying a stale in-area
    /// state/section. Left in, scoring would claim both that state/section mult and a
    /// DXCC mult for one QSO. The server is authoritative and re-derives the class
    /// from enriched country data. Only meaningful once <see cref="Qso.Country"/> is
    /// resolved, so callers invoke this after enrichment.
    /// </summary>
    internal static void SanitizeReceivedExchange(ContestDefinition def, Qso qso)
    {
        var workedClass = ContestScoringEngine.ClassifyWorked(def, qso);
        if (workedClass == ContestRole.All) return; // no per-QSO field branching

        var c = qso.Contest;
        if (c is null) return;

        static bool Applies(ContestField f, ContestRole cls) =>
            f.AppliesTo is null or ContestRole.All || f.AppliesTo == cls;

        foreach (var f in def.RcvdExchange.Where(f => !Applies(f, workedClass)))
        {
            switch (f.Type)
            {
                case ContestFieldType.Serial: c.SerialRcvd = null; break;
                case ContestFieldType.Zone: c.RcvdZone = null; break;
                case ContestFieldType.State: c.RcvdState = null; break;
                case ContestFieldType.Section: c.RcvdSection = null; break;
                case ContestFieldType.Name: c.RcvdName = null; break;
                case ContestFieldType.Power: c.RcvdPower = null; break;
                case ContestFieldType.Grid: c.RcvdGrid = null; break;
            }
            c.RcvdFields?.Remove(f.Key);
        }
    }

    // An active session with no activity for this long is treated as stale: the
    // client shows a resume prompt rather than dropping straight into the entry
    // window. 72h clears the longest contests (48h) so a real run in progress is
    // never interrupted, yet catches a session left over from a past event.
    internal static readonly TimeSpan StaleAfter = TimeSpan.FromHours(72);

    /// <summary>
    /// Whether an active session should be treated as stale (idle beyond
    /// <see cref="StaleAfter"/>). Last activity is the most recent QSO, or the
    /// session start when it has none. Pure so it can be unit-tested directly.
    /// </summary>
    internal static bool IsSessionStale(DateTime startedAt, IReadOnlyCollection<DateTime> qsoTimesUtc, DateTime nowUtc)
    {
        var lastActivity = qsoTimesUtc.Count > 0 ? qsoTimesUtc.Max() : startedAt;
        return nowUtc - lastActivity > StaleAfter;
    }

    private ContestStateDto BuildState(ContestSession session, ContestDefinition def, List<Qso> log)
    {
        var summary = ContestScoringEngine.Recompute(def, session.MyExchange, log);

        // Rates from wall-clock timestamps. CreatedAt/StartedAt come back from LiteDB
        // as *local* time (Kind=Local), so compare in the same frame — using UtcNow
        // here made every QSO read ~(UTC offset) hours old, so the last-hour count
        // (and the rate) was permanently 0.
        var now = DateTime.Now;
        var lastHour = log.Count(q => q.CreatedAt.ToLocalTime() >= now.AddHours(-1));

        var isStale = IsSessionStale(
            session.StartedAt.ToLocalTime(),
            log.Select(q => q.CreatedAt.ToLocalTime()).ToList(),
            now);
        double rate10 = 0;
        var recent = log.Count >= 2 ? log.Skip(Math.Max(0, log.Count - 10)).ToList() : null;
        if (recent is { Count: >= 2 })
        {
            var span = (now - recent[0].CreatedAt.ToLocalTime()).TotalHours;
            if (span > 0.0005) rate10 = recent.Count / span;
        }

        return new ContestStateDto(
            SessionId: session.Id,
            DefinitionId: def.Id,
            DefinitionName: def.Name,
            Label: session.Label,
            Role: session.Role,
            SerialInUse: def.Serial != SerialMode.None,
            NextSerial: ContestSerials.Peek(session, def.Serial, session.BandFilter ?? ""),
            Qsos: summary.Qsos,
            Dupes: summary.Dupes,
            Points: summary.Points,
            Multipliers: summary.Multipliers,
            BonusPoints: summary.BonusPoints,
            Score: summary.Score,
            RateLastHour: lastHour,
            RateLast10: Math.Round(rate10, 1),
            MultsBySource: summary.MultsBySource,
            IsStale: isStale,
            StartedAt: session.StartedAt.ToString("o"));
    }
}
