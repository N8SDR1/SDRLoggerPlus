using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Contracts.Api;

/// <summary>Start a new contest session (and make it active).</summary>
public record StartContestSessionRequest(
    string DefinitionId,
    MyExchange MyExchange,
    string? Label = null
);

/// <summary>
/// Log a QSO from the contest entry window. Exchange carries the received-
/// exchange field values keyed by the definition's field keys (e.g.
/// {"rst":"599","zone":"14"}). Serial-sent is allocated server-side.
/// </summary>
public record LogContestQsoRequest(
    string Callsign,
    string Band,
    string Mode,
    double? Frequency = null,
    string? RstSent = null,
    Dictionary<string, string>? Exchange = null
);

/// <summary>Live dupe/mult check while typing a call.</summary>
public record ContestCheckResponse(
    bool IsDupe,
    int WorkedCount,
    List<string> NewMults,
    // Prefill values for the received-exchange fields (by field key), from the
    // call-history file or the most recent prior QSO with this call. Empty if unknown.
    Dictionary<string, string>? Prefill = null,
    // How the engine classifies the worked station (InArea = W/VE, Dx/OutArea = DX,
    // All = the contest has no home-area split). Drives per-QSO exchange branching
    // in the entry window (state box vs serial box).
    ContestRole WorkedClass = ContestRole.All
);

/// <summary>Batch dupe/new-mult check for a bandmap's spots.</summary>
public record ContestBatchCheckRequest(List<BatchCheckItem> Items);
public record BatchCheckItem(string Call, string Band, string Mode);
public record BatchCheckEntry(string Call, bool IsDupe, bool IsNewMult);

/// <summary>Correct a busted call / exchange on an already-logged contest QSO.</summary>
public record UpdateContestQsoRequest(
    string Callsign,
    Dictionary<string, string>? Exchange = null
);

/// <summary>A logged contest QSO for the recent-QSO strip (edit affordance).</summary>
public record ContestQsoDto(
    string Id,
    string Callsign,
    string Band,
    string Mode,
    string TimeOn,
    int Points,
    bool IsDupe,
    Dictionary<string, string>? Exchange
);

/// <summary>Result of logging a contest QSO: per-QSO evaluation + fresh state.</summary>
public record ContestLogResult(
    string QsoId,
    bool IsDupe,
    int Points,
    List<string> NewMults,
    ContestStateDto State
);

/// <summary>
/// Pushed over SignalR after every contest QSO (and returned by GET state):
/// everything the contest window + score panel need to render.
/// </summary>
public record ContestStateDto(
    string SessionId,
    string DefinitionId,
    string DefinitionName,
    string Label,
    ContestRole Role,
    bool SerialInUse,
    int NextSerial,
    int Qsos,
    int Dupes,
    int Points,
    int Multipliers,
    int BonusPoints,
    int Score,
    double RateLastHour,
    double RateLast10,
    Dictionary<string, List<string>> MultsBySource,
    // True when this active session has had no activity for a long time (see
    // ContestService.StaleAfter) — the client shows a "resume?" prompt instead of
    // auto-opening the entry window, so a contest you forgot to end last year
    // doesn't silently reappear with all its calls.
    bool IsStale,
    // ISO-8601 (UTC) session start, for the resume prompt.
    string StartedAt
);
