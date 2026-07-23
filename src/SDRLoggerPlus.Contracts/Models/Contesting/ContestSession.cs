namespace SDRLoggerPlus.Contracts.Models.Contesting;

/// <summary>
/// A running (or finished) instance of operating a contest. Persisted in the
/// LiteDB <c>contest_sessions</c> collection. QSOs logged from the contest window
/// carry this session's id in <see cref="ContestInfo.SessionId"/>.
/// </summary>
public class ContestSession
{
    /// <summary>LiteDB maps the <c>Id</c> property to <c>_id</c> by convention.</summary>
    public string Id { get; set; } = Guid.NewGuid().ToString("N");

    /// <summary>The <see cref="ContestDefinition.Id"/> this session runs.</summary>
    public string DefinitionId { get; set; } = string.Empty;

    /// <summary>Operator-facing label, e.g. "CQ WW CW 2026".</summary>
    public string Label { get; set; } = string.Empty;

    /// <summary>The operator's own exchange / station facts, used for scoring relations.</summary>
    public MyExchange MyExchange { get; set; } = new();

    /// <summary>
    /// The operator's role for this contest, resolved at start from the
    /// definition's HomeArea + MyExchange. Stored so scoring stays consistent even
    /// if station facts are edited later. <see cref="ContestRole.All"/> for
    /// contests without a role split.
    /// </summary>
    public ContestRole Role { get; set; } = ContestRole.All;

    public DateTime StartedAt { get; set; } = DateTime.UtcNow;
    public DateTime? EndedAt { get; set; }

    /// <summary>Only one session is active at a time; the active id also mirrors to settings.</summary>
    public bool Active { get; set; }

    /// <summary>Next serial for all-band mode (used when Serial = AllBand).</summary>
    public int NextSerialAllBand { get; set; } = 1;

    /// <summary>Next serial per band (used when Serial = PerBand).</summary>
    public Dictionary<string, int> NextSerialPerBand { get; set; } = new();

    public string? BandFilter { get; set; }
    public string? ModeFilter { get; set; }
}

/// <summary>The operator's own station facts for a session (drives scoring relations).</summary>
public class MyExchange
{
    public int? Dxcc { get; set; }

    /// <summary>CTY country name fallback when the ADIF entity number is unknown.</summary>
    public string? Country { get; set; }

    public string? Continent { get; set; }
    public int? CqZone { get; set; }
    public int? ItuZone { get; set; }
    public string? State { get; set; }

    /// <summary>The operator's own county (in-area QSO-party sent exchange).</summary>
    public string? County { get; set; }

    public string? Section { get; set; }
    public string? Grid { get; set; }

    /// <summary>Cabrillo CATEGORY-* / operator category free-form fields.</summary>
    public string? Category { get; set; }
    public string? Power { get; set; }
    public string? Name { get; set; }

    /// <summary>
    /// Operator-declared role, overriding the location-based guess: InArea/OutArea
    /// for a StateCounty-kind contest, InArea/Dx for a WVE-kind one. When set,
    /// <c>ContestScoringEngine.DetermineRole</c> uses it instead of inferring from
    /// <see cref="State"/> vs the contest's home area — so a station operating
    /// portable, or on a border, can pick correctly. Null (or All) means auto-derive.
    /// </summary>
    public ContestRole? RoleOverride { get; set; }

    /// <summary>
    /// Operator-declared bonus/objective points, added to the final score after the
    /// power multiplier. Only collected when the definition sets
    /// <see cref="ContestDefinition.BonusPointsHint"/> (e.g. Winter Field Day's
    /// alternate-power / away / satellite objectives, which are self-declared, not
    /// derived from the QSO log). Null ⇒ none claimed.
    /// </summary>
    public int? BonusPoints { get; set; }
}
