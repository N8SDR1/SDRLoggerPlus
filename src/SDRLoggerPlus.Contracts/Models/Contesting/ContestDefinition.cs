using System.Text.Json.Serialization;

namespace SDRLoggerPlus.Contracts.Models.Contesting;

/// <summary>
/// Declarative, data-driven description of a contest's rules. Built-in
/// definitions ship read-only with the app; user definitions live under
/// %APPDATA%\SDRLoggerPlus\contests\ and are editable/deletable.
///
/// The scoring engine (<c>ContestScoringEngine</c>) evaluates a QSO purely from
/// this definition plus the operator's own exchange, so ~all common contests are
/// expressible without code. <see cref="ScoringStrategyId"/> is an escape hatch
/// for the rare contest whose rules cannot be expressed declaratively.
/// </summary>
public class ContestDefinition
{
    /// <summary>Stable slug, e.g. "cq-ww-cw".</summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>Display name, e.g. "CQ WW DX CW".</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Cabrillo CONTEST: token, e.g. "CQ-WW-CW".</summary>
    public string CabrilloName { get; set; } = string.Empty;

    /// <summary>True for seeded definitions (read-only, clone-only, never deletable).</summary>
    public bool Builtin { get; set; }

    public List<string> Bands { get; set; } = new();
    public List<string> Modes { get; set; } = new();

    /// <summary>Fields the operator sends (informational + Cabrillo mapping).</summary>
    public List<ContestField> SentExchange { get; set; } = new();

    /// <summary>Fields captured per QSO (drive the entry window + scoring).</summary>
    public List<ContestField> RcvdExchange { get; set; } = new();

    public PointsRule QsoPoints { get; set; } = new();

    public List<MultRule> MultiplierRules { get; set; } = new();

    public DupeRule DupeRule { get; set; } = DupeRule.PerBandMode;

    public SerialMode Serial { get; set; } = SerialMode.None;

    /// <summary>
    /// Defines the contest's "home area" for role-based rules (QSO parties, ARRL
    /// DX, CQ 160). Null ⇒ no role split; every operator is
    /// <see cref="ContestRole.All"/> and uses the top-level fields above.
    /// </summary>
    public HomeArea? HomeArea { get; set; }

    /// <summary>
    /// Per-role rule overrides. The operator's role is resolved at session start
    /// from <see cref="HomeArea"/> and the operator's own location; a missing role
    /// — or a missing field within a role — falls back to the top-level
    /// SentExchange/RcvdExchange/QsoPoints/MultiplierRules, so contests without a
    /// role split need no entry here.
    /// </summary>
    public Dictionary<ContestRole, RoleRules>? Roles { get; set; }

    /// <summary>Ordered mapping producing the Cabrillo QSO: line.</summary>
    public List<CabrilloColumn> CabrilloMap { get; set; } = new();

    /// <summary>Optional registered C# strategy id for exotic scoring.</summary>
    public string? ScoringStrategyId { get; set; }
}

/// <summary>A single exchange field, sent or received.</summary>
public class ContestField
{
    /// <summary>Machine key, e.g. "rst", "serial", "zone", "state", "section".</summary>
    public string Key { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public ContestFieldType Type { get; set; } = ContestFieldType.Text;
    public int Width { get; set; } = 6;
    public bool Required { get; set; }

    /// <summary>Optional named validator, e.g. "cqZone", "arrlSection".</summary>
    public string? Validate { get; set; }

    /// <summary>Optional call-history key to prefill from, e.g. "state".</summary>
    public string? PrefillFrom { get; set; }
}

/// <summary>
/// Declarative QSO-point table keyed by the relation between the worked station
/// and the operator's own station. The engine picks the most specific match:
/// SameZone &gt; SameCountry &gt; SameContinent &gt; OtherContinent &gt; Default.
/// Null members fall through to <see cref="Default"/>.
/// </summary>
public class PointsRule
{
    public int? SameCountry { get; set; }
    public int? SameContinent { get; set; }
    public int? OtherContinent { get; set; }
    public int? SameZone { get; set; }
    public int Default { get; set; } = 1;

    /// <summary>
    /// Base points per mode class ("CW", "PH", "RTTY"). Used when no relationship
    /// override (SameCountry/SameContinent/…) matches; falls back to
    /// <see cref="Default"/> when the mode isn't listed. Lets a contest score e.g.
    /// CW/digital=2, phone=1.
    /// </summary>
    public Dictionary<string, int>? ByMode { get; set; }
}

/// <summary>One multiplier dimension, e.g. CQ zones counted per band.</summary>
public class MultRule
{
    public MultSource Source { get; set; }
    public bool PerBand { get; set; }
    public bool PerMode { get; set; }
}

/// <summary>
/// Defines a contest's "home area" and how a worked station is classified for
/// role-based rules. For a QSO party <see cref="States"/> is the host state (e.g.
/// ["OH"]) or the member states of a regional (7QP's 7, NEQP's 6).
/// </summary>
public class HomeArea
{
    public HomeAreaKind Kind { get; set; } = HomeAreaKind.None;

    /// <summary>In-area state/province codes.</summary>
    public List<string> States { get; set; } = new();
}

/// <summary>
/// Role-specific rule overrides. Any null field falls back to the definition's
/// top-level value, so a role only needs to specify what actually differs.
/// </summary>
public class RoleRules
{
    public List<ContestField>? SentExchange { get; set; }
    public List<ContestField>? RcvdExchange { get; set; }
    public PointsRule? QsoPoints { get; set; }
    public List<MultRule>? MultiplierRules { get; set; }

    /// <summary>Which worked stations count for points/mults in this role.</summary>
    public WorkTarget WorksForPoints { get; set; } = WorkTarget.Everyone;
}

/// <summary>Maps a QSO to one column of a Cabrillo QSO: line.</summary>
public class CabrilloColumn
{
    /// <summary>Which datum, e.g. "freq", "mode", "date", "time", "mycall",
    /// "rstSent", "serialSent", "call", "rstRcvd", "serialRcvd", or an exchange key.</summary>
    public string Source { get; set; } = string.Empty;
    public int Width { get; set; }
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum ContestFieldType
{
    Text,
    Rst,
    Serial,
    Zone,
    State,
    Section,
    Grid,
    Name,
    Power,
    Check,
    Precedence,
}

/// <summary>The operator's role for a contest, fixed at session start.</summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum ContestRole
{
    /// <summary>No location split (global contests); uses the top-level rules.</summary>
    All,
    /// <summary>Operator is inside the contest's home area (in-state; W/VE).</summary>
    InArea,
    /// <summary>Operator is outside the home area (rest of W/VE, and DX unless split).</summary>
    OutArea,
    /// <summary>Operator is DX, where a contest treats DX distinctly from OutArea.</summary>
    Dx,
}

/// <summary>How a contest's home area is defined / how a station is classified.</summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum HomeAreaKind
{
    None,
    /// <summary>QSO party: in-area = operator/station state in HomeArea.States.</summary>
    StateCounty,
    /// <summary>ARRL DX style: in-area = W/VE, out-area = DX.</summary>
    WVE,
}

/// <summary>Which worked stations count for points/mults in a given role.</summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum WorkTarget
{
    Everyone,
    /// <summary>Only in-area stations count (typical for out-of-state QSO-party ops).</summary>
    InAreaOnly,
    /// <summary>Only out-of-area stations count (e.g. ARRL DX: W/VE work DX only).</summary>
    OutAreaOnly,
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum MultSource
{
    Dxcc,
    CqZone,
    ItuZone,
    State,
    Section,
    WpxPrefix,
    Grid,
    Continent,
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum DupeRule
{
    /// <summary>A call may be worked once per band (any mode).</summary>
    PerBand,
    /// <summary>A call may be worked once per band per mode.</summary>
    PerBandMode,
    /// <summary>A call may be worked only once for the whole contest.</summary>
    PerContest,
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SerialMode
{
    None,
    PerBand,
    AllBand,
}
