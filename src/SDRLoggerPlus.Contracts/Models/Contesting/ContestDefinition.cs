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
}

/// <summary>One multiplier dimension, e.g. CQ zones counted per band.</summary>
public class MultRule
{
    public MultSource Source { get; set; }
    public bool PerBand { get; set; }
    public bool PerMode { get; set; }
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
