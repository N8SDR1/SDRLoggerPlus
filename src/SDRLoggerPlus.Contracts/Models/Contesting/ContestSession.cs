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
    public string? Continent { get; set; }
    public int? CqZone { get; set; }
    public int? ItuZone { get; set; }
    public string? State { get; set; }
    public string? Section { get; set; }
    public string? Grid { get; set; }

    /// <summary>Cabrillo CATEGORY-* / operator category free-form fields.</summary>
    public string? Category { get; set; }
    public string? Power { get; set; }
    public string? Name { get; set; }
}
