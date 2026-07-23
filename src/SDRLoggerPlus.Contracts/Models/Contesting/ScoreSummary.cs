namespace SDRLoggerPlus.Contracts.Models.Contesting;

/// <summary>The engine's evaluation of a single QSO against a contest definition.</summary>
public class QsoEvaluation
{
    /// <summary>QSO points (0 if a dupe).</summary>
    public int Points { get; set; }

    /// <summary>True if this QSO duplicates an earlier one under the dupe rule.</summary>
    public bool IsDupe { get; set; }

    /// <summary>
    /// Multiplier keys this QSO is the first to claim (empty if none / dupe).
    /// Keys are source-qualified, e.g. "CqZone:14@20M".
    /// </summary>
    public List<string> Mults { get; set; } = new();
}

/// <summary>Running totals for a contest session's whole log.</summary>
public class ScoreSummary
{
    /// <summary>Count of scoring (non-dupe) QSOs.</summary>
    public int Qsos { get; set; }

    /// <summary>Count of dupes (excluded from the totals).</summary>
    public int Dupes { get; set; }

    /// <summary>Sum of QSO points across non-dupe QSOs.</summary>
    public int Points { get; set; }

    /// <summary>Distinct multiplier count across all rules.</summary>
    public int Multipliers { get; set; }

    /// <summary>
    /// Operator-declared bonus/objective points folded into <see cref="Score"/>
    /// (0 for contests without self-declared bonuses). Surfaced separately so the
    /// score panel can show "… + N bonus".
    /// </summary>
    public int BonusPoints { get; set; }

    /// <summary>Final claimed score (Points × Multipliers × power factor + BonusPoints).</summary>
    public int Score { get; set; }

    /// <summary>Distinct multiplier values worked, grouped by source (for the mult panel).</summary>
    public Dictionary<string, List<string>> MultsBySource { get; set; } = new();
}
