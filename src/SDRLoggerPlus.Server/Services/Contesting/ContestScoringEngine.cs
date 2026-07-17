using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services; // WpxPrefixExtractor

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Pure, I/O-free contest scoring. Evaluates dupes, QSO points, and multipliers
/// from a <see cref="ContestDefinition"/> plus the operator's own exchange. This
/// is the correctness-critical core of the contest suite and is unit-tested in
/// isolation; nothing here touches the database, network, or SignalR.
/// </summary>
public static class ContestScoringEngine
{
    /// <summary>
    /// Evaluate a single QSO against the definition, given the operator's exchange
    /// and the previously-logged contest QSOs (chronological). Returns dupe flag,
    /// points, and the multiplier keys this QSO is first to claim.
    /// </summary>
    public static QsoEvaluation Evaluate(
        ContestDefinition def, MyExchange me, IReadOnlyList<Qso> priorQsos, Qso qso)
    {
        var seenDupeKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenMultKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var prior in priorQsos)
        {
            var priorDupeKey = DupeKey(def, prior);
            if (!seenDupeKeys.Add(priorDupeKey))
                continue; // prior itself was a dupe -> contributes no points or mults
            foreach (var m in MultKeys(def, prior))
                seenMultKeys.Add(m);
        }

        return EvaluateAgainst(def, me, seenDupeKeys, seenMultKeys, qso);
    }

    /// <summary>Recompute running totals over a whole session log (chronological).</summary>
    public static ScoreSummary Recompute(
        ContestDefinition def, MyExchange me, IReadOnlyList<Qso> qsos)
    {
        var seenDupeKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenMultKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var multsBySource = new Dictionary<string, HashSet<string>>();

        var summary = new ScoreSummary();

        foreach (var qso in qsos)
        {
            var eval = EvaluateAgainst(def, me, seenDupeKeys, seenMultKeys, qso);
            if (eval.IsDupe)
            {
                summary.Dupes++;
                continue;
            }

            seenDupeKeys.Add(DupeKey(def, qso));
            summary.Qsos++;
            summary.Points += eval.Points;
            foreach (var key in eval.Mults)
            {
                seenMultKeys.Add(key);
                var (source, value) = SplitMultKey(key);
                if (!multsBySource.TryGetValue(source, out var set))
                    multsBySource[source] = set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                set.Add(value);
            }
        }

        summary.Multipliers = seenMultKeys.Count;
        summary.Score = summary.Points * Math.Max(summary.Multipliers, 1);
        // When a contest has no multiplier rules, score is just points.
        if (def.MultiplierRules.Count == 0)
            summary.Score = summary.Points;
        summary.MultsBySource = multsBySource.ToDictionary(
            kv => kv.Key, kv => kv.Value.OrderBy(v => v, StringComparer.OrdinalIgnoreCase).ToList());
        return summary;
    }

    // -- core evaluation against an already-accumulated seen-state ----------

    private static QsoEvaluation EvaluateAgainst(
        ContestDefinition def, MyExchange me,
        HashSet<string> seenDupeKeys, HashSet<string> seenMultKeys, Qso qso)
    {
        var eval = new QsoEvaluation();

        var dupeKey = DupeKey(def, qso);
        if (seenDupeKeys.Contains(dupeKey))
        {
            eval.IsDupe = true;
            return eval; // dupes score nothing and claim no mults
        }

        eval.Points = Points(def, me, qso);
        foreach (var key in MultKeys(def, qso))
        {
            if (!seenMultKeys.Contains(key))
                eval.Mults.Add(key);
        }
        return eval;
    }

    // -- dupe ---------------------------------------------------------------

    private static string DupeKey(ContestDefinition def, Qso qso)
    {
        var call = (qso.Callsign ?? string.Empty).ToUpperInvariant();
        return def.DupeRule switch
        {
            DupeRule.PerContest => call,
            DupeRule.PerBand => $"{call}|{qso.Band}",
            _ => $"{call}|{qso.Band}|{qso.Mode}",
        };
    }

    // -- points -------------------------------------------------------------

    private static int Points(ContestDefinition def, MyExchange me, Qso qso)
    {
        var rule = def.QsoPoints;

        // Precedence: same country > same zone > same continent > other continent.
        // The first relation that both holds AND has a value configured wins.
        if (rule.SameCountry.HasValue && SameCountry(me, qso))
            return rule.SameCountry.Value;

        if (rule.SameZone.HasValue && me.CqZone.HasValue && qso.Station?.CqZone.HasValue == true
            && qso.Station.CqZone == me.CqZone)
            return rule.SameZone.Value;

        var sameContinent = !string.IsNullOrEmpty(me.Continent)
            && string.Equals(me.Continent, qso.Continent, StringComparison.OrdinalIgnoreCase);

        if (sameContinent && rule.SameContinent.HasValue)
            return rule.SameContinent.Value;

        if (!sameContinent && !string.IsNullOrEmpty(qso.Continent) && rule.OtherContinent.HasValue)
            return rule.OtherContinent.Value;

        return rule.Default;
    }

    // DXCC number when both sides have it; otherwise fall back to the CTY country
    // name (the enrichment path resolves names but not always ADIF entity numbers).
    private static bool SameCountry(MyExchange me, Qso qso)
    {
        if (me.Dxcc.HasValue && qso.Dxcc.HasValue)
            return qso.Dxcc == me.Dxcc;
        return !string.IsNullOrEmpty(me.Country)
            && string.Equals(me.Country, qso.Country, StringComparison.OrdinalIgnoreCase);
    }

    // -- multipliers --------------------------------------------------------

    private static IEnumerable<string> MultKeys(ContestDefinition def, Qso qso)
    {
        foreach (var rule in def.MultiplierRules)
        {
            var value = MultValue(rule.Source, qso);
            if (string.IsNullOrWhiteSpace(value))
                continue; // no datum -> no mult contribution

            var key = $"{rule.Source}:{value}";
            if (rule.PerBand) key += $"@{qso.Band}";
            if (rule.PerMode) key += $"+{qso.Mode}";
            yield return key;
        }
    }

    private static string? MultValue(MultSource source, Qso qso) => source switch
    {
        MultSource.Dxcc => qso.Dxcc?.ToString() ?? qso.Country,
        MultSource.CqZone => qso.Station?.CqZone?.ToString(),
        MultSource.ItuZone => qso.Station?.ItuZone?.ToString(),
        MultSource.State => qso.Contest?.RcvdState ?? qso.Station?.State,
        MultSource.Section => qso.Contest?.RcvdSection,
        MultSource.WpxPrefix => WpxPrefixExtractor.Extract(qso.Callsign ?? string.Empty),
        MultSource.Grid => Truncate(qso.Contest?.RcvdGrid ?? qso.Grid, 4),
        MultSource.Continent => qso.Continent,
        _ => null,
    };

    private static string? Truncate(string? s, int len)
        => string.IsNullOrEmpty(s) ? s : (s.Length <= len ? s : s[..len]);

    // Split "CqZone:14@20M" -> ("CqZone", "14@20M"); the source is before the first ':'.
    private static (string source, string value) SplitMultKey(string key)
    {
        var idx = key.IndexOf(':');
        return idx < 0 ? (key, string.Empty) : (key[..idx], key[(idx + 1)..]);
    }
}
