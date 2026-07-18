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
        var eff = Resolve(def, me);
        var (seenDupeKeys, seenMultKeys) = BuildSeen(def, eff, priorQsos);
        return EvaluateAgainst(def, eff, me, seenDupeKeys, seenMultKeys, qso);
    }

    /// <summary>
    /// Evaluate many candidate QSOs against the same prior log (built once) — used
    /// to color a bandmap's spots by dupe/new-mult. Candidates don't affect each
    /// other; results are returned in candidate order (no keying, so duplicate
    /// calls on different bands don't collide).
    /// </summary>
    public static List<QsoEvaluation> EvaluateBatch(
        ContestDefinition def, MyExchange me, IReadOnlyList<Qso> priorQsos, IReadOnlyList<Qso> candidates)
    {
        var eff = Resolve(def, me);
        var (seenDupeKeys, seenMultKeys) = BuildSeen(def, eff, priorQsos);
        return candidates
            .Select(c => EvaluateAgainst(def, eff, me, seenDupeKeys, seenMultKeys, c))
            .ToList();
    }

    // Build the seen dupe-key / mult-key sets from a prior log (dupes and
    // invalid-target QSOs contribute nothing).
    private static (HashSet<string> Dupe, HashSet<string> Mult) BuildSeen(
        ContestDefinition def, Effective eff, IReadOnlyList<Qso> priorQsos)
    {
        var seenDupeKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenMultKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var prior in priorQsos)
        {
            if (!seenDupeKeys.Add(DupeKey(def, prior)))
                continue; // prior itself was a dupe -> contributes no points or mults
            if (!IsValidTarget(eff, prior))
                continue; // station this role can't score claims no mults
            foreach (var m in MultKeys(eff, prior))
                seenMultKeys.Add(m);
        }
        return (seenDupeKeys, seenMultKeys);
    }

    /// <summary>Recompute running totals over a whole session log (chronological).</summary>
    public static ScoreSummary Recompute(
        ContestDefinition def, MyExchange me, IReadOnlyList<Qso> qsos)
    {
        var eff = Resolve(def, me);
        var seenDupeKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var seenMultKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var multsBySource = new Dictionary<string, HashSet<string>>();

        var summary = new ScoreSummary();

        foreach (var qso in qsos)
        {
            var eval = EvaluateAgainst(def, eff, me, seenDupeKeys, seenMultKeys, qso);
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
        // When this role has no multiplier rules, score is just points.
        if (eff.Mults.Count == 0)
            summary.Score = summary.Points;

        // Power-class factor (QRP×2 etc.): scales the final score only.
        var powerFactor = PowerFactor(def, me);
        if (powerFactor != 1.0)
            summary.Score = (int)Math.Round(summary.Score * powerFactor, MidpointRounding.AwayFromZero);
        summary.MultsBySource = multsBySource.ToDictionary(
            kv => kv.Key, kv => kv.Value.OrderBy(v => v, StringComparer.OrdinalIgnoreCase).ToList());
        return summary;
    }

    // -- core evaluation against an already-accumulated seen-state ----------

    private static QsoEvaluation EvaluateAgainst(
        ContestDefinition def, Effective eff, MyExchange me,
        HashSet<string> seenDupeKeys, HashSet<string> seenMultKeys, Qso qso)
    {
        var eval = new QsoEvaluation();

        var dupeKey = DupeKey(def, qso);
        if (seenDupeKeys.Contains(dupeKey))
        {
            eval.IsDupe = true;
            return eval; // dupes score nothing and claim no mults
        }

        // A station this role can't score (e.g. an out-of-state op working
        // another out-of-state station) is logged but worth 0 points and no mults.
        if (!IsValidTarget(eff, qso))
            return eval;

        eval.Points = Points(eff, me, qso);
        foreach (var key in MultKeys(eff, qso))
        {
            if (!seenMultKeys.Contains(key))
                eval.Mults.Add(key);
        }
        return eval;
    }

    // -- role resolution ----------------------------------------------------

    /// <summary>
    /// The operator's role for this contest, from the definition's HomeArea and
    /// the operator's own location. <see cref="ContestRole.All"/> when there is no
    /// role split.
    /// </summary>
    public static ContestRole DetermineRole(ContestDefinition def, MyExchange me)
    {
        var home = def.HomeArea;
        if (home is null || home.Kind == HomeAreaKind.None)
            return ContestRole.All;

        // An explicit operator choice wins over the location-based guess.
        if (me.RoleOverride is { } r && r != ContestRole.All)
            return r;

        return home.Kind switch
        {
            HomeAreaKind.StateCounty =>
                !string.IsNullOrWhiteSpace(me.State)
                && home.States.Any(s => s.Equals(me.State, StringComparison.OrdinalIgnoreCase))
                    ? ContestRole.InArea : ContestRole.OutArea,
            HomeAreaKind.WVE => IsUsOrCanada(me.Country, me.Continent)
                ? ContestRole.InArea : ContestRole.OutArea,
            _ => ContestRole.All,
        };
    }

    // The rules actually in force for the operator's role: the matching RoleRules
    // entry with per-field fallback to the definition's top-level values.
    private static Effective Resolve(ContestDefinition def, MyExchange me)
    {
        RoleRules? rr = null;
        def.Roles?.TryGetValue(DetermineRole(def, me), out rr);
        return new Effective(
            rr?.QsoPoints ?? def.QsoPoints,
            rr?.MultiplierRules ?? def.MultiplierRules,
            rr?.WorksForPoints ?? WorkTarget.Everyone,
            def.HomeArea);
    }

    private readonly record struct Effective(
        PointsRule Points, IReadOnlyList<MultRule> Mults, WorkTarget WorksForPoints, HomeArea? Home);

    private static bool IsValidTarget(Effective eff, Qso qso)
    {
        if (eff.WorksForPoints == WorkTarget.Everyone) return true;
        var cls = ClassifyStation(eff.Home, qso);
        return eff.WorksForPoints switch
        {
            WorkTarget.InAreaOnly => cls == StationClass.InArea,
            WorkTarget.OutAreaOnly => cls != StationClass.InArea,
            _ => true,
        };
    }

    private enum StationClass { InArea, OutArea, Dx }

    // Classify a worked station relative to the contest's home area. For QSO
    // parties, in-area stations send a county code (3+ chars) and out-of-area
    // stations send a 2-letter S/P (or "DX"), so length disambiguates without a
    // county table; an explicit home-state code also counts as in-area. This is a
    // heuristic pending per-contest county reference data (sub-project C).
    private static StationClass ClassifyStation(HomeArea? home, Qso qso)
    {
        if (home is null || home.Kind == HomeAreaKind.None)
            return StationClass.OutArea;

        var na = IsUsOrCanada(qso.Country ?? qso.Station?.Country, qso.Continent);

        if (home.Kind == HomeAreaKind.WVE)
            return na ? StationClass.InArea : StationClass.Dx;

        // StateCounty
        var loc = (qso.Contest?.RcvdState ?? qso.Station?.State ?? string.Empty)
            .Trim().ToUpperInvariant();
        if (loc.Length > 0 && home.States.Any(s => s.Equals(loc, StringComparison.OrdinalIgnoreCase)))
            return StationClass.InArea;
        if (loc.Length == 2) return StationClass.OutArea;   // a state/province code, or "DX"
        if (loc.Length >= 3) return StationClass.InArea;    // a county code (in-area station)
        return na ? StationClass.OutArea : StationClass.Dx; // no location -> classify by entity
    }

    // Final-score multiplier for the operator's power class, or 1 when the contest
    // defines none / the class isn't listed.
    private static double PowerFactor(ContestDefinition def, MyExchange me)
    {
        if (def.PowerMultipliers is null || string.IsNullOrWhiteSpace(me.Power))
            return 1.0;
        return def.PowerMultipliers.TryGetValue(me.Power.Trim().ToUpperInvariant(), out var f) ? f : 1.0;
    }

    private static bool IsUsOrCanada(string? country, string? continent)
    {
        if (!string.IsNullOrEmpty(country)
            && (country.Equals("United States", StringComparison.OrdinalIgnoreCase)
                || country.Equals("Canada", StringComparison.OrdinalIgnoreCase)
                || country.Contains("USA", StringComparison.OrdinalIgnoreCase)))
            return true;
        return false;
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

    private static int Points(Effective eff, MyExchange me, Qso qso)
    {
        var rule = eff.Points;

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

        // No relationship override: per-mode base if configured, else Default.
        if (rule.ByMode is not null && rule.ByMode.TryGetValue(ModeClass(qso.Mode), out var byMode))
            return byMode;

        return rule.Default;
    }

    // Normalized mode class for per-mode points ("CW", "PH", "RTTY", "DIGI").
    private static string ModeClass(string? mode)
    {
        var m = (mode ?? string.Empty).ToUpperInvariant();
        if (m.StartsWith("CW")) return "CW";
        if (m is "SSB" or "USB" or "LSB" or "PH" or "AM" or "FM" or "NFM") return "PH";
        if (m is "RTTY" or "RY") return "RTTY";
        return "DIGI";
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

    private static IEnumerable<string> MultKeys(Effective eff, Qso qso)
    {
        foreach (var rule in eff.Mults)
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
