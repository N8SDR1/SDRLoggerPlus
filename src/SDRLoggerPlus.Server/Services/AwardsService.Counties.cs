using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services.Counties;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// USA-CA — the US Counties Award (MARAC). Kept out of
/// AwardsService.PortedAwards.cs on purpose: those trackers are worked-only,
/// with no QSL gating, so their totals match what SDRLogger+ showed. USA-CA
/// is a confirmation award — "how many counties have I got cards for" is the
/// whole question — so this one reports worked and confirmed separately.
///
/// Counting rules, per MARAC:
///   - a county counts once, regardless of band or mode (no band split);
///   - only US-entity QSOs count, decided by the same UsStateResolver chain
///     WAS uses, so the two awards never disagree about a QSO;
///   - satellite QSOs are excluded;
///   - confirmed means a QSL card or eQSL. LoTW is deliberately not accepted:
///     it does not reliably carry CNTY, so it cannot vouch for the county.
///
/// County values are read via <see cref="CountyResolver"/>, which falls back
/// to the ADIF CNTY in AdifExtra — that is where imported county data actually
/// sits, since ADIF import never mapped the field.
/// </summary>
public partial class AwardsService
{
    /// <summary>Per-county accumulator while scanning the log.</summary>
    private sealed class CountyTally
    {
        public int QsoCount;
        public bool Confirmed;
        public DateTime? FirstWorked;
        public DateTime? LastWorked;

        public void Add(Qso q, bool confirmed)
        {
            QsoCount++;
            // Once a county is confirmed it stays confirmed — a later unconfirmed
            // QSO to the same county does not take the credit away.
            Confirmed |= confirmed;
            var date = q.QsoDate;
            if (FirstWorked == null || date < FirstWorked) FirstWorked = date;
            if (LastWorked == null || date > LastWorked) LastWorked = date;
        }
    }

    public async Task<CountiesStatistics> GetCountiesStatisticsAsync(StatisticsFilters? filters = null)
    {
        var tallies = await TallyCountiesAsync(filters);

        var states = new List<CountiesStateStatus>();
        // Every state appears, worked or not, so the UI renders a full table
        // without hardcoding a state list of its own. The targets come from the
        // reference data, which is the only place that knows them.
        foreach (var state in CountyReference.States)
        {
            tallies.TryGetValue(state, out var counties);
            var worked = counties?.Count ?? 0;
            var confirmed = counties?.Values.Count(c => c.Confirmed) ?? 0;
            var qsoCount = counties?.Values.Sum(c => c.QsoCount) ?? 0;

            states.Add(new CountiesStateStatus(
                State: state,
                Worked: worked,
                Confirmed: confirmed,
                Target: CountyReference.CountyCount(state),
                QsoCount: qsoCount));
        }

        return new CountiesStatistics(
            TotalWorked: states.Sum(s => s.Worked),
            TotalConfirmed: states.Sum(s => s.Confirmed),
            TotalTarget: CountyReference.TotalCounties,
            States: states);
    }

    public async Task<List<CountyDetail>> GetCountyDetailsAsync(string state, StatisticsFilters? filters = null)
    {
        var code = (state ?? string.Empty).Trim().ToUpperInvariant();
        if (code.Length == 0) return [];

        var tallies = await TallyCountiesAsync(filters);
        tallies.TryGetValue(code, out var worked);

        // Every county in the state, so the drilldown doubles as the "still
        // needed" list — the reason to open it at all.
        return CountyReference.CountiesIn(code)
            .Select(county =>
            {
                CountyTally? tally = null;
                worked?.TryGetValue(county, out tally);
                return new CountyDetail(
                    State: code,
                    County: county,
                    QsoCount: tally?.QsoCount ?? 0,
                    Confirmed: tally?.Confirmed ?? false,
                    FirstWorked: tally?.FirstWorked,
                    LastWorked: tally?.LastWorked);
            })
            .ToList();
    }

    /// <summary>state → county → tally, over the filtered log.</summary>
    private async Task<Dictionary<string, Dictionary<string, CountyTally>>> TallyCountiesAsync(StatisticsFilters? filters)
    {
        var qsos = await GetFilteredQsosAsync(filters);
        var byState = new Dictionary<string, Dictionary<string, CountyTally>>(StringComparer.Ordinal);

        foreach (var q in qsos)
        {
            var placement = CountyResolver.Resolve(q, ResolveUsState(q));
            if (placement is not { } p) continue;

            if (!byState.TryGetValue(p.State, out var counties))
                byState[p.State] = counties = new Dictionary<string, CountyTally>(StringComparer.OrdinalIgnoreCase);
            if (!counties.TryGetValue(p.County, out var tally))
                counties[p.County] = tally = new CountyTally();

            tally.Add(q, CountyResolver.IsConfirmed(q));
        }

        return byState;
    }
}
