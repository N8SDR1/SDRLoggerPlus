using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services.Ffma;

namespace SDRLoggerPlus.Server.Services;

public partial class AwardsService
{
    /// <summary>
    /// FFMA progress. The award is fixed to 6 m and any mode; only a date range from
    /// the filters is honoured (band/mode are the award's, not the caller's). Every
    /// required grid is returned with its status so the UI can render the full
    /// checklist and the "still needed" list.
    /// </summary>
    public async Task<FfmaStatistics> GetFfmaStatisticsAsync(StatisticsFilters? filters = null)
    {
        var all = await AllQsosAsync();

        var sixMeter = all.Where(q => string.Equals(q.Band, "6m", StringComparison.OrdinalIgnoreCase));
        if (filters?.FromDate is { } from)
            sixMeter = sixMeter.Where(q => q.QsoDate >= from);
        if (filters?.ToDate is { } to)
            sixMeter = sixMeter.Where(q => q.QsoDate <= to.AddDays(1));

        return ComputeFfma(sixMeter, FfmaGridReference.RequiredGrids, IsFfmaConfirmed);
    }

    /// <summary>
    /// FFMA counts a grid only when it's confirmed by LoTW or a paper QSL — the award
    /// rules name nothing else. Delegates to the shared policy so every ARRL view
    /// answers "confirmed?" identically (#46).
    /// </summary>
    private static bool IsFfmaConfirmed(Qso qso) => ConfirmationPolicy.AwardRules(qso);

    /// <summary>
    /// Pure tally: reduce each 6 m QSO to its four-char grid, keep only the required
    /// ones, and mark each required grid confirmed / worked / needed. Kept separate from
    /// the repository so it can be unit-tested against a synthetic grid set.
    /// </summary>
    internal static FfmaStatistics ComputeFfma(
        IEnumerable<Qso> sixMeterQsos,
        IReadOnlySet<string> requiredGrids,
        Func<Qso, bool> isConfirmed)
    {
        var byGrid = new Dictionary<string, (int Count, bool Confirmed, DateTime? Last)>(StringComparer.Ordinal);

        foreach (var q in sixMeterQsos)
        {
            // Station.Grid first, legacy top-level as fallback — same precedence as VUCC.
            // Live-logged QSOs (manual entry, WSJT-X auto-log) carry their grid ONLY on
            // Station; reading just q.Grid made them invisible to the award (issue #42).
            var grid = NormalizeGrid(q.Station?.Grid ?? q.Grid);
            if (grid is null || !requiredGrids.Contains(grid)) continue;

            byGrid.TryGetValue(grid, out var acc);
            acc.Count += 1;
            acc.Confirmed |= isConfirmed(q);          // sticky: one confirmation is enough
            if (acc.Last is null || q.QsoDate > acc.Last) acc.Last = q.QsoDate;
            byGrid[grid] = acc;
        }

        var rows = requiredGrids
            .OrderBy(g => g, StringComparer.Ordinal)
            .Select(g =>
            {
                if (byGrid.TryGetValue(g, out var acc))
                    return new FfmaGridStatus(g, acc.Confirmed ? "confirmed" : "worked", acc.Count, acc.Last);
                return new FfmaGridStatus(g, "needed", 0, null);
            })
            .ToList();

        return new FfmaStatistics(
            TotalRequired: requiredGrids.Count,
            Worked: rows.Count(r => r.Status != "needed"),
            Confirmed: rows.Count(r => r.Status == "confirmed"),
            ListComplete: requiredGrids.Count == FfmaGridReference.OfficialCount,
            Grids: rows);
    }
}
