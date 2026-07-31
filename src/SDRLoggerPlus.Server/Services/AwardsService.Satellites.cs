using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services.Satellites;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Satellite operating stats — which birds have been worked, and what those
/// contacts are worth as awards.
///
/// Counting rules:
///   - a QSO counts when <see cref="SatelliteResolver.IsSatellite"/> says so;
///     rows are grouped by SAT_NAME, so a satellite QSO with no bird recorded
///     contributes to the grid/state/entity totals but to no per-bird row;
///   - grids are 4-character Maidenhead, matching how VUCC counts them;
///   - confirmed uses the shared <c>IsConfirmed</c> — LoTW included. This
///     differs from USA-CA on purpose: LoTW is excluded there because it does
///     not carry CNTY, but it does carry satellite credit and is the primary
///     confirmation path for VUCC Satellite. Excluding it would report near
///     zero confirmed for an operator who has the credit.
/// </summary>
public partial class AwardsService
{
    /// <summary>
    /// Grids needed for ARRL VUCC Satellite. Satellite is its own VUCC award
    /// rather than a band, which is why it is not in <c>VuccBands</c>.
    /// </summary>
    public const int VuccSatelliteThreshold = 100;

    /// <summary>
    /// Pseudo-band the VUCC tab files satellite grids under. Not a real band —
    /// satellite QSOs also count on their actual band (70cm, 2m), and this row
    /// is the separate ARRL award sitting alongside them.
    /// </summary>
    public const string SatelliteVuccBand = "sat";

    /// <summary>
    /// The grid a satellite QSO credits, 4-char and uppercased. Top-level Grid
    /// first (what VUCC reads) with Station.Grid as fallback (what the grid map
    /// reads), so the SAT tab and the VUCC satellite row always agree.
    /// </summary>
    private static string? SatelliteGrid(Qso q) => NormalizeGrid(q.Grid ?? q.Station?.Grid);

    /// <summary>
    /// Satellite grids as VUCC rows, from an already-filtered QSO set. Shared
    /// with <c>GetVuccStatisticsAsync</c> so both surfaces count identically.
    /// </summary>
    private static List<GridDetail> BuildSatelliteGridRows(IEnumerable<Qso> qsos, string? status,
        Func<Qso, bool> isConfirmed)
    {
        var rows = new List<GridDetail>();

        foreach (var group in qsos
                     .Where(SatelliteResolver.IsSatellite)
                     .Select(q => (Qso: q, Grid: SatelliteGrid(q)))
                     .Where(x => x.Grid != null)
                     .GroupBy(x => x.Grid!, StringComparer.Ordinal))
        {
            var groupQsos = group.Select(x => x.Qso).ToList();
            var confirmed = groupQsos.Any(isConfirmed);

            if (!string.IsNullOrEmpty(status))
            {
                var skip = status switch
                {
                    "confirmed" => !confirmed,
                    "workedNotConfirmed" => confirmed,
                    _ => false
                };
                if (skip) continue;
            }

            rows.Add(new GridDetail(
                Grid: group.Key,
                Band: SatelliteVuccBand,
                QsoCount: groupQsos.Count,
                Confirmed: confirmed,
                FirstWorked: groupQsos.Min(q => q.QsoDate),
                LastWorked: groupQsos.Max(q => q.QsoDate)));
        }

        return rows;
    }

    private sealed class SatelliteTally
    {
        public int QsoCount;
        public int ConfirmedQsos;
        public readonly HashSet<string> Grids = new(StringComparer.Ordinal);
        public DateTime? FirstWorked;
        public DateTime? LastWorked;

        public void Add(Qso q, bool confirmed, string? grid)
        {
            QsoCount++;
            if (confirmed) ConfirmedQsos++;
            if (grid != null) Grids.Add(grid);
            if (FirstWorked == null || q.QsoDate < FirstWorked) FirstWorked = q.QsoDate;
            if (LastWorked == null || q.QsoDate > LastWorked) LastWorked = q.QsoDate;
        }
    }

    public async Task<SatelliteStatistics> GetSatelliteStatisticsAsync(StatisticsFilters? filters = null)
    {
        var qsos = await GetFilteredQsosAsync(filters);

        var byBird = new Dictionary<string, SatelliteTally>(StringComparer.OrdinalIgnoreCase);
        // Grid confirmation is tracked per grid, not per QSO: one confirmed
        // contact is enough to confirm the grid, which is how VUCC credits it.
        var gridConfirmed = new Dictionary<string, bool>(StringComparer.Ordinal);
        var states = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var entities = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var q in qsos)
        {
            if (!SatelliteResolver.IsSatellite(q)) continue;

            var confirmed = IsConfirmed(q);
            var grid = SatelliteGrid(q);

            if (grid != null)
                gridConfirmed[grid] = gridConfirmed.TryGetValue(grid, out var was) ? was || confirmed : confirmed;

            var state = ResolveUsState(q);
            if (state != null) states.Add(state);

            var entity = ResolveCountry(q);
            if (!string.IsNullOrWhiteSpace(entity)) entities.Add(entity);

            // A satellite QSO with no SAT_NAME still counts toward the award
            // totals above — it just cannot say which bird it was.
            var bird = SatelliteResolver.Name(q);
            if (bird == null) continue;

            if (!byBird.TryGetValue(bird, out var tally))
                byBird[bird] = tally = new SatelliteTally();
            tally.Add(q, confirmed, grid);
        }

        var details = byBird
            .OrderByDescending(kv => kv.Value.QsoCount)
            .ThenBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => new SatelliteDetail(
                Satellite: kv.Key,
                QsoCount: kv.Value.QsoCount,
                ConfirmedQsos: kv.Value.ConfirmedQsos,
                UniqueGrids: kv.Value.Grids.Count,
                FirstWorked: kv.Value.FirstWorked,
                LastWorked: kv.Value.LastWorked))
            .ToList();

        return new SatelliteStatistics(
            TotalSatellites: details.Count,
            TotalQsos: details.Sum(d => d.QsoCount),
            UniqueGrids: gridConfirmed.Count,
            ConfirmedGrids: gridConfirmed.Count(kv => kv.Value),
            VuccThreshold: VuccSatelliteThreshold,
            UniqueStates: states.Count,
            UniqueEntities: entities.Count,
            Satellites: details);
    }
}
