namespace SDRLoggerPlus.Contracts.Api;

public record DxccStatistics(
    int TotalEntitiesWorked,
    int TotalEntitiesConfirmed,
    int ChallengeScore,
    List<DxccEntityStatus> Entities,
    Dictionary<string, BandSummary> BandSummaries
);

public record DxccEntityStatus(
    int? DxccCode,
    string EntityName,
    string? Continent,
    Dictionary<string, BandStatus> BandStatus,
    DateTime? FirstWorked,
    DateTime? LastWorked,
    int TotalQsos,
    string? PrimaryPrefix = null
);

public record BandStatus(
    bool Worked,
    bool Confirmed,
    int QsoCount
);

public record BandSummary(
    int EntitiesWorked,
    int EntitiesConfirmed
);

public record StatisticsFilters(
    string? Band = null,
    string? Mode = null,
    string? Continent = null,
    string? Status = null,
    DateTime? FromDate = null,
    DateTime? ToDate = null
);

public record VuccStatistics(
    int TotalUniqueGrids,
    Dictionary<string, GridBandSummary> BandSummaries,
    List<GridDetail> Grids
);

public record GridBandSummary(
    string Band,
    int UniqueGrids,
    int ConfirmedGrids,
    int AwardThreshold,
    int QsoCount
);

public record GridDetail(
    string Grid,
    string Band,
    int QsoCount,
    bool Confirmed,
    DateTime? FirstWorked,
    DateTime? LastWorked
);

/// <summary>
/// FFMA (Fred Fish Memorial Award) progress: work all 488 four-char grids in the
/// contiguous 48 states on 6 m, confirmed by LoTW or paper QSL. Reported as a
/// checklist — every required grid with its status — because chasing the last few
/// is the point. <c>ListComplete</c> is false while the loaded roster isn't the
/// full official 488 (e.g. the shipped placeholder).
/// </summary>
public record FfmaStatistics(
    int TotalRequired,
    int Worked,
    int Confirmed,
    bool ListComplete,
    List<FfmaGridStatus> Grids
);

/// <summary>
/// One required FFMA grid. <c>Status</c> is "confirmed", "worked" (worked on 6 m but
/// not yet confirmed), or "needed" (never worked on 6 m).
/// </summary>
public record FfmaGridStatus(
    string Grid,
    string Status,
    int QsoCount,
    DateTime? LastWorked
);

/// <summary>
/// Satellite operating summary. Grids/states/entities are counted over
/// satellite QSOs only, which is what makes them awards in their own right:
/// ARRL runs VUCC Satellite as a separate award at 100 grids, and the same
/// contacts also chase WAS and DXCC via satellite.
/// </summary>
public record SatelliteStatistics(
    int TotalSatellites,
    int TotalQsos,
    int UniqueGrids,
    int ConfirmedGrids,
    int VuccThreshold,
    int UniqueStates,
    int UniqueEntities,
    List<SatelliteDetail> Satellites
);

public record SatelliteDetail(
    string Satellite,
    int QsoCount,
    int ConfirmedQsos,
    int UniqueGrids,
    DateTime? FirstWorked,
    DateTime? LastWorked
);

// Grid-tracker map: worked 4-char grids across ALL bands (or a filtered band/mode),
// reading Station.Grid with the top-level Grid as fallback. "Needed" = any grid
// NOT in this set; live activity comes from the decode stream on the frontend.
public record WorkedGrid(
    string Grid,
    bool Confirmed,
    int QsoCount
);

public record GridMapStatistics(
    int TotalGrids,
    int ConfirmedGrids,
    List<WorkedGrid> Grids
);

// POTA Statistics
public record PotaStatistics(
    int UniqueParksActivated,
    int UniqueParksHunted,
    int TotalActivationQsos,
    int TotalHuntQsos,
    List<PotaParkDetail> Parks
);

public record PotaParkDetail(
    string ParkReference,
    string ActivityType,
    int QsoCount,
    DateTime? FirstQso,
    DateTime? LastQso
);

public record PotaFilters(
    string? ActivityType = null,
    DateTime? FromDate = null,
    DateTime? ToDate = null
);

// IOTA Statistics
public record IotaStatistics(
    int TotalGroupsWorked,
    int TotalGroupsConfirmed,
    int TotalQsos,
    Dictionary<string, int> GroupsByContinent,
    List<IotaGroupDetail> Groups
);

public record IotaGroupDetail(
    string IotaReference,
    string Continent,
    int QsoCount,
    bool Confirmed,
    DateTime? FirstWorked,
    DateTime? LastWorked
);

public record IotaFilters(
    string? Continent = null,
    string? Status = null,
    DateTime? FromDate = null,
    DateTime? ToDate = null
);

// ─── SDRLogger+ ported awards (WAS / WAZ / WPX / WAC / 5BWAS / 5BDXCC) ───────
// Worked-based counting (no QSL confirmation gating), matching SDRLogger+.

public record WasStatistics(
    int TotalWorked,
    int TotalNeeded,
    List<WasStateStatus> States
);

public record WasStateStatus(
    string State,
    Dictionary<string, List<string>> Bands,  // band → modes worked
    int QsoCount
);

public record WazStatistics(
    int TotalWorked,
    int TotalNeeded,
    List<WazZoneStatus> Zones
);

public record WazZoneStatus(
    int Zone,
    Dictionary<string, List<string>> Bands,
    List<string> Entities
);

public record WpxStatistics(
    int TotalWorked,
    List<WpxPrefixStatus> Prefixes
);

public record WpxPrefixStatus(
    string Prefix,
    Dictionary<string, List<string>> Bands,
    List<string> Calls,
    int BandCount
);

public record WacStatistics(
    int BaseWorked,
    int ExtraWorked,
    bool Achieved,
    List<WacContinentStatus> Continents
);

public record WacContinentStatus(
    string Code,
    string Name,
    bool IsExtra,           // Antarctica endorsement, not part of the base award
    Dictionary<string, List<string>> Bands,
    List<string> Entities
);

public record FiveBandStatistics(
    bool Achieved,
    int UnionCount,
    List<FiveBandBandStatus> Bands
);

public record FiveBandBandStatus(
    string Band,
    int Count,
    int Threshold,
    bool Achieved,
    List<string> Items      // states or entity names worked on this band
);

// ---------------------------------------------------------------------------
// USA-CA (US Counties Award, MARAC). Unlike the ported awards — which are
// worked-only so their totals match what SDRLogger+ showed — this one reports
// worked AND confirmed separately: confirmation is the point of the award.
// A county counts once regardless of band or mode; there is no band split.
// ---------------------------------------------------------------------------

public record CountiesStatistics(
    int TotalWorked,
    int TotalConfirmed,
    int TotalTarget,
    List<CountiesStateStatus> States
);

public record CountiesStateStatus(
    string State,
    int Worked,             // distinct counties worked in this state
    int Confirmed,          // distinct counties confirmed in this state
    int Target,             // counties this state has
    int QsoCount
);

public record CountyDetail(
    string State,
    string County,
    int QsoCount,
    bool Confirmed,
    DateTime? FirstWorked,
    DateTime? LastWorked
);
