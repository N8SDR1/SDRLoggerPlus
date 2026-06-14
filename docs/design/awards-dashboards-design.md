# Awards Dashboards (WAS / WAZ / WPX / WAC / 5BWAS / 5BDXCC) — Design

**Date:** 2026-06-10 · Part of the [SDRLogger+ port roadmap](sdrloggerplus-port-roadmap.md)

## Purpose

Add six award trackers to SDRLoggerPlus's existing Statistics panel, replicating SDRLogger+'s counting rules (v1.09, `main.py` lines 6667–7110) so the user sees the same numbers. All counts are **worked-based** (any logged QSO counts; no QSL confirmation gating) — confirmed-tier tracking is a possible later enhancement, not in this scope.

## Counting rules (ported from SDRLogger+)

All trackers support optional band and mode filters and return per-band/mode breakdowns.

### WAS — Worked All States (50 needed)
State resolution chain, in order:
1. QSO state field (`Qso.Station.State`) if it's a valid 2-letter US state code (or full state name → code).
2. Extract a state code from the QTH text.
3. If the callsign's DXCC entity is Alaska → AK, Hawaii → HI.
4. Otherwise skip the QSO.
Guard: whatever produced the state, the call must resolve (cty.dat) to United States / Alaska / Hawaii — a "TX" extracted from a non-US QTH must not count.

### WAZ — Worked All Zones (40 needed)
CQ zone from cty.dat prefix lookup per callsign. Note: SDRLoggerPlus's `CtyService` parses `CqZone` into `CtyEntity` but doesn't expose it publicly yet — extend `GetCountryFromCallsign` (or add `GetEntityFromCallsign`) to return it. Prefer the QSO's stored `Station.CqZone` when present, falling back to cty lookup (SDRLogger+ only had the lookup; using stored ADIF data first is strictly more accurate).

### WPX — Worked Prefixes (no fixed threshold; running count)
Prefix extraction (port of `_extract_prefix`):
- Split on `/`; strip short portable suffixes (`P M QRP MM AM A B`, ≤3 chars); for stroke calls use the **shorter** part as prefix source; multiple slashes → first part.
- Prefix = leading alphanumerics through the **last digit** of the leading run (e.g. `3DA0XYZ` → `3DA0`, `N8ABC` → `N8`).
- No digit at all → first letter + `"0"` (WPX convention).

### WAC — Worked All Continents (6 standard + Antarctica bonus)
Continent per QSO: stored `Qso.Continent`/`Station.Continent` first, else cty lookup. Standard set NA/SA/EU/AS/AF/OC gates the award; AN shown as a separate endorsement card. `achieved = base_worked >= 6`.

### 5BWAS — all 50 states on each of 80/40/20/15/10
Per-band state sets using the WAS resolution chain. Per-band `achieved = count >= 50`; overall achieved when all five bands achieve.

### 5BDXCC — 100 entities on each of 80/40/20/15/10
Per-band DXCC entity sets (entity = `Qso.Country`, same keying as existing `AwardsService.GetDxccStatisticsAsync`). Per-band `achieved = count >= 100`; overall when all five achieve. Also report the union count.

## SDRLoggerPlus design

### Backend

Extend `IAwardsService`/`AwardsService` with:

```
GetWasStatisticsAsync(StatisticsFilters?)   → WasStatistics
GetWazStatisticsAsync(StatisticsFilters?)   → WazStatistics
GetWpxStatisticsAsync(StatisticsFilters?)   → WpxStatistics
GetWacStatisticsAsync(StatisticsFilters?)   → WacStatistics
Get5BWasStatisticsAsync(string? mode)       → FiveBandWasStatistics
Get5BDxccStatisticsAsync(string? mode)      → FiveBandDxccStatistics
```

DTOs in `SDRLoggerPlus.Contracts/Api/StatisticsDto.cs` following the existing `VuccStatistics` shape: totals, needed, achieved flag, and per-item detail (bands→modes map, sample calls/entities capped like SDRLogger+ caps them). New helper `WpxPrefixExtractor` (static, in `Services/`) and a `UsStateResolver` helper shared by WAS/5BWAS — both pure and unit-testable.

`StatisticsController` gains matching `GET /api/statistics/{was|waz|wpx|wac|5bwas|5bdxcc}` endpoints with `band`/`mode` query params (band ignored on the 5-band awards).

QSO volume note: existing service materializes `GetAllAsync()` per call; awards endpoints follow the same pattern (acceptable at this log size; not the place to introduce caching).

### Frontend

`StatisticsPlugin.tsx` currently hosts tabs (DXCC, VUCC, POTA, IOTA). Add six tabs following the existing tab-component pattern (`VuccStatisticsTab.tsx` is the template):

- **WAS / 5BWAS:** State × Band matrix (the SDRLogger+ "every open slot visible" view) + progress bar; 5BWAS shows per-band bars and a gold achieved banner.
- **WAZ:** 40-zone grid with worked zones filled, per-zone band/mode detail on hover/click.
- **WPX:** count cards + sortable prefix table (prefix, bands, sample calls).
- **WAC:** six continent cards + Antarctica endorsement card, worked-band chips per continent.
- **5BDXCC:** per-band progress bars (n/100) + expandable entity lists.

If the tab strip gets crowded, group as a two-row strip or a dropdown — match existing styling; no new visual language.

## Error handling

QSOs with missing/unresolvable data are skipped silently per award (ported behavior). Endpoints never 500 on bad log data; cty lookup failures simply exclude the QSO.

## Testing

- Unit: `WpxPrefixExtractor` (table-driven: stroke calls, portable suffixes, no-digit calls, `3DA0`-style), `UsStateResolver` chain incl. the non-US "TX" guard, zone fallback logic, per-award counting with synthetic QSO sets, 5-band achieved logic.
- Integration: controller endpoints against seeded repository.
- Frontend: tab render tests with mocked API per existing Vitest patterns.
