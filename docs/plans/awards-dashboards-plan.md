# Awards Dashboards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Six award trackers (WAS, WAZ, WPX, WAC, 5BWAS, 5BDXCC) as Statistics-panel tabs, counting rules ported from SDRLogger+ (see `docs/design/awards-dashboards-design.md` for the exact rules — that spec is the source of truth for all counting behavior).

**Architecture:** Two pure helper classes (`WpxPrefixExtractor`, `UsStateResolver`) + a CtyService extension for CQ zones feed six new `AwardsService` methods, exposed via `StatisticsController`, rendered as six new tab components following the existing `VuccStatisticsTab` pattern.

**Tech Stack:** .NET 10, xUnit/Moq/FluentAssertions; React + @tanstack/react-query + Vitest.

---

### Task 1: WpxPrefixExtractor (pure) — TDD

**Files:** Create `src/SDRLoggerPlus.Server/Services/WpxPrefixExtractor.cs`, test `src/SDRLoggerPlus.Server.Tests/Tests/Services/WpxPrefixExtractorTests.cs`

Port `_extract_prefix` exactly (spec §WPX). Table-driven test cases:

| Input | Expected |
|---|---|
| `N8ABC` | `N8` |
| `3DA0XYZ` | `3DA0` |
| `K5P` | `K5` |
| `W1AW/P` | `W1` (portable suffix stripped) |
| `N8SDR/QRP` | `N8` |
| `VK9/N8SDR` | `VK9` (shorter part wins) |
| `N8SDR/VK9` | `VK9` |
| `F/ON4UN/P` | `F0` (multi-slash → first part; no digit → first letter + "0") |
| `RAEM` | `R0` (no digit) |
| `` (empty) | `null` |
| `KH6ABC` | `KH6` |

Steps: failing tests → run (compile error) → implement → pass → commit `feat(awards): WPX prefix extractor`.

### Task 2: UsStateResolver (pure) — TDD

**Files:** Create `src/SDRLoggerPlus.Server/Services/UsStateResolver.cs`, test `src/SDRLoggerPlus.Server.Tests/Tests/Services/UsStateResolverTests.cs`

API: `static string? Resolve(string? stateField, string? qth, string? country)`. Chain per spec §WAS:
1. stateField is valid US 2-letter code (or full name) → that code
2. else extract from QTH (token split on `[,\s/]+` matching code; substring match for full names)
3. else country == "Alaska" → AK, "Hawaii" → HI
4. Guard: resolved state counts only when country ∈ {United States, Alaska, Hawaii} (case-insensitive)

Includes `_US_STATES` (50 codes) + `_US_STATE_NAMES` (50 full names) tables ported verbatim. Test cases: state field "OH" + country "United States" → OH; state "Ohio" → OH; QTH "Columbus, OH" → OH; QTH "TX" + country "Japan" → null (guard); country "Alaska" no state/qth → AK; country "Hawaii" → HI; nothing resolvable → null; state "ZZ" qth null country US → null.

Commit `feat(awards): US state resolver`.

### Task 3: CtyService CQ zone exposure

**Files:** Modify `src/SDRLoggerPlus.Server/Services/CtyService.cs`, extend `src/SDRLoggerPlus.Server.Tests/Tests/Services/CtyServiceTests.cs`

Add `GetEntityFromCallsign(string)` returning `(string? Country, string? Continent, int? CqZone)` using the same longest-prefix walk as `GetCountryFromCallsign`. Tests: `W1AW` → zone 5, `JA1ABC` → zone 25, `VK2ABC` → zone 30, empty → nulls. Commit `feat(awards): expose CQ zone from cty lookup`.

### Task 4: DTOs + AwardsService methods — TDD

**Files:** Modify `src/SDRLoggerPlus.Contracts/Api/StatisticsDto.cs`, `src/SDRLoggerPlus.Server/Services/AwardsService.cs`; test `src/SDRLoggerPlus.Server.Tests/Tests/Services/AwardsServiceAwardsTests.cs`

DTOs (shapes; follow existing DTO style in the file):

```csharp
public record WasStatistics(int TotalWorked, int TotalNeeded, List<WasStateStatus> States);
public record WasStateStatus(string State, Dictionary<string, List<string>> Bands, int QsoCount);
public record WazStatistics(int TotalWorked, int TotalNeeded, List<WazZoneStatus> Zones);
public record WazZoneStatus(int Zone, Dictionary<string, List<string>> Bands, List<string> Entities);
public record WpxStatistics(int TotalWorked, List<WpxPrefixStatus> Prefixes);
public record WpxPrefixStatus(string Prefix, Dictionary<string, List<string>> Bands, List<string> Calls, int BandCount);
public record WacStatistics(int BaseWorked, int ExtraWorked, bool Achieved, List<WacContinentStatus> Continents);
public record WacContinentStatus(string Code, string Name, bool IsExtra, Dictionary<string, List<string>> Bands, List<string> Entities);
public record FiveBandStatistics(bool Achieved, int UnionCount, List<FiveBandBandStatus> Bands);
public record FiveBandBandStatus(string Band, int Count, int Threshold, bool Achieved, List<string> Items);
```

Service methods (band/mode filters where the spec says so): `GetWasStatisticsAsync`, `GetWazStatisticsAsync`, `GetWpxStatisticsAsync`, `GetWacStatisticsAsync`, `Get5BWasStatisticsAsync(mode)`, `Get5BDxccStatisticsAsync(mode)` — counting rules verbatim from spec (worked-based; WAZ prefers `Station.CqZone` then cty; WAC prefers `Continent`/`Station.Continent` then cty; bands normalized uppercase; 5-band set = 80M/40M/20M/15M/10M, thresholds 50/100).

Tests with a mocked `IQsoRepository` returning a synthetic log exercising: WAS chain incl. Alaska→AK and the non-US guard; WAZ from stored zone + cty fallback; WPX grouping + band counts; WAC achieved at 6 + AN as extra; 5B per-band thresholds + union; band/mode filters. Commit `feat(awards): award counting in AwardsService`.

### Task 5: Controller endpoints

**Files:** Modify `src/SDRLoggerPlus.Server/Controllers/StatisticsController.cs` + `IAwardsService` interface.

`GET /api/statistics/was|waz|wpx|wac` (band+mode query), `GET /api/statistics/5bwas|5bdxcc` (mode query). Follow the existing endpoint style exactly. Build + commit `feat(awards): statistics endpoints for six awards`.

### Task 6: Frontend — API client + six tabs

**Files:** Modify `src/SDRLoggerPlus.Web/src/api/client.ts` (6 methods + mirror types), `src/SDRLoggerPlus.Web/src/plugins/StatisticsPlugin.tsx` (tab strip → dxcc·was·waz·wpx·wac·5bwas·5bdxcc·vucc·pota·iota); create `WasStatisticsTab.tsx`, `WazStatisticsTab.tsx`, `WpxStatisticsTab.tsx`, `WacStatisticsTab.tsx`, `FiveBandWasTab.tsx`, `FiveBandDxccTab.tsx` in `src/SDRLoggerPlus.Web/src/plugins/`.

Each tab follows `VuccStatisticsTab.tsx` structure (useQuery + filters + summary + scrollable detail):
- **WAS:** progress n/50 + State×Band matrix table (rows = 50 states incl. unworked, columns = bands present in data, ✓ cell when worked).
- **WAZ:** progress n/40 + zone grid 1–40 (worked filled) + per-zone band/entity detail.
- **WPX:** count card + sortable table (prefix, band count, sample calls).
- **WAC:** six continent cards + Antarctica endorsement card; achieved banner at 6/6.
- **5BWAS / 5BDXCC:** five per-band progress bars (thresholds 50/100) + expandable worked-item lists + gold achieved banner.

Typecheck + vitest green. Commit `feat(awards): six award dashboard tabs`.

### Task 7: Verification + roadmap

Full `Category=Unit` suite + frontend suite green; mark feature 2 implemented in roadmap; commit.
